/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.types.StructType


abstract class LogicalPlan extends QueryPlan[LogicalPlan] with Logging {

  // LogicalPlan 有两个 属性
  // resolved 是否被 解析完成
  // canonicalized 和 QueryPlan 里的差不多意思，是否经过规整
  private var _analyzed: Boolean = false

  /**
   * Marks this plan as already analyzed.  This should only be called by CheckAnalysis.
   */
  private[catalyst] def setAnalyzed(): Unit = { _analyzed = true }

  /**
   * Returns true if this node and its children have already been gone through analysis and
   * verification.  Note that this is only an optimization used to avoid analyzing trees that
   * have already been analyzed, and can be reset by transformations.
   */
  def analyzed: Boolean = _analyzed

  /** Returns true if this subtree contains any streaming data sources. */
  def isStreaming: Boolean = children.exists(_.isStreaming == true)

  /**
   * Returns a copy of this node where `rule` has been recursively applied first to all of its
   * children and then itself (post-order). When `rule` does not apply to a given node, it is left
   * unchanged.  This function is similar to `transformUp`, but skips sub-trees that have already
   * been marked as analyzed.
   *
   * @param rule the function use to transform this nodes children
   */
  // 逻辑挺烧脑的，各种偏函数
  def resolveOperators(rule: PartialFunction[LogicalPlan, LogicalPlan]): LogicalPlan = {
    if (!analyzed) {
      val afterRuleOnChildren = transformChildren(rule, (t, r) => t.resolveOperators(r))
      if (this fastEquals afterRuleOnChildren) {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(this, identity[LogicalPlan])
        }
      } else {
        CurrentOrigin.withOrigin(origin) {
          rule.applyOrElse(afterRuleOnChildren, identity[LogicalPlan])
        }
      }
    } else {
      this
    }
  }

  /**
   * Recursively transforms the expressions of a tree, skipping nodes that have already
   * been analyzed.
   */
  // 对 LogicalPlan 的 expression 递归的进行逻辑转换
  def resolveExpressions(r: PartialFunction[Expression, Expression]): LogicalPlan = {
    this resolveOperators  {
      // 传入的是一个偏函数的格式，所以这里的p不需要指定
      // 在真正执行这个偏函数的时候，会真正传入需要执行 transformExpressions 的 p
      case p => p.transformExpressions(r)
    }
  }

  /**
   * Computes [[Statistics]] for this plan. The default implementation assumes the output
   * cardinality is the product of all child plan's cardinality, i.e. applies in the case
   * of cartesian joins.
   *
   * [[LeafNode]]s must override this.
   */
  def statistics: Statistics = {
    if (children.isEmpty) {
      throw new UnsupportedOperationException(s"LeafNode $nodeName must implement statistics.")
    }
    Statistics(sizeInBytes = children.map(_.statistics.sizeInBytes).product)
  }

  /**
   * Returns the maximum number of rows that this plan may compute.
   *
   * Any operator that a Limit can be pushed passed should override this function (e.g., Union).
   * Any operator that can push through a Limit should override this function (e.g., Project).
   */
   // 获取当前 LogicalPlan 可能计算的最大行数，一般用于 Limit 这种 Operator
  def maxRows: Option[Long] = None

  /**
   * Returns true if this expression and all its children have been resolved to a specific schema
   * and false if it still contains any unresolved placeholders. Implementations of LogicalPlan
   * can override this (e.g.
   * [[org.apache.spark.sql.catalyst.analysis.UnresolvedRelation UnresolvedRelation]]
   * should return `false`).
   */
  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved


  // 如果这个 LogicalPlan 还没被 resolved， 那么会有一个 ' 的前缀进行标注
  override protected def statePrefix = if (!resolved) "'" else super.statePrefix

  /**
   * Returns true if all its children of this query plan have been resolved.
   */
  // 看看 子LogicalPlan 节点是否已经 resolved
  def childrenResolved: Boolean = children.forall(_.resolved)

  override lazy val canonicalized: LogicalPlan = EliminateSubqueryAliases(this)

  /**
   * Resolves a given schema to concrete [[Attribute]] references in this query plan. This function
   * should only be called on analyzed plans since it will throw [[AnalysisException]] for
   * unresolved [[Attribute]]s.
   */
  // Resolver 从源码看来，就是用于标记是否大小写敏感
  // 如果敏感，就是字符串判断
  // 如果不敏感，就是 (a: String, b: String) => a.equalsIgnoreCase(b)

  // 把一个 schema 解析成具体的 Attribute
  // schema 里的每一个字段，最终用的是 自身的 output 去匹配的
  def resolve(schema: StructType, resolver: Resolver): Seq[Attribute] = {
    schema.map { field =>
      resolve(field.name :: Nil, resolver).map {
        case a: AttributeReference => a
        case other => sys.error(s"can not handle nested schema yet...  plan $this")
      }.getOrElse {
        throw new AnalysisException(
          s"Unable to resolve ${field.name} given [${output.map(_.name).mkString(", ")}]")
      }
    }
  }

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] using the input from all child
   * nodes of this LogicalPlan. The attribute is expressed as
   * as string in the following form: `[scope].AttributeName.[nested].[fields]...`.
   */

   // 递归的用孩子的 LogicalPlan 来解析这个 LogicalPlan 的 UnresolvedAttribute
   // 一次调用可能还不够，需要多次调用可能才能完成
  // 比如有些 LogicalPlan 的 子LogicalPlan 还没被解析完成，那么这个LogicalPlan 的 attribute 是不会被解析的
  // 把没有 resolve 的 attribute 进行解析
  // 这里的 nameParts 是一个 Seq[String]，表示还没有被 resolve 的 attribute 的 name 用 . 分割形成的list
  // nameParts 属于同一个 UnresolvedAttribute ，对应 UnresolvedAttribute 的 nameParts 字段
  def resolveChildren(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    //   children 的output ，作为一个参数，调用 resolve 这个方法
    // 这个时候，children 的 output 都已经 resolved
    // 因此，nameParts 是该 LogicalPlan 没有解析的 attribute
    // children.flatMap(_.output) 是 这个LogicalPlan 的孩子们的已经解析了的output组成的list
    resolve(nameParts, children.flatMap(_.output), resolver)

  /**
   * Optionally resolves the given strings to a [[NamedExpression]] based on the output of this
   * LogicalPlan. The attribute is expressed as string in the following form:
   * `[scope].AttributeName.[nested].[fields]...`.
   */
  // 用 LogicalPlan 自身的 output 去 resolve  nameParts
  def resolve(
      nameParts: Seq[String],
      resolver: Resolver): Option[NamedExpression] =
    resolve(nameParts, output, resolver)

  /**
   * Given an attribute name, split it to name parts by dot, but
   * don't split the name parts quoted by backticks, for example,
   * `ab.cd`.`efg` should be split into two parts "ab.cd" and "efg".
   */
  // 和 上面的方法类似，但是 `` 中反引号包住的带. 的也当作一个整体进行处理
  def resolveQuoted(
      name: String,
      resolver: Resolver): Option[NamedExpression] = {
    resolve(UnresolvedAttribute.parseAttributeName(name), output, resolver)
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * This assumes `name` has multiple parts, where the 1st part is a qualifier
   * (i.e. table name, alias, or subquery alias).
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  // 被 LogicalPlan 内部调用
  // attribute 是 resolved attribute
  // 来自 LogicalPlan 的 孩子的 output
  // nameParts 是这个 LogicalPlan unresolved 的 attribute
  // 进入到这个方法，nameParts 的第一个元素是一个限定词，例如table 名，别名alias之类的
  // 否则进不来
  private def resolveAsTableColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    assert(nameParts.length > 1)
    // resolver 就是一个方法，判断结果，取决于大小写是否敏感
    // resolver 就是接受两个参数，然后判断他们是否相等
    // 返回 true 或者 false
    // nameParts.head 拿到的是 nameParts 中的第一个元素
    if (attribute.qualifier.exists(resolver(_, nameParts.head))) {
      // At least one qualifier matches. See if remaining parts match.
      // nameParts.tail 拿到的是除了第一个元素之外的剩余所有元素
      val remainingParts = nameParts.tail
      resolveAsColumn(remainingParts, resolver, attribute)
    } else {
      None
    }
  }

  /**
   * Resolve the given `name` string against the given attribute, returning either 0 or 1 match.
   *
   * Different from resolveAsTableColumn, this assumes `name` does NOT start with a qualifier.
   * See the comment above `candidates` variable in resolve() for semantics the returned data.
   */
  // 被 LogicalPlan 内部调用
  // 进入到这个方法，nameParts 的第一部分不是限定词，直接是 Column 的名了
  private def resolveAsColumn(
      nameParts: Seq[String],
      resolver: Resolver,
      attribute: Attribute): Option[(Attribute, List[String])] = {
    if (!attribute.isGenerated && resolver(attribute.name, nameParts.head)) {
      // 匹配才会返回
      Option((attribute.withName(nameParts.head), nameParts.tail.toList))
    } else {
      None
    }
  }

  /** Performs attribute resolution given a name and a sequence of possible attributes. */
  // 被 LogicalPlan 内部调用
  // nameParts 属于同一个 UnresolvedAttribute
  // 说白了就是用所有 children 的 output（也就是已经 resolved 的 attribute）
  // 去匹配这个 LogicalPlan 的 某一个 UnresolvedAttribute
  protected def resolve(
      nameParts: Seq[String],
      input: Seq[Attribute],
      resolver: Resolver): Option[NamedExpression] = {

    // A sequence of possible candidate matches.
    // Each candidate is a tuple. The first element is a resolved attribute, followed by a list
    // of parts that are to be resolved.
    // For example, consider an example where "a" is the table name, "b" is the column name,
    // and "c" is the struct field name, i.e. "a.b.c". In this case, Attribute will be "a.b",
    // and the second element will be List("c").
    // Attribute 这个字段的值最多到table，字段里头还有其他折叠的，就不再管

    // `table.column` pattern
    var candidates: Seq[(Attribute, List[String])] = {
      // If the name has 2 or more parts, try to resolve it as `table.column` first.
      if (nameParts.length > 1) {
        input.flatMap { option =>
          // option 是 resolved attribute
          // 来自 LogicalPlan 的 孩子的 output
          // nameParts 是这个 LogicalPlan unresolved 的 attribute
          resolveAsTableColumn(nameParts, resolver, option)
        }
      } else {
        Seq.empty
      }
    }

    // If none of attributes match `table.column` pattern, we try to resolve it as a column.
    // `column` pattern
    if (candidates.isEmpty) {
      candidates = input.flatMap { candidate =>
        resolveAsColumn(nameParts, resolver, candidate)
      }
    }

    def name = UnresolvedAttribute(nameParts).name

    candidates.distinct match {
      // One match, no nested fields, use it.
      // 这个 LogicalPlan 的 UnresolvedAttribute 和 来自 children 的 某个 resolved Attribute 匹配上了
      // 而且，column 这级之后没有需要继续匹配的
      // 一般的 SparkSQL 就是到这个层级就完事，因为一般也就是写到 column 这个级别的解析
      case Seq((a, Nil)) => Some(a)

      // One match, but we also need to extract the requested nested field.
      // column 这级之后，需要继续匹配
      // 这部分 抽取嵌套结构的 就没有继续看
      case Seq((a, nestedFields)) =>
        // The foldLeft adds ExtractValues for every remaining parts of the identifier,
        // and aliased it with the last part of the name.
        // For example, consider "a.b.c", where "a" is resolved to an existing attribute.
        // Then this will add ExtractValue("c", ExtractValue("b", a)), and alias the final
        // expression as "c".
        val fieldExprs = nestedFields.foldLeft(a: Expression)((expr, fieldName) =>
          ExtractValue(expr, Literal(fieldName), resolver))
        Some(Alias(fieldExprs, nestedFields.last)())

      // No matches.
      // 没有匹配
      case Seq() =>
        logTrace(s"Could not find $name in ${input.mkString(", ")}")
        None

      // More than one match.
      // 多个匹配，就会出现歧义，报错
      case ambiguousReferences =>
        val referenceNames = ambiguousReferences.map(_._1).mkString(", ")
        throw new AnalysisException(
          s"Reference '$name' is ambiguous, could be: $referenceNames.")
    }
  }

  /**
   * Refreshes (or invalidates) any metadata/data cached in the plan recursively.
   */
  // 刷新 LogicalPlan 中的 meta data
  def refresh(): Unit = children.foreach(_.refresh())
}

/**
 * A logical plan node with no children.
 */
// LeafNode 意味着 这个LogicalPlan 没有 子LogicalPlan
    // RunnableCommand 是可以直接运行的命令，是一类较多的 LeafNode 类型的 这个LogicalPlan，位于：org/apache/spark/sql/execution/command/commands.scala

// UnaryNode 有一个 子LogicalPlan

// BinaryNode 有左右两个 子LogicalPlan



// LeafNode 类型的 LogicalPlan
    // org.apache.spark.sql.catalyst.analysis
        // UnresolvedRelation
        // UnresolvedInlineTable
        // UnresolvedTableValuedFunction
    // org.apache.spark.sql.hive
        // MetastoreRelation
    // org.apache.spark.sql.catalyst.catalog
        // SimpleCatalogRelation
    // org.apache.spark.sql.execution
        // LogicalRDD
        // ExternalRDD
    // org.apache.spark.sql.catalyst.plans.logical
        // Range
        // OneRowRelation，没有 from 的 select 语句，relation 会对应到这个
        // LocalRelation
        // Command，这是一个trait
    // org.apache.spark.sql.execution.datasources
        // RunnableCommand，也是一个 trait，继承自 Command
        // 然后这个 package 下面的命令都是继承并实现了 RunnableCommand，不一一列举
    // org.apache.spark.sql.execution.datasources
        // 这个 package 下面还有若干 case class，也是 继承了 LeafNode 或者 RunnableCommand
    // org.apache.spark.sql.execution.streaming
        // MemoryPlan
        // StreamingExecutionRelation
        // StreamingRelation
    // org.apache.spark.sql.execution.columnar
        // InMemoryRelation
    // org.apache.spark.sql.hive.execution
        // CreateHiveTableAsSelectCommand，继承自 RunnableCommand
    // org.apache.spark.sql.catalyst
        // SQLTable



// UnaryNode 类型的 LogicalPlan，主要分成4大类
    // 定义重新分区
        // RedistributeData，一个 abstract class，有两个子类，都是 case class
            // SortPartitions
            // RepartitionByExpression
    // 脚本转换的相关操作
        // ScriptTransformation，使用特定的脚本对输入的数据进行转换
    // Object 相关的操作
        // DeserializeToObject
        // TypedFilter
        // AppendColumns
        // MapGroups
        // FlatMapGroupsInR
        // ObjectConsumer，这还是一个 trait
            // SerializeFromObject
            // MapPartitions
            // MapPartitionsInR
            // MapElements
            // AppendColumnsWithObject
    // 基本操作算子，Basic Logical Operators，
    // 基本操作算子算是最重要的一部分吧
    // 全部来自 org/apache/spark/sql/catalyst/plans/logical/basicLogicalOperators.scala
        // ReturnAnswer
        // Project
        // Generate
        // Filter
        // BroadcastHint
        // With
        // WithWindowDefinition
        // Sort
        // Aggregate
        // Window
        // Expand
        // GroupingSets
        // Pivot
        // Limit
        // GlobalLimit
        // LocalLimit
        // SubqueryAlias
        // Sample
        // Distinct
        // Repartition


// BinaryNode 类型的 LogicalPlan
    // Join，最复杂也最重要！！！！！！！！！！！！！！！！！！！！！！！！！！！
    // CoGroup
    // SetOperation，集合操作
        // Except
        // Intersect


// 除此之外，还有几个直接继承自 LogicalPlan 的算子
    // ObjectProducer，是一个trait，用于产生只包含Object列的行数据
    // EventTimeWatermark，针对Spark Streaming 中 watermark 机制，SparkSQL 用的很少
    // Union




abstract class LeafNode extends LogicalPlan {
  override final def children: Seq[LogicalPlan] = Nil
  override def producedAttributes: AttributeSet = outputSet
}

/**
 * A logical plan node with single child.
 */
abstract class UnaryNode extends LogicalPlan {
  def child: LogicalPlan

  override final def children: Seq[LogicalPlan] = child :: Nil

  /**
   * Generates an additional set of aliased constraints by replacing the original constraint
   * expressions with the corresponding alias
   */
  protected def getAliasedConstraints(projectList: Seq[NamedExpression]): Set[Expression] = {
    var allConstraints = child.constraints.asInstanceOf[Set[Expression]]
    projectList.foreach {
      case a @ Alias(e, _) =>
        // For every alias in `projectList`, replace the reference in constraints by its attribute.
        allConstraints ++= allConstraints.map(_ transform {
          case expr: Expression if expr.semanticEquals(e) =>
            a.toAttribute
        })
        allConstraints += EqualNullSafe(e, a.toAttribute)
      case _ => // Don't change.
    }

    allConstraints -- child.constraints
  }

  override protected def validConstraints: Set[Expression] = child.constraints

  override def statistics: Statistics = {
    // There should be some overhead in Row object, the size should not be zero when there is
    // no columns, this help to prevent divide-by-zero error.
    val childRowSize = child.output.map(_.dataType.defaultSize).sum + 8
    val outputRowSize = output.map(_.dataType.defaultSize).sum + 8
    // Assume there will be the same number of rows as child has.
    var sizeInBytes = (child.statistics.sizeInBytes * outputRowSize) / childRowSize
    if (sizeInBytes == 0) {
      // sizeInBytes can't be zero, or sizeInBytes of BinaryNode will also be zero
      // (product of children).
      sizeInBytes = 1
    }

    child.statistics.copy(sizeInBytes = sizeInBytes)
  }
}

/**
 * A logical plan node with a left and right child.
 */
abstract class BinaryNode extends LogicalPlan {
  def left: LogicalPlan
  def right: LogicalPlan

  override final def children: Seq[LogicalPlan] = Seq(left, right)
}
