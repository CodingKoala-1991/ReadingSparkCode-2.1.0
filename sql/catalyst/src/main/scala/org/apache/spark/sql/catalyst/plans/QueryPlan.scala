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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.types.{DataType, StructType}

abstract class QueryPlan[PlanType <: QueryPlan[PlanType]] extends TreeNode[PlanType] {
  self: PlanType =>

// 对于LogicalPlan 或者 SparkPlan，最终输出的结果就是用 若干个Attribute 来表示
// Attribute 可以理解为表的字段，output 中的 Attribute 可能会来自不同的table
  def output: Seq[Attribute]

  /**
   * Extracts the relevant constraints from a given set of constraints based on the attributes that
   * appear in the [[outputSet]].
   */
  protected def getRelevantConstraints(constraints: Set[Expression]): Set[Expression] = {
    constraints
      .union(inferAdditionalConstraints(constraints))
      .union(constructIsNotNullConstraints(constraints))
      .filter(constraint =>
        constraint.references.nonEmpty && constraint.references.subsetOf(outputSet) &&
          constraint.deterministic)
  }

  /**
   * Infers a set of `isNotNull` constraints from null intolerant expressions as well as
   * non-nullable attributes. For e.g., if an expression is of the form (`a > 5`), this
   * returns a constraint of the form `isNotNull(a)`
   */
   // 针对特定的列构造 isNotNull 的约束条件
  private def constructIsNotNullConstraints(constraints: Set[Expression]): Set[Expression] = {
    // First, we propagate constraints from the null intolerant expressions.
    var isNotNullConstraints: Set[Expression] = constraints.flatMap(inferIsNotNullConstraints)

    // Second, we infer additional constraints from non-nullable attributes that are part of the
    // operator's output
    val nonNullableAttributes = output.filterNot(_.nullable)
    isNotNullConstraints ++= nonNullableAttributes.map(IsNotNull).toSet

    isNotNullConstraints -- constraints
  }

  /**
   * Infer the Attribute-specific IsNotNull constraints from the null intolerant child expressions
   * of constraints.
   */
  private def inferIsNotNullConstraints(constraint: Expression): Seq[Expression] =
    constraint match {
      // When the root is IsNotNull, we can push IsNotNull through the child null intolerant
      // expressions
      case IsNotNull(expr) => scanNullIntolerantAttribute(expr).map(IsNotNull(_))
      // Constraints always return true for all the inputs. That means, null will never be returned.
      // Thus, we can infer `IsNotNull(constraint)`, and also push IsNotNull through the child
      // null intolerant expressions.
      case _ => scanNullIntolerantAttribute(constraint).map(IsNotNull(_))
    }

  /**
   * Recursively explores the expressions which are null intolerant and returns all attributes
   * in these expressions.
   */
  private def scanNullIntolerantAttribute(expr: Expression): Seq[Attribute] = expr match {
    case a: Attribute => Seq(a)
    case _: NullIntolerant => expr.children.flatMap(scanNullIntolerantAttribute)
    case _ => Seq.empty[Attribute]
  }

  // Collect aliases from expressions, so we may avoid producing recursive constraints.
  // 别名信息其实也是一个 Expression
  // 这里要收集 该Operator 和 孩子Operator 的 Expression 中的所有别名
  // 生成一个 Map
  private lazy val aliasMap = AttributeMap(
    (expressions ++ children.flatMap(_.expressions)).collect {
      case a: Alias => (a.toAttribute, a.child)
    })

  /**
   * Infers an additional set of constraints from a given set of equality constraints.
   * For e.g., if an operator has constraints of the form (`a = 5`, `a = b`), this returns an
   * additional constraint of the form `b = 5`.
   *
   * [SPARK-17733] We explicitly prevent producing recursive constraints of the form `a = f(a, b)`
   * as they are often useless and can lead to a non-converging set of constraints.
   */
  private def inferAdditionalConstraints(constraints: Set[Expression]): Set[Expression] = {
    val constraintClasses = generateEquivalentConstraintClasses(constraints)

    var inferredConstraints = Set.empty[Expression]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        val candidateConstraints = constraints - eq
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(l) &&
            !isRecursiveDeduction(r, constraintClasses) => r
        })
        inferredConstraints ++= candidateConstraints.map(_ transform {
          case a: Attribute if a.semanticEquals(r) &&
            !isRecursiveDeduction(l, constraintClasses) => l
        })
      case _ => // No inference
    }
    inferredConstraints -- constraints
  }

  /*
   * Generate a sequence of expression sets from constraints, where each set stores an equivalence
   * class of expressions. For example, Set(`a = b`, `b = c`, `e = f`) will generate the following
   * expression sets: (Set(a, b, c), Set(e, f)). This will be used to search all expressions equal
   * to an selected attribute.
   */
  private def generateEquivalentConstraintClasses(
      constraints: Set[Expression]): Seq[Set[Expression]] = {
    var constraintClasses = Seq.empty[Set[Expression]]
    constraints.foreach {
      case eq @ EqualTo(l: Attribute, r: Attribute) =>
        // Transform [[Alias]] to its child.
        val left = aliasMap.getOrElse(l, l)
        val right = aliasMap.getOrElse(r, r)
        // Get the expression set for an equivalence constraint class.
        val leftConstraintClass = getConstraintClass(left, constraintClasses)
        val rightConstraintClass = getConstraintClass(right, constraintClasses)
        if (leftConstraintClass.nonEmpty && rightConstraintClass.nonEmpty) {
          // Combine the two sets.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: rightConstraintClass :: Nil) :+
            (leftConstraintClass ++ rightConstraintClass)
        } else if (leftConstraintClass.nonEmpty) { // && rightConstraintClass.isEmpty
          // Update equivalence class of `left` expression.
          constraintClasses = constraintClasses
            .diff(leftConstraintClass :: Nil) :+ (leftConstraintClass + right)
        } else if (rightConstraintClass.nonEmpty) { // && leftConstraintClass.isEmpty
          // Update equivalence class of `right` expression.
          constraintClasses = constraintClasses
            .diff(rightConstraintClass :: Nil) :+ (rightConstraintClass + left)
        } else { // leftConstraintClass.isEmpty && rightConstraintClass.isEmpty
          // Create new equivalence constraint class since neither expression presents
          // in any classes.
          constraintClasses = constraintClasses :+ Set(left, right)
        }
      case _ => // Skip
    }

    constraintClasses
  }

  /*
   * Get all expressions equivalent to the selected expression.
   */
  private def getConstraintClass(
      expr: Expression,
      constraintClasses: Seq[Set[Expression]]): Set[Expression] =
    constraintClasses.find(_.contains(expr)).getOrElse(Set.empty[Expression])

  /*
   *  Check whether replace by an [[Attribute]] will cause a recursive deduction. Generally it
   *  has the form like: `a -> f(a, b)`, where `a` and `b` are expressions and `f` is a function.
   *  Here we first get all expressions equal to `attr` and then check whether at least one of them
   *  is a child of the referenced expression.
   */
  private def isRecursiveDeduction(
      attr: Attribute,
      constraintClasses: Seq[Set[Expression]]): Boolean = {
    val expr = aliasMap.getOrElse(attr, attr)
    getConstraintClass(expr, constraintClasses).exists { e =>
      expr.children.exists(_.semanticEquals(e))
    }
  }

  /**
   * An [[ExpressionSet]] that contains invariants about the rows output by this operator. For
   * example, if this set contains the expression `a = 2` then that expression is guaranteed to
   * evaluate to `true` for all rows produced.
   */
  lazy val constraints: ExpressionSet = ExpressionSet(getRelevantConstraints(validConstraints))

  /**
   * This method can be overridden by any child class of QueryPlan to specify a set of constraints
   * based on the given operator's constraint propagation logic. These constraints are then
   * canonicalized and filtered automatically to contain only those attributes that appear in the
   * [[outputSet]].
   *
   * See [[Canonicalize]] for more details.
   */
  // 返回所有 可用的约束条件
  protected def validConstraints: Set[Expression] = Set.empty

  /**
   * Returns the set of attributes that are output by this node.
   */
   // 就是将 output 转成 set 的形式而已
  def outputSet: AttributeSet = AttributeSet(output)

  /**
   * All Attributes that appear in expressions from this operator.  Note that this set does not
   * include attributes that are implicitly referenced by being passed through to the output tuple.
   */
  // Project Filter 这些被称作为 Operator，每个 Operator 在构建的时候，可能会传入不同形式的 Expression
  // 因此这些 Operator 也会存在 references 这个字段，表示这个 Operator 引用了哪些 字段
  // 个人理解，references 中涉及的 Attribute 肯定比 孩子Operator 的 output 传过来的 Attribute 要少
  // references 中的 Attributes 都是该 Operator 的 Expressions 涉及的 Attribute
  def references: AttributeSet = AttributeSet(expressions.flatMap(_.references))

  /**
   * The set of all attributes that are input to this operator by its children.
   */
  // 这个 Operator 的输入 AttributeSet
  // 来自这个 Operator 的所有 Operator 的 output
  def inputSet: AttributeSet =
    AttributeSet(children.flatMap(_.asInstanceOf[QueryPlan[PlanType]].output))

  /**
   * The set of all attributes that are produced by this node.
   */
   // 这个 Operator 产生的属性
   // 在 继承LogicalPlan 的 LeafNode 中 这个方法的返回值 和 output 一样
  def producedAttributes: AttributeSet = AttributeSet.empty

  /**
   * Attributes that are referenced by expressions but not provided by this nodes children.
   * Subclasses should override this method if they produce attributes internally as it is used by
   * assertions designed to prevent the construction of invalid plans.
   */
  // 该 Operator 涉及的 Attribute，但是最终没有输出的
  // references 中的属性来自 该 Operator 涉及的 Expression
  def missingInput: AttributeSet = references -- inputSet -- producedAttributes

  /**
   * Runs [[transform]] with `rule` on all expressions present in this query operator.
   * Users should not expect a specific directionality. If a specific directionality is needed,
   * transformExpressionsDown or transformExpressionsUp should be used.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  // 借助 rule，转换该  Operator 的 所持有的 Expression，先序遍历（down），因为持有的 Expression 也是一个树状结构，就是遍历 Expression Tree
  // 最终调用下面的 transformExpressionsDown 方法
  def transformExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    transformExpressionsDown(rule)
  }

  /**
   * Runs [[transformDown]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   */
  // 先序遍历的方式，递归的转换这个  Operator 持有的所有 expression
  // 下面的 transformExpressionsUp 方法 和 transformExpressionsDown 几乎类似
  // 只是换了一种遍历方式
  def transformExpressionsDown(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionDown(e: Expression): Expression = {
    // 内部的 transformExpressionDown 方法
      // 实际调用 TreeNode 的 transformDown 方法
      // 先序遍历的方式 去替换该Expression
      // 一个 QueryPlan 可能有多个 Expression 对象
      val newE = e.transformDown(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
    // recursiveTransform 这个内部方法只匹配 和 Expression 相关的参数
    // 匹配出 和 Expression 相关的参数之后，调用内部 transformExpressionDown 的方法进行转换
      case e: Expression => transformExpressionDown(e)
      case Some(e: Expression) => Some(transformExpressionDown(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    // 遍历这个 QueryPlan 构建的时候的参数，然后调用 内部的 recursiveTransform 方法
    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Runs [[transformUp]] with `rule` on all expressions present in this query operator.
   *
   * @param rule the rule to be applied to every expression in this operator.
   * @return
   */
  def transformExpressionsUp(rule: PartialFunction[Expression, Expression]): this.type = {
    var changed = false

    @inline def transformExpressionUp(e: Expression): Expression = {
      val newE = e.transformUp(rule)
      if (newE.fastEquals(e)) {
        e
      } else {
        changed = true
        newE
      }
    }

    def recursiveTransform(arg: Any): AnyRef = arg match {
      case e: Expression => transformExpressionUp(e)
      case Some(e: Expression) => Some(transformExpressionUp(e))
      case m: Map[_, _] => m
      case d: DataType => d // Avoid unpacking Structs
      case seq: Traversable[_] => seq.map(recursiveTransform)
      case other: AnyRef => other
      case null => null
    }

    val newArgs = mapProductIterator(recursiveTransform)

    if (changed) makeCopy(newArgs).asInstanceOf[this.type] else this
  }

  /**
   * Returns the result of running [[transformExpressions]] on this node
   * and all its children.
   */
  // 在 该QueryPlan 和 他的所有孩子 QueryPlan 上执行 transformExpressions 方法
  // 转换 该QueryPlan 的 expressions 和 他的所有孩子节点的 expressions
  // 这个方法 和 上面 transformExpressionsDown 和 transformExpressionsUp 方法不一样
  // transformExpressionsDown 和 transformExpressionsUp 方法是只转换 这个 QueryPlan 的 expressions
  // 反正这里的这个 偏函数 比较绕口
  def transformAllExpressions(rule: PartialFunction[Expression, Expression]): this.type = {
    // transform 方法是从 TreeNode过来的，接受的参数就是一个偏函数
    transform {
      // 传入的是一个偏函数
      // 入参类型是 QueryPlan
      // 意思就是，如果传入的是一个 QueryPlan 的 object
      // 就会调用这个 QueryPlan 对象的 transformExpressions 方法
      // QueryPlan 的 transformExpressions 方法，实际上就是 调用 rule 按照先序遍历的方式
      // 把这个 QueryPlan 的 expressions 转换一遍
      case q: QueryPlan[_] => q.transformExpressions(rule).asInstanceOf[PlanType]
    }.asInstanceOf[this.type]
  }

  /** Returns all of the expressions present in this query plan operator. */
  // 返回 该Operator 的所有 expression
  // 个人理解
  // 每一个 Expression 其实也是一个 Tree 的结构，然后一个完整的 Expression 是由多个 子Expression 通过 Tree 的结构组合而成
  // 这里，返回的 Expression 的 list，应该是构造这个 Operator 的时候传入的 Expression 对象
  // 这个 Expression 对象可以理解为 一个 root节点，完整代表了整个表达式，通过这个 Expression 往下遍历，访问子Expression就是访问表达式的子表达式
  final def expressions: Seq[Expression] = {
    // Recursively find all expressions from a traversable.
    def seqToExpressions(seq: Traversable[Any]): Traversable[Expression] = seq.flatMap {
      case e: Expression => e :: Nil
      case s: Traversable[_] => seqToExpressions(s)
      case other => Nil
    }

    // productIterator 是 Scala 特有的语法
    // 应该是一个类如果是 Product 的子类（Product是 Scala 中的一个基础类）
    // 那么就可以通过 productIterator 来遍历这个类的构造参数
    // 下面这里，不同的 Operator 在构造的时候，可能依赖的 Expression 是一个单个的 Expression，也可能是一个 Expression 的 list
    // 下面就是通过 Scala 中的 case 来进行匹配的
    productIterator.flatMap {
      case e: Expression => e :: Nil  // 某些 Operator 可能只依赖一个 Expression
      case Some(e: Expression) => e :: Nil
      case seq: Traversable[_] => seqToExpressions(seq)  // 某些 Operator 可能依赖若干个 Expression
      case other => Nil
    }.toSeq
  }

  // 解析 output 得到schema，可以理解成 该Operator 最终输出的 schema 结构
  lazy val schema: StructType = StructType.fromAttributes(output)


  // 下面这几个都是 字符串操作
  /** Returns the output schema in the tree format. */
  def schemaString: String = schema.treeString

  /** Prints out the schema in the tree format */
  // scalastyle:off println
  def printSchema(): Unit = println(schemaString)
  // scalastyle:on println

  /**
   * A prefix string used when printing the plan.
   *
   * We use "!" to indicate an invalid plan, and "'" to indicate an unresolved plan.
   */
   // 标记状态，如果这个 Plan 不可用，会用 ！来标记
  protected def statePrefix = if (missingInput.nonEmpty && children.nonEmpty) "!" else ""

  override def simpleString: String = statePrefix + super.simpleString

  override def verboseString: String = simpleString

  /**
   * All the subqueries of current plan.
   */
  // 没看懂，获取子查询？？？？？
  def subqueries: Seq[PlanType] = {
    expressions.flatMap(_.collect {
      case e: PlanExpression[_] => e.plan.asInstanceOf[PlanType]
    })
  }

  // 没看懂，获取子查询？？？？？
  // 调用的 subqueries 方法
  override protected def innerChildren: Seq[QueryPlan[_]] = subqueries

  /**
   * Canonicalized copy of this query plan.
   */
   // 规范化字段，这里直接指向自身
  protected lazy val canonicalized: PlanType = this

  /**
   * Returns true when the given query plan will return the same results as this query plan.
   *
   * Since its likely undecidable to generally determine if two given plans will produce the same
   * results, it is okay for this function to return false, even if the results are actually
   * the same.  Such behavior will not affect correctness, only the application of performance
   * enhancements like caching.  However, it is not acceptable to return true if the results could
   * possibly be different.
   *
   * By default this function performs a modified version of equality that is tolerant of cosmetic
   * differences like attribute naming and or expression id differences. Operators that
   * can do better should override this function.
   */
  // 规范化方法，用来判断两个 QueryPlan 的输出数据是不是相同
  def sameResult(plan: PlanType): Boolean = {
    val left = this.canonicalized
    val right = plan.canonicalized
    left.getClass == right.getClass &&
      left.children.size == right.children.size &&
      left.cleanArgs == right.cleanArgs &&
      (left.children, right.children).zipped.forall(_ sameResult _)
  }

  /**
   * All the attributes that are used for this plan.
   */
  // 个人理解，references 中涉及的 Attribute 只是 该Operator 的孩子 Operator 的 output 的一部分
  // 所以 该节点涉及的所有 Attribute 应该是来自 孩子Operator 的 output
  lazy val allAttributes: AttributeSeq = children.flatMap(_.output)

  protected def cleanExpression(e: Expression): Expression = e match {
    case a: Alias =>
      // As the root of the expression, Alias will always take an arbitrary exprId, we need
      // to erase that for equality testing.
      val cleanedExprId =
        Alias(a.child, a.name)(ExprId(-1), a.qualifier, isGenerated = a.isGenerated)
      BindReferences.bindReference(cleanedExprId, allAttributes, allowFailures = true)
    case other =>
      BindReferences.bindReference(other, allAttributes, allowFailures = true)
  }

  /** Args that have cleaned such that differences in expression id should not affect equality */
  protected lazy val cleanArgs: Seq[Any] = {
    def cleanArg(arg: Any): Any = arg match {
      // Children are checked using sameResult above.
      case tn: TreeNode[_] if containsChild(tn) => null
      case e: Expression => cleanExpression(e).canonicalized
      case other => other
    }

    mapProductIterator {
      case s: Option[_] => s.map(cleanArg)
      case s: Seq[_] => s.map(cleanArg)
      case m: Map[_, _] => m.mapValues(cleanArg)
      case other => cleanArg(other)
    }.toSeq
  }
}
