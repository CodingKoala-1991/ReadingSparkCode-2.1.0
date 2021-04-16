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

package org.apache.spark.sql.catalyst.optimizer

import scala.annotation.tailrec
import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.java.function.FilterFunction
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{CatalystConf, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.Literal.{FalseLiteral, TrueLiteral}
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.types._

/**
 * Abstract class all optimizers should inherit of, contains the standard batches (extending
 * Optimizers can override this.
 */
abstract class Optimizer(sessionCatalog: SessionCatalog, conf: CatalystConf)
  extends RuleExecutor[LogicalPlan] {

  protected val fixedPoint = FixedPoint(conf.optimizerMaxIterations)

  //  SparkOptimizer 这个class 里面还有几条 优化的 batch！！！！！！！！！！！！！！！！！！！！！
  // 比如：
  // PruneFileSourcePartitions：对数据文件中分区进行剪裁，尽可能不要读入无关分区，没有细看，但是大概应该是用 属性 和 catalog 进行结合然后过滤无效分区

  def batches: Seq[Batch] = {
    // Technically some of the rules in Finish Analysis are not optimizer rules and belong more
    // in the analyzer, because they are needed for correctness (e.g. ComputeCurrentTime).
    // However, because we also use the analyzer to canonicalized queries (for view definition),
    // we do not eliminate subqueries or compute current time in the analyzer.
    Batch("Finish Analysis", Once,
      EliminateSubqueryAliases,   // 清除 SubqueryAlias，直接把 child 接上去
      ReplaceExpressions,  // 查找所有不可计算的表达式，并用可计算的语义等效表达式替换/重写它们，没有细看
      ComputeCurrentTime,  // 在优化阶段对current_database(), current_date(), current_timestamp()函数直接计算出值。
      GetCurrentDatabase(sessionCatalog),  // 在优化阶段对current_database(), current_date(), current_timestamp()函数直接计算出值。
      RewriteDistinctAggregates) ::  // 没细看
                                     // https://zhuanlan.zhihu.com/p/74519963 里面有关于这个 的一个例子
    //////////////////////////////////////////////////////////////////////////////////////////
    // Optimizer rules start here
    //////////////////////////////////////////////////////////////////////////////////////////
    // - Do the first call of CombineUnions before starting the major Optimizer rules,
    //   since it can reduce the number of iteration and the other rules could add/move
    //   extra operators between two adjacent Union operators.
    // - Call CombineUnions again in Batch("Operator Optimizations"),
    //   since the other rules might make two separate Unions operators adjacent.
    Batch("Union", Once,
      CombineUnions) ::  // 针对多个 Union 操作进行合并
    Batch("Subquery", Once,
      OptimizeSubqueries) ::  // 针对所有子查询做Optimizer中优化规则，SubqueryExpression 是包含 LogicalPlan 的 Expression
      // 例如 SELECT * FROM a WHERE   a.id IN (SELECT id FROM b)，就是 IN 这个表达式里包含了一个 LogicalPlan
    Batch("Replace Operators", fixedPoint,
      ReplaceIntersectWithSemiJoin,  // 用LEFT SemiJoin操作来替换“Intersect distinct”操作，注意不针对"Intersect all"
      ReplaceExceptWithAntiJoin,  // LEFT AntiJoin操作来替换“except distinct”操作，注意不针对"except all"
      ReplaceDistinctWithAggregate) ::  // 用 Aggregate LogicalPlan 替换 Distinct LogicalPlan，换句话说Distinct操作不会出现在最终的Physical Plan中的
    Batch("Aggregate", fixedPoint,
      RemoveLiteralFromGroupExpressions,  // 去除 group 表达式中的 字面量，因为不会对结果造成影响
      RemoveRepetitionFromGroupExpressions) ::  // 去除 group by 中相同的表达式
    Batch("Operator Optimizations", fixedPoint,
      // Operator push down
      PushProjectionThroughUnion,  // 将 Project 下推到 Union
                                   // 在 Union 的时候，有一些字段最终是不需要的，提前select 出来，可以减少数据量
      ReorderJoin,  // 把所有的条件表达式分配到join的子树中，使每个子树至少有一个条件表达式。
                    // 重排序的顺序依据条件顺序，
                    // 例如：select * from tb1,tb2,tb3 where tb1.a=tb3.c and tb3.b=tb2.b，
                    // 那么join的顺序就是join(tb1,tb3,tb1.a=tb3.c)，join(tb3,tb2,tb3.b=tb2.b)
                    // 说白了就是 Filter 的 condition 和 Join 的 condition 自底向上 交替出现
                    // 然后对这种特殊的结构 进行下推优化
      EliminateOuterJoin,   // 把 OUTER JOIN 转为等价的 非 OUTER Join，可以减少数据量。但是我没怎么看懂
      PushPredicateThroughJoin,  // 谓词下推到 Join 算子，该优化规则是将Filter中的条件下移到Join算子中
      PushDownPredicate,  // 谓词下推，上面那个是 谓词下推到Join，不一样
                          // 谓词下推到 Join，是说把 Filter 下推到 left 或者 right 的 LogicalPlan
                          // 谓词下推，就是把 Filter 下推到 子LogicalPlan
      LimitPushDown,  // Limit 下推，可以减小Child操作返回不必要的字段条目，针对 Union 和 Join
      ColumnPruning,  // 字段剪枝，即删除Child无用的的output字段
      InferFiltersFromConstraints,  // 是将总的Filter中相对于子树约束多余的约束删除掉，只支持inter join和leftsemi join
                                    // 例如：select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y where a, b
                                    // ==>
                                    // select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y
      // Operator combine
      CollapseRepartition,  // 对多次的 Repartition 操作进行合并，Repartition是一种基于exchange的shuffle操作，操作很重，剪枝很有必要
      CollapseProject,  // 针对 Project 这种 LogicalPlan 进行合并。将Project与子Project或子Aggregate进行合并。是一种剪枝操作
      CollapseWindow,  // 如果相邻的 两个 Window LogicalPlan，他们的 order by 和 partition by 都相同，那么就可以把 window 的 expression 合并到一起，然后合并两个 Window LogicalPlan
      CombineFilters,  // Filter操作合并。针对连续多次Filter进行语义合并，即AND合并
      CombineLimits,  // Limit 操作的 合并。针对GlobalLimit，LocalLimit，Limit三个，如果连续多次，会选择最小的一次limit来进行合并。
      CombineUnions,  // 针对多个 Union 操作进行合并，就是把相邻的 Union 合并
      // Constant folding and strength reduction
      NullPropagation,  // 对NULL常量参与表达式计算进行优化。与True/False相似，如果NULL常量参与计算，那么可以直接把结果设置为NULL，或者简化计算表达式。
      FoldablePropagation, // 提取可以直接计算的表达式，然后替换别名
                           //    SELECT 1.0 x, 'abc' y, Now() z ORDER BY x, y, 3 ==>  SELECT 1.0 x, 'abc' y, Now() z ORDER BY 1.0, 'abc', Now()
                           // 例如上面样例，x y z 3 这些别名，其实背后都对应了 一个可以直接执行 （foldable） 的 Expression
                           // 那么这个优化策略就是要把这些 别名替换掉，变成可以直接运行的值
      OptimizeIn(conf),  // 使用HashSet来优化set 的 in 操作
      ConstantFolding,  // 将可以静态直接求值的表达式直接求出常量，Add(1,2)==>3
      ReorderAssociativeOperator,  // 将与整型相关的可确定的表达式直接计算出其部分结果，例如：Add(Add(1,a),2)==>Add(a,3)；与ConstantFolding不同的是，这里的Add或Multiply是不确定的，但是可以尽可能计算出部分结果
      LikeSimplification,  // 简化正则匹配计算。针对前缀，后缀，包含，相等四种正则表达式，可以将Like操作转换为普通的字符串比较。
      BooleanSimplification,  // 简化 Bool 表达式，AND OR 这种组合逻辑进行优化
      SimplifyConditionals,  // 简化IF/Case语句逻辑，即删除无用的Case/IF语句
      RemoveDispensableExpressions,  // 这个规则仅仅去掉没必要的 LogicalPlan，包括两类：UnaryPositive和PromotePrecision，二者仅仅是对子表达式的封装，并无额外操作
      SimplifyBinaryComparison,  // 针对>=,<=,==等运算，如果两边表达式semanticEquals相等，即可以他们进行简化。
      PruneFilters,  // 对Filter LogicalPlan 所持有的 Expression 进行剪枝和优化，因为有一些约束可能在 孩子 LogicalPlan 中就已经体现
      EliminateSorts,  // 如果 Sort LogicalPlan 中的  orders（就是 order by 后面的 若干个 expression）是空的，那么就去除这个 Sort LogicalPlan
      SimplifyCasts,  // 删除无用的cast转换。如果cast前后数据类型没有变化，即可以删除掉cast操作
      SimplifyCaseConversionExpressions,  // 简化字符串的大小写转换函数。如果对字符串进行连续多次的Upper/Lower操作，只需要保留最后一次转换即可。
      RewriteCorrelatedScalarSubquery,  // ScalarSubquery指的是只返回一个元素（一行一列）的 Expression
                                        // 如果Filter，Project，Aggregate操作中包含相应的ScalarSubquery，就重写之，
                                        // 思想就是因为ScalarSubquery结果很小，可以过滤大部分无用元素，所以优先使用left OUTER join过滤：
                                        // Aggregate操作，max(a) 就是一个 返回单行单列结果 的 Expression
                                        // 例如select max(a), b from tb1 group by max(a)
                                        // ==>
                                        // select max(a), b from tb1 left OUTER join max(a) group by max(a)，这样先做join，效率要比先做group by操作效率高
                                        //
                                        // Project操作，
                                        // 例如select max(a), b from tb1
                                        // ==>
                                        // select max(a), b from tb1 left OUTER join max(a)
                                        //
                                        // Filter，
                                        // 例如select b from tb1 where max(a)
                                        // ==>
                                        // select b (select * from tb1 left OUTER join max(a) where max(a))
      EliminateSerialization,// 消除序列化，没有细看
      RemoveAliasOnlyProject) ::  // 消除仅仅由子查询的输出的别名构成的Project，例如：select a from (select a from t) ==> select a from t
                                  // 例如顶层的 select a 构成了一个 Project，但是这个 Project select 的数据全部来自 子查询的输出
                                  // 因此干掉这个 顶层的 Project
                                  // 实际情况中，被干掉的 Project 不一定位于最顶层，注释里写了
    Batch("Check Cartesian Products", Once,
      CheckCartesianProducts(conf)) ::  // 检查 LogicalPlan tree 中是否包含 笛卡尔积 类型的 Join 操作。如果存在这样的操作，但是在sql语句中又没有显示的使用 cross join，那么就会抛出异常
                                        // 所以这个检查必须在 ReorderJoin 之后执行
    Batch("Decimal Optimizations", fixedPoint,
      DecimalAggregates) ::  // 用于处理聚合操作中 和 Decimal 类型相关的问题。在聚合查询中，如果涉及浮点数的精度处理，性能有影响。固定精度的 Decimal 类型，可以加速聚合操作的速度
    Batch("Typed Filter Optimization", fixedPoint,
      CombineTypedFilters) ::  // 对 TypedFilter 进行合并，我理解TypedFilter 不是 SparkSQL 中的优化策略，是 DF 中的，就是 dataFrame.filter(func)，这个时候的filter 就变成了 CombineTypedFilters
    Batch("LocalRelation", fixedPoint,
      ConvertToLocalRelation,  // 将Project中的每个无需数据交换的表达式应用于LocalRelation中的相应元素，
      // 例如select a + 1 from tb1转为一个属性名为“a+1”的relation，并且tb1.a的每个值都是加过1的
      // 而且 tb1 也是一个 LocalRelation
      // 优化之前，有 Project 和 LocalRelation 两层 LogicalPlan
      // 优化之后，只有 LocalRelation 这一层 LogicalPlan
      PropagateEmptyRelation) ::  // 针对有空表的LogicalPlan进行变换
    Batch("OptimizeCodegen", Once,
      OptimizeCodegen(conf)) ::  // 用生成的代码替换 表达式，主要针对 CASE WHEN 这种表达式，分支不超过上限，就会触发优化，Spark 2.3 之后这个才比较完善
    Batch("RewriteSubquery", Once,
      RewritePredicateSubquery,  // 将EXISTS/NOT EXISTS改为left semi/anti join形式，将IN/NOT IN也改为left semi/anti join形式
      CollapseProject) :: Nil  // 针对 Project 这种 LogicalPlan 进行合并。将Project与子Project或子Aggregate进行合并。是一种剪枝操作
  }

  /**
   * Optimize all the subqueries inside expression.
   */
  // 针对所有子查询做Optimizer中优化规则，SubqueryExpression 是包含 LogicalPlan 的 Expression
  object OptimizeSubqueries extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case s: SubqueryExpression =>
        s.withNewPlan(Optimizer.this.execute(s.plan))
    }
  }
}

/**
 * An optimizer used in test code.
 *
 * To ensure extendability, we leave the standard rules in the abstract optimizer rules, while
 * specific rules go to the subclasses
 */
object SimpleTestOptimizer extends SimpleTestOptimizer

class SimpleTestOptimizer extends Optimizer(
  new SessionCatalog(
    new InMemoryCatalog,
    EmptyFunctionRegistry,
    new SimpleCatalystConf(caseSensitiveAnalysis = true)),
  new SimpleCatalystConf(caseSensitiveAnalysis = true))

/**
 * Removes the Project only conducting Alias of its child node.
 * It is created mainly for removing extra Project added in EliminateSerialization rule,
 * but can also benefit other operators.
 */
// 消除仅仅由子查询的输出的别名构成的Project，例如：select a from (select a from t) ==> select a from t
// 例如顶层的 select a 构成了一个 Project，但是这个 Project select 的数据全部来自 子查询的输出
// 因此干掉这个 顶层的 Project
object RemoveAliasOnlyProject extends Rule[LogicalPlan] {
  /**
   * Returns true if the project list is semantically same as child output, after strip alias on
   * attribute.
   */
  private def isAliasOnly(
      projectList: Seq[NamedExpression],
      childOutput: Seq[Attribute]): Boolean = {
    if (projectList.length != childOutput.length) {
      false
    } else {
      stripAliasOnAttribute(projectList).zip(childOutput).forall {
        case (a: Attribute, o) if a semanticEquals o => true
        case _ => false
      }
    }
  }

  private def stripAliasOnAttribute(projectList: Seq[NamedExpression]) = {
    projectList.map {
      // Alias with metadata can not be stripped, or the metadata will be lost.
      // If the alias name is different from attribute name, we can't strip it either, or we may
      // accidentally change the output schema name of the root plan.
      case a @ Alias(attr: Attribute, name) if a.metadata == Metadata.empty && name == attr.name =>
        attr
      case other => other
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    // 首先找到 这个要被干掉的 Project
    // 这个要干掉的Project 要符合以下条件：
    // 1 构成这个 Project 的 属性长度 和 孩子 LogicalPlan 的属性长度 要一致
    // 2 这个 Project 的每一个属性 在语义上要和 孩子 LogicalPlan 的属性一一对应
    val aliasOnlyProject = plan.collectFirst {
      case p @ Project(pList, child) if isAliasOnly(pList, child.output) => p
    }

    // 找到这个要被替换的 Project 之后，因为这个 Project 可能在整个 LogicalPlan 里面不是 root
    // 所以还需要递归向下去找
    aliasOnlyProject.map { case proj =>
      // 获取这个 Project 输出的 字段，并和 来自孩子LogicalPlan 的字段进行 map 关联
      // 做成map
      val attributesToReplace = proj.output.zip(proj.child.output).filterNot {
        case (a1, a2) => a1 semanticEquals a2
      }
      val attrMap = AttributeMap(attributesToReplace)
      plan transform {
        // 从root开始遍历，遇到这个 Project 就干掉
        case plan: Project if plan eq proj => plan.child
        // 如果不是这个要干掉的 Project，则递归的检查当前LogicalPlan 中表达式里有没有来自被干掉的Project 的输出，然后用被干掉的 Project 的 孩子LogicalPlan 的输出进行替换
        case plan => plan transformExpressions {
          case a: Attribute if attrMap.contains(a) => attrMap(a)
        }
      }
    }.getOrElse(plan)
  }
}

/**
 * Pushes down [[LocalLimit]] beneath UNION ALL and beneath the streamed inputs of outer joins.
 */
// Limit 下推，可以减小Child操作返回不必要的字段条目，针对 Union 和 Join
//
// LocalLimit(exp, Union(children)) 将limit操作下移到每个 Union上面；
// 实例：(select a from t where a>10 union all select b from t where b>20) limit 30 --> (select a from t where a>10 limit 30 union all select b from t where b>20 limit 30) limit 30
// 注意：该规则中的Union操作为UNION ALL，不适用于UNION DISTINCT
//
// LocalLimit(exp, join @ Join(left, right, joinType, _)) 根据Join操作的类型，将limit操作移下移到left或者right。
object LimitPushDown extends Rule[LogicalPlan] {

  private def stripGlobalLimitIfPresent(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case GlobalLimit(_, child) => child
      case _ => plan
    }
  }

  private def maybePushLimit(limitExp: Expression, plan: LogicalPlan): LogicalPlan = {
    (limitExp, plan.maxRows) match {
      case (IntegerLiteral(maxRow), Some(childMaxRows)) if maxRow < childMaxRows =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case (_, None) =>
        LocalLimit(limitExp, stripGlobalLimitIfPresent(plan))
      case _ => plan
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // Adding extra Limits below UNION ALL for children which are not Limit or do not have Limit
    // descendants whose maxRow is larger. This heuristic is valid assuming there does not exist any
    // Limit push-down rule that is unable to infer the value of maxRows.
    // Note: right now Union means UNION ALL, which does not de-duplicate rows, so it is safe to
    // pushdown Limit through it. Once we add UNION DISTINCT, however, we will not be able to
    // pushdown Limit.
    case LocalLimit(exp, Union(children)) =>
      LocalLimit(exp, Union(children.map(maybePushLimit(exp, _))))
    // Add extra limits below OUTER JOIN. For LEFT OUTER and FULL OUTER JOIN we push limits to the
    // left and right sides, respectively. For FULL OUTER JOIN, we can only push limits to one side
    // because we need to ensure that rows from the limited side still have an opportunity to match
    // against all candidates from the non-limited side. We also need to ensure that this limit
    // pushdown rule will not eventually introduce limits on both sides if it is applied multiple
    // times. Therefore:
    //   - If one side is already limited, stack another limit on top if the new limit is smaller.
    //     The redundant limit will be collapsed by the CombineLimits rule.
    //   - If neither side is limited, limit the side that is estimated to be bigger.
    case LocalLimit(exp, join @ Join(left, right, joinType, _)) =>
      val newJoin = joinType match {
        // RightOuter ，limit 下推到 right 的 LogicalPlan
        case RightOuter => join.copy(right = maybePushLimit(exp, right))
        // LeftOuter ，limit 下推到 left 的 LogicalPlan
        case LeftOuter => join.copy(left = maybePushLimit(exp, left))
        // 哪边有最大限制，就下推到哪一边
        case FullOuter =>
          (left.maxRows, right.maxRows) match {
            case (None, None) =>
              if (left.statistics.sizeInBytes >= right.statistics.sizeInBytes) {
                join.copy(left = maybePushLimit(exp, left))
              } else {
                join.copy(right = maybePushLimit(exp, right))
              }
            case (Some(_), Some(_)) => join
            case (Some(_), None) => join.copy(left = maybePushLimit(exp, left))
            case (None, Some(_)) => join.copy(right = maybePushLimit(exp, right))

          }
        case _ => join
      }
      LocalLimit(exp, newJoin)
  }
}

/**
 * Pushes Project operator to both sides of a Union operator.
 * Operations that are safe to pushdown are listed as follows.
 * Union:
 * Right now, Union means UNION ALL, which does not de-duplicate rows. So, it is
 * safe to pushdown Filters and Projections through it. Filter pushdown is handled by another
 * rule PushDownPredicate. Once we add UNION DISTINCT, we will not be able to pushdown Projections.
 */
// Project 下推到 Union 算子
// 在 Union 的时候，有一些字段最终是不需要的，提前select 出来，可以减少数据量
// 然后 Union 其实只是 列对得上就可以，不要求left 和 right 的字段名称一致
object PushProjectionThroughUnion extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Maps Attributes from the left side to the corresponding Attribute on the right side.
   */
  // 用 第一个 child，就是包含了所有字段的 那个child
  // 去生成 字段映射map，因为right select 出来的字段名可能和left 不一样，但是 字段数量一致就可以
  // 因此这里需要做一个映射
  private def buildRewrites(left: LogicalPlan, right: LogicalPlan): AttributeMap[Attribute] = {
    assert(left.output.size == right.output.size)
    AttributeMap(left.output.zip(right.output))
  }

  /**
   * Rewrites an expression so that it can be pushed to the right side of a
   * Union or Except operator. This method relies on the fact that the output attributes
   * of a union/intersect/except are always equal to the left child's output.
   */
  private def pushToRight[A <: Expression](e: A, rewrites: AttributeMap[Attribute]) = {
    val result = e transform {
      case a: Attribute => rewrites(a)
    }

    // We must promise the compiler that we did not discard the names in the case of project
    // expressions.  This is safe since the only transformation is from Attribute => Attribute.
    result.asInstanceOf[A]
  }

  /**
   * Splits the condition expression into small conditions by `And`, and partition them by
   * deterministic, and finally recombine them by `And`. It returns an expression containing
   * all deterministic expressions (the first field of the returned Tuple2) and an expression
   * containing all non-deterministic expressions (the second field of the returned Tuple2).
   */
  private def partitionByDeterministic(condition: Expression): (Expression, Expression) = {
    val andConditions = splitConjunctivePredicates(condition)
    andConditions.partition(_.deterministic) match {
      case (deterministic, nondeterministic) =>
        deterministic.reduceOption(And).getOrElse(Literal(true)) ->
        nondeterministic.reduceOption(And).getOrElse(Literal(true))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {

    // Push down deterministic projection through UNION ALL
    // Project -> Union
    // 然后执行下推，Union 中每一个 child 的结构都是一样的，因此新的第一个 child 需要包含完整schema去定调
    case p @ Project(projectList, Union(children)) =>
      assert(children.nonEmpty)
      if (projectList.forall(_.deterministic)) {
        // 第一个new child 需要包含所有的 字段，封装成一个 Project
        val newFirstChild = Project(projectList, children.head)
        // 剩下的其他 child 就按照第一个 Project 的字段去映射
        val newOtherChildren = children.tail.map { child =>
          // 个人理解，就是改写 原有的字段，然后在 旧的LogicalPlan 上套一个 Project
          val rewrites = buildRewrites(children.head, child)
          Project(projectList.map(pushToRight(_, rewrites)), child)
        }
        // 再在这些 new child list 上面再套 一个 Union
        Union(newFirstChild +: newOtherChildren)
      } else {
        p
      }
  }
}

/**
 * Attempts to eliminate the reading of unneeded columns from the query plan.
 *
 * Since adding Project before Filter conflicts with PushPredicatesThroughProject, this rule will
 * remove the Project p2 in the following pattern:
 *
 *   p1 @ Project(_, Filter(_, p2 @ Project(_, child))) if p2.outputSet.subsetOf(p2.inputSet)
 *
 * p2 is usually inserted by this rule and useless, p1 could prune the columns anyway.
 */
// 字段剪枝，即删除Child无用的的output字段
// 例如：select c from (select max(a) as c,max(b) as d from t) --> select c from (select max(a) as c from t)
object ColumnPruning extends Rule[LogicalPlan] {
  private def sameOutput(output1: Seq[Attribute], output2: Seq[Attribute]): Boolean =
    output1.size == output2.size &&
      output1.zip(output2).forall(pair => pair._1.semanticEquals(pair._2))

  def apply(plan: LogicalPlan): LogicalPlan = removeProjectBeforeFilter(plan transform {
    // Prunes the unused columns from project list of Project/Aggregate/Expand
    // 如果p2输出的字段有p中不需要的，即可以简化p2的输出。
    // 实例：select a from (select a,b from t) --> select a from (select a from t)。在下面的CollapseProject会对这个表达式进行二次优化。
    case p @ Project(_, p2: Project) if (p2.outputSet -- p.references).nonEmpty =>
      p.copy(child = p2.copy(projectList = p2.projectList.filter(p.references.contains)))
    case p @ Project(_, a: Aggregate) if (a.outputSet -- p.references).nonEmpty =>
      p.copy(
        child = a.copy(aggregateExpressions = a.aggregateExpressions.filter(p.references.contains)))
    case a @ Project(_, e @ Expand(_, _, grandChild)) if (e.outputSet -- a.references).nonEmpty =>
      val newOutput = e.output.filter(a.references.contains(_))
      val newProjects = e.projections.map { proj =>
        proj.zip(e.output).filter { case (_, a) =>
          newOutput.contains(a)
        }.unzip._1
      }
      a.copy(child = Expand(newProjects, newOutput, grandChild))

    // Prunes the unused columns from child of `DeserializeToObject`
    case d @ DeserializeToObject(_, _, child) if (child.outputSet -- d.references).nonEmpty =>
      d.copy(child = prunedChild(child, d.references))

    // Prunes the unused columns from child of Aggregate/Expand/Generate
    case a @ Aggregate(_, _, child) if (child.outputSet -- a.references).nonEmpty =>
      a.copy(child = prunedChild(child, a.references))
    case e @ Expand(_, _, child) if (child.outputSet -- e.references).nonEmpty =>
      e.copy(child = prunedChild(child, e.references))
    case g: Generate if !g.join && (g.child.outputSet -- g.references).nonEmpty =>
      g.copy(child = prunedChild(g.child, g.references))

    // Turn off `join` for Generate if no column from it's child is used
    case p @ Project(_, g: Generate)
        if g.join && !g.outer && p.references.subsetOf(g.generatedSet) =>
      p.copy(child = g.copy(join = false))

    // Eliminate unneeded attributes from right side of a Left Existence Join.
    // 对于 left join，右边没有必要的 字段也干掉
    case j @ Join(_, right, LeftExistence(_), _) =>
      j.copy(right = prunedChild(right, j.references))

    // all the columns will be used to compare, so we can't prune them
    case p @ Project(_, _: SetOperation) => p
    case p @ Project(_, _: Distinct) => p
    // Eliminate unneeded attributes from children of Union.
    case p @ Project(_, u: Union) =>
      if ((u.outputSet -- p.references).nonEmpty) {
        val firstChild = u.children.head
        val newOutput = prunedChild(firstChild, p.references).output
        // pruning the columns of all children based on the pruned first child.
        val newChildren = u.children.map { p =>
          val selected = p.output.zipWithIndex.filter { case (a, i) =>
            newOutput.contains(firstChild.output(i))
          }.map(_._1)
          Project(selected, p)
        }
        p.copy(child = u.withNewChildren(newChildren))
      } else {
        p
      }

    // Prune unnecessary window expressions
    case p @ Project(_, w: Window) if (w.windowOutputSet -- p.references).nonEmpty =>
      p.copy(child = w.copy(
        windowExpressions = w.windowExpressions.filter(p.references.contains)))

    // Eliminate no-op Window
    case w: Window if w.windowExpressions.isEmpty => w.child

    // Eliminate no-op Projects
    case p @ Project(_, child) if sameOutput(child.output, p.output) => child

    // Can't prune the columns on LeafNode
    case p @ Project(_, _: LeafNode) => p

    // for all other logical plans that inherits the output from it's children
    // child和p有相同的输出，就可以删除Project的封装
    // 实例：select b from (select b from t) --> select b from t
    case p @ Project(_, child) =>
      val required = child.references ++ p.references
      if ((child.inputSet -- required).nonEmpty) {
        val newChildren = child.children.map(c => prunedChild(c, required))
        p.copy(child = child.withNewChildren(newChildren))
      } else {
        p
      }
  })

  /** Applies a projection only when the child is producing unnecessary attributes */
  // 孩子 logicalPlan 产生了 没有使用的无必要 attributes
  // 那么就裁掉，就是更新 孩子LogicalPlan 的 output 字段
  private def prunedChild(c: LogicalPlan, allReferences: AttributeSet) =
    if ((c.outputSet -- allReferences.filter(c.outputSet.contains)).nonEmpty) {
      Project(c.output.filter(allReferences.contains), c)
    } else {
      c
    }

  /**
   * The Project before Filter is not necessary but conflict with PushPredicatesThroughProject,
   * so remove it.
   */
  private def removeProjectBeforeFilter(plan: LogicalPlan): LogicalPlan = plan transform {
    case p1 @ Project(_, f @ Filter(_, p2 @ Project(_, child)))
      if p2.outputSet.subsetOf(child.outputSet) =>
      p1.copy(child = f.copy(child = child))
  }
}

/**
 * Combines two adjacent [[Project]] operators into one and perform alias substitution,
 * merging the expressions into one single expression.
 */
// 针对 相邻 的 Project 这种 LogicalPlan 进行合并。将Project与子Project或子Aggregate进行合并。是一种剪枝操作
object CollapseProject extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case p1 @ Project(_, p2: Project) =>
      if (haveCommonNonDeterministicOutput(p1.projectList, p2.projectList)) {
        p1
      } else {
        // 连续两次Project操作，并且Project输出都是deterministic类型，那么就两个Project进行合并。
        // select c + 1 from (select a+b as c from t) -->select a+b+1 as c+1 from t。
        p2.copy(projectList = buildCleanedProjectList(p1.projectList, p2.projectList))
      }
    case p @ Project(_, agg: Aggregate) =>
      if (haveCommonNonDeterministicOutput(p.projectList, agg.aggregateExpressions)) {
        p
      } else {
        // 和上面的效果类似
        // select c+1 from (select max(a) as c from t) --> select max(a)+1 as c+1 from t
        agg.copy(aggregateExpressions = buildCleanedProjectList(
          p.projectList, agg.aggregateExpressions))
      }
  }

  private def collectAliases(projectList: Seq[NamedExpression]): AttributeMap[Alias] = {
    AttributeMap(projectList.collect {
      // case class Alias(child: Expression, name: String)
      // Alias 这个 Expression 的 构造参数有两个
      // child： 实际代表 这个 Alias 的 Expression
      // name：这个 Alias 的 名字
      //
      // 所以这里构建的 k-v 结构如下：
      // key，这个 Alias 的 name
      // value，这个 Alias 对象
      case a: Alias => a.toAttribute -> a
    })
  }

  // upper：上层 Project 的 Expression
  // lower: 下层 Project 的 Expression
  private def haveCommonNonDeterministicOutput(
      upper: Seq[NamedExpression], lower: Seq[NamedExpression]): Boolean = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    // 这里构造了一个 Map，这是用 下层 Project 的 Expression 构建的
    // key 是 某个 Alias 的 name
    // value 就是这个 Alias 的 具体 Object
    val aliases = collectAliases(lower)

    // Collapse upper and lower Projects if and only if their overlapped expressions are all
    // deterministic.
    upper.exists(_.collect {
      case a: Attribute if aliases.contains(a) => aliases(a).child
    }.exists(!_.deterministic))
  }

  // 合并上下两个 LogicalPlan
  private def buildCleanedProjectList(
      upper: Seq[NamedExpression],
      lower: Seq[NamedExpression]): Seq[NamedExpression] = {
    // Create a map of Aliases to their values from the lower projection.
    // e.g., 'SELECT ... FROM (SELECT a + b AS c, d ...)' produces Map(c -> Alias(a + b, c)).
    val aliases = collectAliases(lower)

    // Substitute any attributes that are produced by the lower projection, so that we safely
    // eliminate it.
    // e.g., 'SELECT c + 1 FROM (SELECT a + b AS C ...' produces 'SELECT a + b + 1 ...'
    // Use transformUp to prevent infinite recursion.
    val rewrittenUpper = upper.map(_.transformUp {
      case a: Attribute => aliases.getOrElse(a, a)
    })
    // collapse upper and lower Projects may introduce unnecessary Aliases, trim them here.
    rewrittenUpper.map { p =>
      CleanupAliases.trimNonTopLevelAliases(p).asInstanceOf[NamedExpression]
    }
  }
}

/**
 * Combines adjacent [[Repartition]] and [[RepartitionByExpression]] operator combinations
 * by keeping only the one.
 * 1. For adjacent [[Repartition]]s, collapse into the last [[Repartition]].
 * 2. For adjacent [[RepartitionByExpression]]s, collapse into the last [[RepartitionByExpression]].
 * 3. For a combination of [[Repartition]] and [[RepartitionByExpression]], collapse as a single
 *    [[RepartitionByExpression]] with the expression and last number of partition.
 */
// 对多次的 Repartition 操作进行合并，Repartition是一种基于exchange的shuffle操作，操作很重，剪枝很有必要
//
// 注意：Repartition操作只针对在DataFrame's上调用coalesce or repartition函数，是无法通过SQL来构造含有Repartition的Plan。
// SQL中类似的为RepartitionByExpression，但是它不适合这个规则
// 比如：select * from (select * from t distribute by a) distribute by a
// 会产生两次RepartitionByExpression操作。
// == Optimized Logical Plan ==
// RepartitionByExpression [a#391]
//     +- RepartitionByExpression [a#391]
//     +- MetastoreRelation default, t
object CollapseRepartition extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // Case 1
    // 连续两次两次 Repartition， 以最外层的参数为准
    case Repartition(numPartitions, shuffle, Repartition(_, _, child)) =>
      Repartition(numPartitions, shuffle, child)
    // Case 2
    case RepartitionByExpression(exprs, RepartitionByExpression(_, child, _), numPartitions) =>
      RepartitionByExpression(exprs, child, numPartitions)
    // Case 3
    case Repartition(numPartitions, _, r: RepartitionByExpression) =>
      r.copy(numPartitions = Some(numPartitions))
    // Case 3
    case RepartitionByExpression(exprs, Repartition(_, _, child), numPartitions) =>
      RepartitionByExpression(exprs, child, numPartitions)
  }
}

/**
 * Collapse Adjacent Window Expression.
 * - If the partition specs and order specs are the same, collapse into the parent.
 */
// 如果相邻的 两个 Window LogicalPlan，他们的 order by 和 partition by 都相同，那么就可以把 window 的 expression 合并到一起，然后合并两个 Window LogicalPlan
object CollapseWindow extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case w @ Window(we1, ps1, os1, Window(we2, ps2, os2, grandChild)) if ps1 == ps2 && os1 == os2 =>
      w.copy(windowExpressions = we2 ++ we1, child = grandChild)
  }
}

/**
 * Generate a list of additional filters from an operator's existing constraint but remove those
 * that are either already part of the operator's condition or are part of the operator's child
 * constraints. These filters are currently inserted to the existing conditions in the Filter
 * operators and on either side of Join operators.
 *
 * Note: While this optimization is applicable to all types of join, it primarily benefits Inner and
 * LeftSemi joins.
 */
// 是将总的Filter中相对于子树约束多余的约束删除掉，只支持inter join和leftsemi join
// 例如：select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y where a, b
// ==>
// select * from (select * from tb1 where a) as x, (select * from tb2 where b) as y
object InferFiltersFromConstraints extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, child) =>
      // 用 Filter 中自带的 约束 减去 所有孩子的约束，看看还能不不能剩下东西
      // 如果剩下就构建一个新的 Filter
      val newFilters = filter.constraints --
        (child.constraints ++ splitConjunctivePredicates(condition))
      if (newFilters.nonEmpty) {
        Filter(And(newFilters.reduce(And), condition), child)
      } else {
        filter
      }

    case join @ Join(left, right, joinType, conditionOpt) =>
      // Only consider constraints that can be pushed down completely to either the left or the
      // right child
      // 从 left 和 right 中找出在 left 和 right output 相关的 限制条件 list
      val constraints = join.constraints.filter { c =>
        c.references.subsetOf(left.outputSet) || c.references.subsetOf(right.outputSet)
      }
      // Remove those constraints that are already enforced by either the left or the right child
      // 减去 在 left 和 right 已经使用了的 限制
      // 剩下的 限制就是 只能在 Join 使用的
      val additionalConstraints = constraints -- (left.constraints ++ right.constraints)
      // newConditionOpt 是去除了 多余的 限制条件 的 Join 的限制条件
      // conditionOpt 是Join 中，left 和 right 进行join的匹配条件
      val newConditionOpt = conditionOpt match {
        case Some(condition) =>
          val newFilters = additionalConstraints -- splitConjunctivePredicates(condition)
          if (newFilters.nonEmpty) Option(And(newFilters.reduce(And), condition)) else None
        case None =>
          additionalConstraints.reduceOption(And)
      }
      // 用新的 匹配条件构建 Join
      if (newConditionOpt.isDefined) Join(left, right, joinType, newConditionOpt) else join
  }
}

/**
 * Combines all adjacent [[Union]] operators into a single [[Union]].
 */
// 针对多个 Union 操作进行合并，就是把相邻的 Union 合并
object CombineUnions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case u: Union => flattenUnion(u, false)
    case Distinct(u: Union) => Distinct(flattenUnion(u, true))
  }

  private def flattenUnion(union: Union, flattenDistinct: Boolean): Union = {
    val stack = mutable.Stack[LogicalPlan](union)
    val flattened = mutable.ArrayBuffer.empty[LogicalPlan]
    while (stack.nonEmpty) {
      stack.pop() match {
        case Distinct(Union(children)) if flattenDistinct =>
          stack.pushAll(children.reverse)
        case Union(children) =>
          stack.pushAll(children.reverse)
        case child =>
          flattened += child
      }
    }
    Union(flattened)
  }
}

/**
 * Combines two adjacent [[Filter]] operators into one, merging the non-redundant conditions into
 * one conjunctive predicate.
 */
// 连续的 Filter操作合并。针对连续多次Filter进行语义合并，即AND合并
// 实例：select a from (select a from t where a > 10) where a>20 --> select a from t where a > 10 and a>20
// 实例：select a as c from (select a from t where a > 10) --> select a as c from t where a > 10
object CombineFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Filter(fc, nf @ Filter(nc, grandChild)) =>
      (ExpressionSet(splitConjunctivePredicates(fc)) --
        ExpressionSet(splitConjunctivePredicates(nc))).reduceOption(And) match {
        case Some(ac) =>
          Filter(And(nc, ac), grandChild)
        case None =>
          nf
      }
  }
}

/**
 * Removes no-op SortOrder from Sort
 */
// 如果 Sort LogicalPlan 中的  orders（就是 order by 后面的 若干个 expression）是空的，那么就去除这个 Sort LogicalPlan
object EliminateSorts extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case s @ Sort(orders, _, child) if orders.isEmpty || orders.exists(_.child.foldable) =>
      // 留下不能直接计算的 order by 语句
      val newOrders = orders.filterNot(_.child.foldable)
      if (newOrders.isEmpty) child else s.copy(order = newOrders)
  }
}

/**
 * Removes filters that can be evaluated trivially.  This can be done through the following ways:
 * 1) by eliding the filter for cases where it will always evaluate to `true`.
 * 2) by substituting a dummy empty relation when the filter will always evaluate to `false`.
 * 3) by eliminating the always-true conditions given the constraints on the child's output.
 */
// 对Filter LogicalPlan 所持有的 Expression 进行剪枝和优化
//
// 如果Filter逻辑判断整体结果为True，那么是可以删除这个Filter表达式
// 实例：select * from t where true or a>10 --> select * from t
//
// 如果Filter逻辑判断整体结果为False或者NULL，可以把整个plan返回data设置为Seq.empty，Scheme保持不变。
// 实例：select a from t where false --> LocalRelation <empty>, [a#655]
//
// 对于f @ Filter(fc, p: LogicalPlan)，如果fc中判断条件在Child Plan的约束下，肯定为Ture，那么就可以移除这个Filter判断，即Filter表达式与父表达式重叠。
// 实例：select b from (select b from t where a/b>10 and b=2) where b=2 --> select b from (select b from t where a/b>10 and b=2)
object PruneFilters extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // If the filter condition always evaluate to true, remove the filter.
    case Filter(Literal(true, BooleanType), child) => child
    // If the filter condition always evaluate to null or false,
    // replace the input with an empty relation.
    case Filter(Literal(null, _), child) => LocalRelation(child.output, data = Seq.empty)
    case Filter(Literal(false, BooleanType), child) => LocalRelation(child.output, data = Seq.empty)
    // If any deterministic condition is guaranteed to be true given the constraints on the child's
    // output, remove the condition
    case f @ Filter(fc, p: LogicalPlan) =>
      val (prunedPredicates, remainingPredicates) =
        splitConjunctivePredicates(fc).partition { cond =>
          cond.deterministic && p.constraints.contains(cond)
        }
      // 这里把 Filter 的 expressions 拆成了两部分
      // prunedPredicates：Filter 中 expressions 在 孩子LogicalPlan 中重叠的部分
      // remainingPredicates：孩子LogicalPlan 中的约束条件表达式 不重叠的部分
      if (prunedPredicates.isEmpty) {
        f  // 如果没有约束重叠，直接还是原来的f
      } else if (remainingPredicates.isEmpty) {
        p  // 如果 remainingPredicates 是空，说明没有不重叠的部分，也就是都重叠，那么直接干掉这个 Filter，因为这个Filter 没啥意义
      } else {
         // 如果既有重叠也有不重叠，那么就要把不重叠的部分部分抽出来
         // 然后重新构造 Filter 的 表达式部分
        val newCond = remainingPredicates.reduce(And)
        Filter(newCond, p)
      }
  }
}

/**
 * Pushes [[Filter]] operators through many operators iff:
 * 1) the operator is deterministic
 * 2) the predicate is deterministic and the operator will not change any of rows.
 *
 * This heuristic is valid assuming the expression evaluation cost is minimal.
 */
// 对于Filter操作，原则上它处于越底层越好，他可以显著减小后面计算的数据量。谓词下推
//
// filter @ Filter(condition, project @ Project(fields, grandChild))
// 实例：select rand(),a from (select * from t) where a>1 --> select rand(),a from t where a>1 //如果Project包含nondeterministic
// 实例：select rand(),a,id from (select *,spark_partition_id() as id from t) where a>1; //是无法进行这个优化。
//
// filter @ Filter(condition, aggregate: Aggregate) 对于Aggregate,Filter下移作用很明显。但不是所有的filter都可以下移，有些filter需要依赖整个aggregate最终的运行结果。如下所示
// 实例：select a,d from (select count(a) as d, a from t group by a) where a>1 and d>10 对于a>1和d>10两个Filter，显然a>1是可以下移一层，从而可以减小group by数据量。
// 而d>10显然不能，因此它优化以后的结果为 select a,d from (select count(a) as d, a from t where a>1 group by a) where d>10
//
// filter @ Filter(condition, union: Union)原理一样还有大部分的一元操作，比如Limit，都可以尝试把Filter下移，来进行优化。
// 实例：select * from (select * from t limit 10) where a>10
//
// 谓词下推，和谓词下推到 Join 不一样
// 谓词下推，就是把 Filter 下推到 子LogicalPlan
object PushDownPredicate extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // SPARK-13473: We can't push the predicate down when the underlying projection output non-
    // deterministic field(s).  Non-deterministic expressions are essentially stateful. This
    // implies that, for a given input row, the output are determined by the expression's initial
    // state and all the input rows processed before. In another word, the order of input rows
    // matters for non-deterministic expressions, while pushing down predicates changes the order.
    // Filter 下面是 Project
    // Filter 的 condition 能否下推到 Project 的孩子 LogicalPlan
    case filter @ Filter(condition, project @ Project(fields, grandChild))
      if fields.forall(_.deterministic) && canPushThroughCondition(grandChild, condition) =>

      // Create a map of Aliases to their values from the child projection.
      // e.g., 'SELECT a + b AS c, d ...' produces Map(c -> a + b).
      val aliasMap = AttributeMap(fields.collect {
        case a: Alias => (a.toAttribute, a.child)
      })

      project.copy(child = Filter(replaceAlias(condition, aliasMap), grandChild))

    // Push [[Filter]] operators through [[Window]] operators. Parts of the predicate that can be
    // pushed beneath must satisfy the following conditions:
    // 1. All the expressions are part of window partitioning key. The expressions can be compound.
    // 2. Deterministic.
    // 3. Placed before any non-deterministic predicates.
    // Filter 下面是 Window
    case filter @ Filter(condition, w: Window)
        if w.partitionSpec.forall(_.isInstanceOf[AttributeReference]) =>
      val partitionAttrs = AttributeSet(w.partitionSpec.flatMap(_.references))

      val (candidates, containingNonDeterministic) =
        splitConjunctivePredicates(condition).span(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        cond.references.subsetOf(partitionAttrs)
      }

      val stayUp = rest ++ containingNonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val newWindow = w.copy(child = Filter(pushDownPredicate, w.child))
        if (stayUp.isEmpty) newWindow else Filter(stayUp.reduce(And), newWindow)
      } else {
        filter
      }

    // Filter 下面是 Aggregate
    case filter @ Filter(condition, aggregate: Aggregate) =>
      // Find all the aliased expressions in the aggregate list that don't include any actual
      // AggregateExpression, and create a map from the alias to the expression
      val aliasMap = AttributeMap(aggregate.aggregateExpressions.collect {
        case a: Alias if a.child.find(_.isInstanceOf[AggregateExpression]).isEmpty =>
          (a.toAttribute, a.child)
      })

      // For each filter, expand the alias and check if the filter can be evaluated using
      // attributes produced by the aggregate operator's child operator.
      val (candidates, containingNonDeterministic) =
        splitConjunctivePredicates(condition).span(_.deterministic)

      val (pushDown, rest) = candidates.partition { cond =>
        val replaced = replaceAlias(cond, aliasMap)
        cond.references.nonEmpty && replaced.references.subsetOf(aggregate.child.outputSet)
      }

      val stayUp = rest ++ containingNonDeterministic

      if (pushDown.nonEmpty) {
        val pushDownPredicate = pushDown.reduce(And)
        val replaced = replaceAlias(pushDownPredicate, aliasMap)
        val newAggregate = aggregate.copy(child = Filter(replaced, aggregate.child))
        // If there is no more filter to stay up, just eliminate the filter.
        // Otherwise, create "Filter(stayUp) <- Aggregate <- Filter(pushDownPredicate)".
        if (stayUp.isEmpty) newAggregate else Filter(stayUp.reduce(And), newAggregate)
      } else {
        filter
      }

    // Filter 下面是 Union
    case filter @ Filter(condition, union: Union) =>
      // Union could change the rows, so non-deterministic predicate can't be pushed down
      // 分解成两部分，可以下推的 和 还要保持在 Filter 里的 condition
      val (pushDown, stayUp) = splitConjunctivePredicates(condition).span(_.deterministic)

      // Union 有多个孩子LogicalPlan
      // pushDown 就是要下推的部分 condition
      // 然后挨个 孩子LogicalPlan遍历，确认下推的部分扔到哪个孩子去
      if (pushDown.nonEmpty) {
        val pushDownCond = pushDown.reduceLeft(And)
        val output = union.output
        val newGrandChildren = union.children.map { grandchild =>
          val newCond = pushDownCond transform {
            case e if output.exists(_.semanticEquals(e)) =>
              grandchild.output(output.indexWhere(_.semanticEquals(e)))
          }
          assert(newCond.references.subsetOf(grandchild.outputSet))
          Filter(newCond, grandchild)
        }
        val newUnion = union.withNewChildren(newGrandChildren)
        if (stayUp.nonEmpty) {
          Filter(stayUp.reduceLeft(And), newUnion)
        } else {
          newUnion
        }
      } else {
        filter
      }

    // Filter 下面是其他 单孩子的 LogicalPlan
    case filter @ Filter(condition, u: UnaryNode)
        // canPushThrough 判断特定类型的 LogicalPlan 能不能做下推
        // 同时还要满足 这个 LogicalPlan 的 所有 expressions 都是 deterministic
        if canPushThrough(u) && u.expressions.forall(_.deterministic) =>
      // pushDownPredicate 是一个内部方法，把Filter 的 condition 下推到 当前 LogicalPlan 的 孩子 LogicalPlan
      // 意味着，这个下推向下了2层
      pushDownPredicate(filter, u.child) { predicate =>
        // withNewChildren 就是用 Seq(Filter(predicate, u.child)) 这批 孩子LogicalPlan
        // 替换原来 u 这个 LogicalPlan 的孩子
        // 如果下推成功的话，就是用下推的 predicate（condition），去在原来的 孙子 LogicalPlan 上套一层 Filter
        u.withNewChildren(Seq(Filter(predicate, u.child)))
      }
  }

  private def canPushThrough(p: UnaryNode): Boolean = p match {
    // 像 Project Window Aggregate Union 这些其实也可以返回 True
    // 但是单独抽出来提前进行特殊处理了
    // Note that some operators (e.g. project, aggregate, union) are being handled separately
    // (earlier in this rule).
    case _: AppendColumns => true
    case _: BroadcastHint => true
    case _: Distinct => true
    case _: Generate => true
    case _: Pivot => true
    case _: RedistributeData => true
    case _: Repartition => true
    case _: ScriptTransformation => true
    case _: Sort => true
    case _ => false
  }

  // 结构是这样的
  // Filter -> 当前的 UnaryNode LogicalPlan -> 当前 UnaryNode LogicalPlan 的 孩子 LogicalPlan
  // 就是要把 Filter 的 condition 向下推2层，
  // 到达
  // 当前 UnaryNode LogicalPlan 的 孩子 LogicalPlan
  // 柯里化函数，可以认为3个参数
  // 1 filter：Filter
  // 2 grandchild：filter 的 孩子LogicalPlan 的 孩子LogicalPlan（也就是 filter 的孙子LogicalPlan）
  // 3 insertFilter：一个函数，入参 Expression，输出 LogicalPlan
  // 整个方法最终返回一个新的 LogicalPlan
  private def pushDownPredicate(
      filter: Filter,
      grandchild: LogicalPlan)(insertFilter: Expression => LogicalPlan): LogicalPlan = {
    // Only push down the predicates that is deterministic and all the referenced attributes
    // come from grandchild.
    // TODO: non-deterministic predicates could be pushed through some operators that do not change
    // the rows.

    // 首先 拆分 Filter 中的 condition
    val (candidates, containingNonDeterministic) =
      splitConjunctivePredicates(filter.condition).span(_.deterministic)

    // 然后看看哪些condition 可以被下推到 孙子 LogicalPlan
    // pushDown 是可以下推的 condition
    val (pushDown, rest) = candidates.partition { cond =>
      cond.references.subsetOf(grandchild.outputSet)
    }

    // 仍然保存在 Filter 中不下推的 condition
    val stayUp = rest ++ containingNonDeterministic

    if (pushDown.nonEmpty) {
      // 用传进来的函数 ，传入 下推的 condition，构建被下推的 新的 孙子 LogicalPlan
      val newChild = insertFilter(pushDown.reduceLeft(And))
      // 用不下推的 condition 构建新的 Filter
      if (stayUp.nonEmpty) {
        Filter(stayUp.reduceLeft(And), newChild)
      } else {
      // 如果都下推了，原来的 Filter 就可以干掉了
        newChild
      }
    } else {
      filter
    }
  }

  /**
   * Check if we can safely push a filter through a projection, by making sure that predicate
   * subqueries in the condition do not contain the same attributes as the plan they are moved
   * into. This can happen when the plan and predicate subquery have the same source.
   */
  //  plan ：Filter 的 孙子 LogicalPlan
  // condition ： Filter 中的 condition
  // 判断Filter 中的 condition 能否下推到 Filter 的孙子LogicalPlan
  // 我理解的，例如 select field1 from table1 where table1.field2 > 0
  // 就会有这样的结构
  // Filter -> Project -> Relation
  // Filter 中的 condition 中 包含了 LogicalPlan（PredicateSubquery 是 SubqueryExpression 的子类，SubqueryExpression 就是包含 LogicalPlan 的 Expression）
  // Filter 中的 condition 包含的 LogicalPlan 是一个 Relation，他的output 和 Filter 的 孙子 LogicalPlan 的 outputSet有重合
  // 所以这个 condition 可以加入 matched
  // 最终 matched 不为空，说明，Filter 的 condition 可以下推到这个Filter 的 孙子 LogicalPlan
  private def canPushThroughCondition(plan: LogicalPlan, condition: Expression): Boolean = {
    val attributes = plan.outputSet
    val matched = condition.find {
      case PredicateSubquery(p, _, _, _) => p.outputSet.intersect(attributes).nonEmpty
      case _ => false
    }
    matched.isEmpty
  }
}

/**
 * Pushes down [[Filter]] operators where the `condition` can be
 * evaluated using only the attributes of the left or right side of a join.  Other
 * [[Filter]] conditions are moved into the `condition` of the [[Join]].
 *
 * And also pushes down the join filter, where the `condition` can be evaluated using only the
 * attributes of the left or right side of sub query when applicable.
 *
 * Check https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior for more details
 */
// 谓词下推到 Join 算子
// 该优化规则是将Filter中的条件下移到Join算子中
object PushPredicateThroughJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Splits join condition expressions or filter predicates (on a given join's output) into three
   * categories based on the attributes required to evaluate them. Note that we explicitly exclude
   * on-deterministic (i.e., stateful) condition expressions in canEvaluateInLeft or
   * canEvaluateInRight to prevent pushing these predicates on either side of the join.
   *
   * @return (canEvaluateInLeft, canEvaluateInRight, haveToEvaluateInBoth)
   */
  // 所以 split 方法的三个参数分别是：
  // 如果是 Filter 包住 Join
  // 1 Seq[Expression], Filter LogicalPlan 的 condition 按照 AND 切分后形成的 List
  // 2 left LogicalPlan
  // 3 right LogicalPlan
  // 如果是 Join 自己
  // 1 Join 的 condition 按照 AND 拆分形成的list
  // 2 left LogicalPlan
  // 3 right LogicalPlan
  private def split(condition: Seq[Expression], left: LogicalPlan, right: LogicalPlan) = {
    // Note: In order to ensure correctness, it's important to not change the relative ordering of
    // any deterministic expression that follows a non-deterministic expression. To achieve this,
    // we only consider pushing down those expressions that precede the first non-deterministic
    // expression in the condition.
    //
    // list 的 span 方法根据输入的bool表达式，将list进行分割。返回一个list集合。但是碰到第一个不满足的元素，即返回。如：
    // 例如 val a = List[1,1,2,3,4]
    // a.span(_ == 1) 则返回 [1, 1] 和 [2,3,4]
    // 这里其实要保证前半截是 deterministic = true的
    // 这里分成了两个部分：pushDownCandidates 和 containingNonDeterministic
    // deterministic = true 的 list，就是 pushDownCandidates，就会成为下推的候选集
    val (pushDownCandidates, containingNonDeterministic) = condition.span(_.deterministic)
    // 这里有个 partition 函数，符合 partition 函数的条件的list 和 不符合条件的list 组成一个二元组
    // 把下推的候选集 和 left LogicalPlan 的 output 比较，如果和 output 匹配，这个 下推候选条件 就可以下推到 left
    val (leftEvaluateCondition, rest) =
      pushDownCandidates.partition(_.references.subsetOf(left.outputSet))
    // 从剩下的 候选集中 匹配可以下推到 right 的
    val (rightEvaluateCondition, commonCondition) =
        rest.partition(expr => expr.references.subsetOf(right.outputSet))
    // 分成了 3部分，可以下推到 left 的，可以下推到right 的，没办法下推的
    (leftEvaluateCondition, rightEvaluateCondition, commonCondition ++ containingNonDeterministic)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // push the where condition down into join filter
    // 第 1 种情况，Filter LogicalPlan 下面就是 Join LogicalPlan
    // 把 where 条件 下推到 join 的 filter
    case f @ Filter(filterCondition, Join(left, right, joinType, joinCondition)) =>
      val (leftFilterConditions, rightFilterConditions, commonFilterCondition) =
        // splitConjunctivePredicates 方法传入一个 Expression，如果这个 Expression 是用 And 连接的，拆成两份，继续递归下去
        // 最终得到 一个 Seq[Expression],其实就是把 AND 连接的条件拆开
        // 所以 split 方法的三个参数分别是：
        // 1 Seq[Expression],Filter LogicalPlan 的 condition 按照 AND 切分后形成的 List
        // 2 left LogicalPlan
        // 3 right LogicalPlan
        // 分成了 3部分，可以下推到 left 的，可以下推到right 的，没办法下推的
        split(splitConjunctivePredicates(filterCondition), left, right)

      // 然后去匹配 join 的类型
      joinType match {
        case _: InnerLike =>
          // push down the single side `where` condition into respective sides
          // 把 下推到 left 的 条件用 AND 连起来，然后在 left 上套个 FILTER LogicalPlan，就是一个新的 left
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          // 同理，右边也是这样
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          // 然后用新的 Join 合并 left 和 right
          // condition 也是新的
          // 再套个新的 Filter，过滤的 condition 也是新的
          val (newJoinConditions, others) =
            commonFilterCondition.partition(e => !SubqueryExpression.hasCorrelatedSubquery(e))
          val newJoinCond = (newJoinConditions ++ joinCondition).reduceLeftOption(And)

          val join = Join(newLeft, newRight, joinType, newJoinCond)
          if (others.nonEmpty) {
            Filter(others.reduceLeft(And), join)
          } else {
            join
          }
        //   RightOuter 或者 LeftOuter，只下推到 一边
        case RightOuter =>
          // push down the right side only `where` condition
          val newLeft = left
          val newRight = rightFilterConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, RightOuter, newJoinCond)

          (leftFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        case LeftOuter | LeftExistence(_) =>
          // push down the left side only `where` condition
          val newLeft = leftFilterConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = joinCondition
          val newJoin = Join(newLeft, newRight, joinType, newJoinCond)

          (rightFilterConditions ++ commonFilterCondition).
            reduceLeftOption(And).map(Filter(_, newJoin)).getOrElse(newJoin)
        // FullOuter 啥都不干
        case FullOuter => f // DO Nothing for Full Outer Join
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }

    // push down the join filter into sub query scanning if applicable
    // 第 2 种情况，直接就是一个 Join LogicalPlan
    case j @ Join(left, right, joinType, joinCondition) =>
      val (leftJoinConditions, rightJoinConditions, commonJoinCondition) =
        // 这里同样给 split 这个内部方法传3个参数
        // 1 Join 的 condition 按照 AND 拆分形成的list
        // 2 left LogicalPlan
        // 3 right LogicalPlan
        split(joinCondition.map(splitConjunctivePredicates).getOrElse(Nil), left, right)

      // 和上面的差不多
      joinType match {
        case _: InnerLike | LeftSemi =>
          // push down the single side only join filter for both sides sub queries
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = commonJoinCondition.reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case RightOuter =>
          // push down the left side only join filter for left side sub query
          val newLeft = leftJoinConditions.
            reduceLeftOption(And).map(Filter(_, left)).getOrElse(left)
          val newRight = right
          val newJoinCond = (rightJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, RightOuter, newJoinCond)
        case LeftOuter | LeftAnti | ExistenceJoin(_) =>
          // push down the right side only join filter for right sub query
          val newLeft = left
          val newRight = rightJoinConditions.
            reduceLeftOption(And).map(Filter(_, right)).getOrElse(right)
          val newJoinCond = (leftJoinConditions ++ commonJoinCondition).reduceLeftOption(And)

          Join(newLeft, newRight, joinType, newJoinCond)
        case FullOuter => j
        case NaturalJoin(_) => sys.error("Untransformed NaturalJoin node")
        case UsingJoin(_, _) => sys.error("Untransformed Using join node")
      }
  }
}

/**
 * Combines two adjacent [[Limit]] operators into one, merging the
 * expressions into one single expression.
 */
// 临近的 Limit 操作的 合并。针对GlobalLimit，LocalLimit，Limit三个，如果连续多次，会选择最小的一次limit来进行合并
// 实例：select * from (select * from t limit 10) limit 5 --> select * from t limit 5
// 实例：select * from (select * from t limit 5) limit 10 --> select * from t limit 5
//
// case 中，两次的 limit 操作对应的 LogicalPlan 并不联在一起，所以不是直接 合并的
// 但是两个 Project 会进行合并，然后 Limit 操作就
object CombineLimits extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case GlobalLimit(le, GlobalLimit(ne, grandChild)) =>
      GlobalLimit(Least(Seq(ne, le)), grandChild)
    case LocalLimit(le, LocalLimit(ne, grandChild)) =>
      LocalLimit(Least(Seq(ne, le)), grandChild)
    case Limit(le, Limit(ne, grandChild)) =>
      Limit(Least(Seq(ne, le)), grandChild)
  }
}

/**
 * Check if there any cartesian products between joins of any type in the optimized plan tree.
 * Throw an error if a cartesian product is found without an explicit cross join specified.
 * This rule is effectively disabled if the CROSS_JOINS_ENABLED flag is true.
 *
 * This rule must be run AFTER the ReorderJoin rule since the join conditions for each join must be
 * collected before checking if it is a cartesian product. If you have
 * SELECT * from R, S where R.r = S.s,
 * the join between R and S is not a cartesian product and therefore should be allowed.
 * The predicate R.r = S.s is not recognized as a join condition until the ReorderJoin rule.
 */
// 检查 LogicalPlan tree 中是否包含 笛卡尔积 类型的 Join 操作。如果存在这样的操作，但是在sql语句中又没有显示的使用 cross join，那么就会抛出异常
// 所以这个优化必须在 RecordJoin 规则执行之后才能执行
case class CheckCartesianProducts(conf: CatalystConf)
    extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Check if a join is a cartesian product. Returns true if
   * there are no join conditions involving references from both left and right.
   */
  // 遍历看表达式中是否包含 cross join 的信息
  def isCartesianProduct(join: Join): Boolean = {
    val conditions = join.condition.map(splitConjunctivePredicates).getOrElse(Nil)
    !conditions.map(_.references).exists(refs => refs.exists(join.left.outputSet.contains)
        && refs.exists(join.right.outputSet.contains))
  }

  def apply(plan: LogicalPlan): LogicalPlan =
    if (conf.crossJoinEnabled) {
      plan
    } else plan transform {
      case j @ Join(left, right, Inner | LeftOuter | RightOuter | FullOuter, condition)
        if isCartesianProduct(j) =>
          throw new AnalysisException(
            s"""Detected cartesian product for ${j.joinType.sql} join between logical plans
               |${left.treeString(false).trim}
               |and
               |${right.treeString(false).trim}
               |Join condition is missing or trivial.
               |Use the CROSS JOIN syntax to allow cartesian products between these relations."""
            .stripMargin)
    }
}

/**
 * Speeds up aggregates on fixed-precision decimals by executing them on unscaled Long values.
 *
 * This uses the same rules for increasing the precision and scale of the output as
 * [[org.apache.spark.sql.catalyst.analysis.DecimalPrecision]].
 */
// 用于处理聚合操作中 和 Decimal 类型相关的问题。在聚合查询中，如果涉及浮点数的精度处理，性能有影响。固定精度的 Decimal 类型，可以加速聚合操作的速度

// 看起来就是用固定精度 去进行聚合操作
object DecimalAggregates extends Rule[LogicalPlan] {
  import Decimal.MAX_LONG_DIGITS

  /** Maximum number of decimal digits representable precisely in a Double */
  private val MAX_DOUBLE_DIGITS = 15

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case q: LogicalPlan => q transformExpressionsDown {
      case we @ WindowExpression(ae @ AggregateExpression(af, _, _, _), _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(we.copy(windowFunction = ae.copy(aggregateFunction = Sum(UnscaledValue(e)))),
            prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr =
            we.copy(windowFunction = ae.copy(aggregateFunction = Average(UnscaledValue(e))))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4))

        case _ => we
      }
      case ae @ AggregateExpression(af, _, _, _) => af match {
        case Sum(e @ DecimalType.Expression(prec, scale)) if prec + 10 <= MAX_LONG_DIGITS =>
          MakeDecimal(ae.copy(aggregateFunction = Sum(UnscaledValue(e))), prec + 10, scale)

        case Average(e @ DecimalType.Expression(prec, scale)) if prec + 4 <= MAX_DOUBLE_DIGITS =>
          val newAggExpr = ae.copy(aggregateFunction = Average(UnscaledValue(e)))
          Cast(
            Divide(newAggExpr, Literal.create(math.pow(10.0, scale), DoubleType)),
            DecimalType(prec + 4, scale + 4))

        case _ => ae
      }
    }
  }
}

/**
 * Converts local operations (i.e. ones that don't require data exchange) on LocalRelation to
 * another LocalRelation.
 *
 * This is relatively simple as it currently handles only a single case: Project.
 */
 // 将Project中的每个无需数据交换的表达式应用于LocalRelation中的相应元素，
 // 例如select a + 1 from tb1转为一个属性名为“a+1”的relation，并且tb1.a的每个值都是加过1的
 // 而且 tb1 也是一个 LocalRelation
 // 优化之前，有 Project 和 LocalRelation 两层 LogicalPlan
 // 优化之后，只有 LocalRelation 这一层 LogicalPlan
object ConvertToLocalRelation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Project(projectList, LocalRelation(output, data))
        if !projectList.exists(hasUnevaluableExpr) =>
      val projection = new InterpretedProjection(projectList, output)
      projection.initialize(0)
      LocalRelation(projectList.map(_.toAttribute), data.map(projection))
  }

  private def hasUnevaluableExpr(expr: Expression): Boolean = {
    expr.find(e => e.isInstanceOf[Unevaluable] && !e.isInstanceOf[AttributeReference]).isDefined
  }
}

/**
 * Replaces logical [[Distinct]] operator with an [[Aggregate]] operator.
 * {{{
 *   SELECT DISTINCT f1, f2 FROM t  ==>  SELECT f1, f2 FROM t GROUP BY f1, f2
 * }}}
 */
// 用 Aggregate LogicalPlan 替换 Distinct LogicalPlan
object ReplaceDistinctWithAggregate extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Distinct(child) => Aggregate(child.output, child.output, child)
  }
}

/**
 * Replaces logical [[Intersect]] operator with a left-semi [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 INTERSECT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT SEMI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to INTERSECT DISTINCT. Do not use it for INTERSECT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
// 用LEFT SemiJoin操作来替换“Intersect distinct”操作，注意不针对"Intersect all"
object ReplaceIntersectWithSemiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Intersect(left, right) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftSemi, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Replaces logical [[Except]] operator with a left-anti [[Join]] operator.
 * {{{
 *   SELECT a1, a2 FROM Tab1 EXCEPT SELECT b1, b2 FROM Tab2
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 LEFT ANTI JOIN Tab2 ON a1<=>b1 AND a2<=>b2
 * }}}
 *
 * Note:
 * 1. This rule is only applicable to EXCEPT DISTINCT. Do not use it for EXCEPT ALL.
 * 2. This rule has to be done after de-duplicating the attributes; otherwise, the generated
 *    join conditions will be incorrect.
 */
// LeftAnti join 操作来替换“except distinct”操作，注意不针对"except all"
object ReplaceExceptWithAntiJoin extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right) =>
      assert(left.output.size == right.output.size)
      val joinCond = left.output.zip(right.output).map { case (l, r) => EqualNullSafe(l, r) }
      Distinct(Join(left, right, LeftAnti, joinCond.reduceLeftOption(And)))
  }
}

/**
 * Removes literals from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
// 去除 group 表达式中的 字面量，因为不会对结果造成影响
object RemoveLiteralFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) if grouping.nonEmpty =>
      val newGrouping = grouping.filter(!_.foldable)
      if (newGrouping.nonEmpty) {
        a.copy(groupingExpressions = newGrouping)
      } else {
        // All grouping expressions are literals. We should not drop them all, because this can
        // change the return semantics when the input of the Aggregate is empty (SPARK-17114). We
        // instead replace this by single, easy to hash/sort, literal expression.
        a.copy(groupingExpressions = Seq(Literal(0, IntegerType)))
      }
  }
}

/**
 * Removes repetition from group expressions in [[Aggregate]], as they have no effect to the result
 * but only makes the grouping key bigger.
 */
// 去除 group by 中相同的表达式
object RemoveRepetitionFromGroupExpressions extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case a @ Aggregate(grouping, _, _) =>
      val newGrouping = ExpressionSet(grouping).toSeq
      a.copy(groupingExpressions = newGrouping)
  }
}
