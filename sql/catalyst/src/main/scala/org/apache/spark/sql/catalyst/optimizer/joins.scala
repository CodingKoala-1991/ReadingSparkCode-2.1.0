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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractFiltersAndInnerJoins
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 */
// 把所有的条件表达式分配到join的子树中，使每个子树至少有一个条件表达式。
// 重排序的顺序依据条件顺序，例如：select * from tb1,tb2,tb3 where tb1.a=tb3.c and tb3.b=tb2.b，
// 那么join的顺序就是join(tb1,tb3,tb1.a=tb3.c)，join(tb3,tb2,tb3.b=tb2.b)
// 这种优化策略处理的是一种特殊的 结构
// 自底向上，
// Join 和 Filter 交替出现
// 这个时候就要考虑如何把 Filter 中的condition 有效的下推到 各个 Join 的子树中
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  def createOrderedJoin(input: Seq[(LogicalPlan, InnerLike)], conditions: Seq[Expression])
    : LogicalPlan = {
    assert(input.size >= 2)
    // 如果 input 的 size == 2，说明只有一个 Inner Join
    // 那么结构就是 Filter -> Join -> left LogicalPlan
    //                           -> right LogicalPlan
    // 那么下推就和之前 Filter 下推到 Join 一样
    // 找出一些合适的下推到 left 或者 right
    // 也有一些会依然保留在 Filter
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(
        e => !SubqueryExpression.hasCorrelatedSubquery(e))
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    // 至少不是只有 一个 Inner Join
    // input 从左到右遍历，就是这个新的 LogicalPlan tree 自底向上 递归构建
    } else {
      // input 中和 left 相邻的，肯定是和 这个left 进行 Join 的 right
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join
      // 然后找出 和 这个 left 、right 相关的 condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      // left 和 right 的output，作为 condition 能不能下推的参考条件
      val joinedRefs = left.outputSet ++ right.outputSet
      // 将当前 condition 分成两部分
      // 新的 Join 使用的condition，以及当前 Join 还用不上的
      val (joinConditions, others) = conditions.partition(
        e => e.references.subsetOf(joinedRefs) && !SubqueryExpression.hasCorrelatedSubquery(e))
      // 构建新的 Join 节点
      val joined = Join(left, right, innerJoinType, joinConditions.reduceLeftOption(And))

      // should not have reference to same logical plan
      // 然后递归构建，传入的input 是新的 Join + 去掉了right 的序列
      // conditions 也减少了，变成了 others
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

/**
 * A pattern that collects the filter and inner joins.
 *
 *          Filter
 *            |
 *        inner Join
 *          /    \            ---->      (Seq(plan0, plan1, plan2), conditions)
 *      Filter   plan2
 *        |
 *  inner join
 *      /    \
 *   plan0    plan1
 */
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    //  ExtractFiltersAndInnerJoin并不是一个节点，而是一种具体形式的子树。
    //  实际上该类的作用就是“解构”子树，并无其他作用，所以其被调用到的方法只有unapply。这棵子树的结构就是Filter和InnerJoin交替出现。
    // ExtractFiltersAndInnerJoins 就是 解构之后的 class
    // 解构为 (Seq(plan0, plan1, plan2), conditions)
    //
    // 这里的 input 就是 一个 Seq，表明 原来这个LogicalPna tree 中 有多少个 LogicalPlan
    case j @ ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      createOrderedJoin(input, conditions)
  }
}

/**
 * Elimination of outer joins, if the predicates can restrict the result sets so that
 * all null-supplying rows are eliminated
 *
 * - full outer -> inner if both sides have such predicates
 * - left outer -> inner if the right side has such predicates
 * - right outer -> inner if the left side has such predicates
 * - full outer -> left outer if only the left side has such predicates
 * - full outer -> right outer if only the right side has such predicates
 *
 * This rule should be executed before pushing down the Filter
 */
 // 把 OUTER JOIN 转为等价的 非 OUTER Join，可以减少数据量
 // 但其实我没怎么看懂
object EliminateOuterJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether the expression returns null or false when all inputs are nulls.
   */
  private def canFilterOutNull(e: Expression): Boolean = {
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    val attributes = e.references.toSeq
    val emptyRow = new GenericInternalRow(attributes.length)
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.find(_.isInstanceOf[Unevaluable]).isDefined) return false
    val v = boundE.eval(emptyRow)
    v == null || v == false
  }

  private def buildNewJoinType(filter: Filter, join: Join): JoinType = {
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if rightHasNonNullPredicate => RightOuter
      case o => o
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _)) =>
      val newJoinType = buildNewJoinType(f, j)
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))
  }
}
