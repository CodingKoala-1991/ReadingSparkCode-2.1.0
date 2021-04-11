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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.CatalystConf
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin.withOrigin
import org.apache.spark.sql.types.IntegerType

/**
 * Replaces ordinal in 'order by' or 'group by' with UnresolvedOrdinal expression.
 */
class SubstituteUnresolvedOrdinals(conf: CatalystConf) extends Rule[LogicalPlan] {
  private def isIntLiteral(e: Expression) = e match {
    case Literal(_, IntegerType) => true
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    // group by 1,2,3 这种，或者 order by 1, 2, 3 这种
    case s: Sort if conf.orderByOrdinal && s.order.exists(o => isIntLiteral(o.child)) =>
      // s.order 返回的数据结构是 Seq[SortOrder]
      // 现在就替换成了 Seq[UnresolvedOrdinal]
      // UnresolvedOrdinal 这个 case class 只有一个属性，就是 index
      val newOrders = s.order.map {
        case order @ SortOrder(ordinal @ Literal(index: Int, IntegerType), _, _) =>
          val newOrdinal = withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
          withOrigin(order.origin)(order.copy(child = newOrdinal))
        case other => other
      }
      withOrigin(s.origin)(s.copy(order = newOrders))

    case a: Aggregate if conf.groupByOrdinal && a.groupingExpressions.exists(isIntLiteral) =>
      val newGroups = a.groupingExpressions.map {
        // 在 group by 语句中，如果匹配出 Literal 这种 Expression，且属性是 index 的这种 Int的
        // 那么就要替换成 UnresolvedOrdinal 这种Expression
        case ordinal @ Literal(index: Int, IntegerType) =>
          withOrigin(ordinal.origin)(UnresolvedOrdinal(index))
        case other => other
      }
      withOrigin(a.origin)(a.copy(groupingExpressions = newGroups))
  }
}
