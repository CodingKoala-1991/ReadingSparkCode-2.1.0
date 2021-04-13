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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types._

/**
 * A placeholder expression for cube/rollup, which will be replaced by analyzer
 */
trait GroupingSet extends Expression with CodegenFallback {

  def groupByExprs: Seq[Expression]
  override def children: Seq[Expression] = groupByExprs

  // this should be replaced first
  override lazy val resolved: Boolean = false

  override def dataType: DataType = throw new UnsupportedOperationException
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def eval(input: InternalRow): Any = throw new UnsupportedOperationException
}

case class Cube(groupByExprs: Seq[Expression]) extends GroupingSet {}

case class Rollup(groupByExprs: Seq[Expression]) extends GroupingSet {}

/**
 * Indicates whether a specified column expression in a GROUP BY list is aggregated or not.
 * GROUPING returns 1 for aggregated or 0 for not aggregated in the result set.
 */
// 样例
// Select country, city, sum(sales) as total_sales
//      GROUPING(country) as GP_country,
//      GROUPING(city) as GP_city
//      from sales_history
//      GROUP BY ROLLUP (country, city);
//
//      +--------+----------+-------------+-------------+------------+
//      | country| city     | total_sales | GP_country  | GP_city    |
//      +--------+----------+-------------+-------------+------------+
//      | US     | San Jose | 1000        | 0           | 0          |
//      | US     | Fremont  | 2000        | 0           | 0          |
//      | US     | NULL     | 3000        | 0           | 1          |
//      | Japan  | Hiroshima| 5000        | 0           | 0          |
//      | Japan  | Tokyo    | 3000        | 0           | 0          |
//      | Japan  | NULL     | 4000        | 0           | 0          |
//      | Japan  | NULL     | 12000       | 0           | 1          |
//      | NULL   | NULL     | 15000       | 1           | 1          |
//      +--------+----------+-------------+-------------+------------+
//
// GP_country 来自 GROUPING(country)， GP_country = 0，表示在多维分析的时候，这一行 country 这个字段是被引入进行维度拆分的
// GP_city 来自 GROUPING(city) as GP_city
//
// 第6行，| Japan  | NULL     | 4000        | 0           | 0          | 这一行，表示 来自日本的，其他城市的 销售总额
// city 这一列为 NULL，不是代表着 city 没有作为 维度拆分的 一个字段
// 第7行，| Japan  | NULL     | 12000       | 0           | 1          |，city 这一列NULL，真的表示 city 没有参与维度拆分，所以 GP_city = 1
case class Grouping(child: Expression) extends Expression with Unevaluable {
  override def references: AttributeSet = AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = child :: Nil
  override def dataType: DataType = ByteType
  override def nullable: Boolean = false
}

/**
 * GroupingID is a function that computes the level of grouping.
 *
 * If groupByExprs is empty, it means all grouping expressions in GroupingSets.
 */
// 根据字段有没有 参与到 维度拆分里，生成一个 掩码 一样的数字，就是二进制的每一个bit 表示一个字段有没有参与
case class GroupingID(groupByExprs: Seq[Expression]) extends Expression with Unevaluable {
  override def references: AttributeSet = AttributeSet(VirtualColumn.groupingIdAttribute :: Nil)
  override def children: Seq[Expression] = groupByExprs
  override def dataType: DataType = IntegerType
  override def nullable: Boolean = false
  override def prettyName: String = "grouping_id"
}
