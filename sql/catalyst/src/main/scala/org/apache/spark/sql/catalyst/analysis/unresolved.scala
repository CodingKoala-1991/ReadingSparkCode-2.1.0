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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.errors.TreeNodeException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodegenFallback, ExprCode}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.types.{DataType, Metadata, StructType}

/**
 * Thrown when an invalid attempt is made to access a property of a tree that has yet to be fully
 * resolved.
 */
class UnresolvedException[TreeType <: TreeNode[_]](tree: TreeType, function: String)
  extends TreeNodeException(tree, s"Invalid call to $function on unresolved object", null)

/**
 * Holds the name of a relation that has yet to be looked up in a catalog.
 */
// 一张表，如果没有catalog 中的信息解析
case class UnresolvedRelation(
// 构造需要两个参数
// tableIdentifier： 表示这个 unresolved 的 relation 对应的表的名称，但是并没有真正的绑定到 Catalog 上具体的真实表。
// alias：表的别名
    tableIdentifier: TableIdentifier,
    alias: Option[String] = None) extends LeafNode {

  /** Returns a `.` separated name for this relation. */
  def tableName: String = tableIdentifier.unquotedString

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

/**
 * An inline table that has not been resolved yet. Once resolved, it is turned by the analyzer into
 * a [[org.apache.spark.sql.catalyst.plans.logical.LocalRelation]].
 *
 * @param names list of column names
 * @param rows expressions for the data
 */
// inline table，就是用 VALUES 关键字 初始化的表
// 样例
// SELECT * FROM VALUES ("one", 1);
// +----+----+
// |col1|col2|
// +----+----+
// | one|   1|
// +----+----+
//
// SELECT * FROM VALUES ("one", array(0, 1)), ("two", array(2, 3)) AS data(a, b);
// +---+------+
// |  a|     b|
// +---+------+
// |one|[0, 1]|
// |two|[2, 3]|
// +---+------+
//
// names：每一列的列名，如果没有指定列名，似乎就是用下标数字表示
// rows：其实就是整个inline table 的数据，是一个二维数组，数组的每一个元素都用 Expression 来表示
case class UnresolvedInlineTable(
    names: Seq[String],
    rows: Seq[Seq[Expression]])  // 构造的时候需要多个 Expression
  extends LeafNode {

  lazy val expressionsResolved: Boolean = rows.forall(_.forall(_.resolved))
  override lazy val resolved = false
  override def output: Seq[Attribute] = Nil
}

/**
 * A table-valued function, e.g.
 * {{{
 *   select * from range(10);
 * }}}
 */
// 用函数表示的 table
// 例如上面整个样例： functionName = range，functionArgs = [10]
case class UnresolvedTableValuedFunction(functionName: String, functionArgs: Seq[Expression])
  extends LeafNode {
  // 函数表达式做成 table
  // 例如这里 range(10) 就是一个数据源

  override def output: Seq[Attribute] = Nil

  override lazy val resolved = false
}

/**
 * Holds the name of an attribute that has yet to be resolved.
 */
// 没有被解析的 属性，或者说叫做没有被解析的列
//  nameParts：用来标示列的，比如 table1.field1     table1.field1.key1
case class UnresolvedAttribute(nameParts: Seq[String]) extends Attribute with Unevaluable {

  def name: String =
    nameParts.map(n => if (n.contains(".")) s"`$n`" else n).mkString(".")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override lazy val resolved = false

  override def newInstance(): UnresolvedAttribute = this
  override def withNullability(newNullability: Boolean): UnresolvedAttribute = this
  override def withQualifier(newQualifier: Option[String]): UnresolvedAttribute = this
  override def withName(newName: String): UnresolvedAttribute = UnresolvedAttribute.quoted(newName)
  override def withMetadata(newMetadata: Metadata): Attribute = this

  override def toString: String = s"'$name"

  override def sql: String = quoteIdentifier(name)
}

object UnresolvedAttribute {
  /**
   * Creates an [[UnresolvedAttribute]], parsing segments separated by dots ('.').
   */
  // 个人理解，在创建 UnresolvedAttribute 的时候
  // 传入的字段，比如是 table1.column1 是一个完整的字符串
  // 但是在下面这个伴生对象的时候，会首先根据  .   分割得到一个list
  // list 在这里就是 [table1, column1]
  // 然后用这个list 去初始化一个真正的 UnresolvedAttribute 对象
  // 这个list 其实就是对应 UnresolvedAttribute 里的 nameParts 这个属性
  def apply(name: String): UnresolvedAttribute = new UnresolvedAttribute(name.split("\\."))

  /**
   * Creates an [[UnresolvedAttribute]], from a single quoted string (for example using backticks in
   * HiveQL.  Since the string is consider quoted, no processing is done on the name.
   */
  // `field` 这种形式的 字段
  def quoted(name: String): UnresolvedAttribute = new UnresolvedAttribute(Seq(name))

  /**
   * Creates an [[UnresolvedAttribute]] from a string in an embedded language.  In this case
   * we treat it as a quoted identifier, except for '.', which must be further quoted using
   * backticks if it is part of a column name.
   */
  def quotedString(name: String): UnresolvedAttribute =
    new UnresolvedAttribute(parseAttributeName(name))

  /**
   * Used to split attribute name by dot with backticks rule.
   * Backticks must appear in pairs, and the quoted string must be a complete name part,
   * which means `ab..c`e.f is not allowed.
   * Escape character is not supported now, so we can't use backtick inside name part.
   */
  def parseAttributeName(name: String): Seq[String] = {
    def e = new AnalysisException(s"syntax error in attribute name: $name")
    val nameParts = scala.collection.mutable.ArrayBuffer.empty[String]
    val tmp = scala.collection.mutable.ArrayBuffer.empty[Char]
    var inBacktick = false
    var i = 0
    while (i < name.length) {
      val char = name(i)
      if (inBacktick) {
        if (char == '`') {
          inBacktick = false
          if (i + 1 < name.length && name(i + 1) != '.') throw e
        } else {
          tmp += char
        }
      } else {
        if (char == '`') {
          if (tmp.nonEmpty) throw e
          inBacktick = true
        } else if (char == '.') {
          if (name(i - 1) == '.' || i == name.length - 1) throw e
          nameParts += tmp.mkString
          tmp.clear()
        } else {
          tmp += char
        }
      }
      i += 1
    }
    if (inBacktick) throw e
    nameParts += tmp.mkString
    nameParts.toSeq
  }
}

/**
 * Represents an unresolved generator, which will be created by the parser for
 * the [[org.apache.spark.sql.catalyst.plans.logical.Generate]] operator.
 * The analyzer will resolve this generator.
 */
// 样例
// CREATE TABLE person (id INT, name STRING, age INT, class INT, address STRING);
//INSERT INTO person VALUES
//    (100, 'John', 30, 1, 'Street 1'),
//    (200, 'Mary', NULL, 1, 'Street 2'),
//    (300, 'Mike', 80, 3, 'Street 3'),
//    (400, 'Dan', 50, 4, 'Street 4');
//
//SELECT * FROM person
//    LATERAL VIEW EXPLODE(ARRAY(30, 60)) tableName AS c_age
//    LATERAL VIEW EXPLODE(ARRAY(40, 80)) AS d_age;
//+------+-------+-------+--------+-----------+--------+--------+
//|  id  | name  |  age  | class  |  address  | c_age  | d_age  |
//+------+-------+-------+--------+-----------+--------+--------+
//| 100  | John  | 30    | 1      | Street 1  | 30     | 40     |
//| 100  | John  | 30    | 1      | Street 1  | 30     | 80     |
//| 100  | John  | 30    | 1      | Street 1  | 60     | 40     |
//| 100  | John  | 30    | 1      | Street 1  | 60     | 80     |
//| 200  | Mary  | NULL  | 1      | Street 2  | 30     | 40     |
//| 200  | Mary  | NULL  | 1      | Street 2  | 30     | 80     |
//| 200  | Mary  | NULL  | 1      | Street 2  | 60     | 40     |
//| 200  | Mary  | NULL  | 1      | Street 2  | 60     | 80     |
//| 300  | Mike  | 80    | 3      | Street 3  | 30     | 40     |
//| 300  | Mike  | 80    | 3      | Street 3  | 30     | 80     |
//| 300  | Mike  | 80    | 3      | Street 3  | 60     | 40     |
//| 300  | Mike  | 80    | 3      | Street 3  | 60     | 80     |
//| 400  | Dan   | 50    | 4      | Street 4  | 30     | 40     |
//| 400  | Dan   | 50    | 4      | Street 4  | 30     | 80     |
//| 400  | Dan   | 50    | 4      | Street 4  | 60     | 40     |
//| 400  | Dan   | 50    | 4      | Street 4  | 60     | 80     |
//+------+-------+-------+--------+-----------+--------+--------+
//
//  LATERAL VIEW + Function 就是一个 Generator 的效果
// 这里进行列拆分，展开成多行
// 两个属性
// 1 name，FunctionIdentifier 类型，标记的是一个函数，这个类型既不是 LogicalPlan 也不是 Expression
// 2 children，我理解应该是 传给函数的参数
//
// 例如上面这个case
// name 就是 EXPLODE 这个函数
// children 对应 ARRAY(30, 60) 这个表达式
case class UnresolvedGenerator(name: FunctionIdentifier, children: Seq[Expression])
  // 这是一个 Expression
  extends Generator {

  override def elementSchema: StructType = throw new UnresolvedException(this, "elementTypes")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = name.unquotedString
  override def toString: String = s"'$name(${children.mkString(", ")})"

  override def eval(input: InternalRow = null): TraversableOnce[InternalRow] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")

  override def terminate(): TraversableOnce[InternalRow] =
    throw new UnsupportedOperationException(s"Cannot evaluate expression: $this")
}

// name，函数的名称对应的 FunctionIdentifier
// children，这个函数传入的参数
case class UnresolvedFunction(
    name: FunctionIdentifier,
    children: Seq[Expression],
    isDistinct: Boolean)
  extends Expression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def prettyName: String = name.unquotedString
  override def toString: String = s"'$name(${children.mkString(", ")})"
}

object UnresolvedFunction {
  def apply(name: String, children: Seq[Expression], isDistinct: Boolean): UnresolvedFunction = {
    UnresolvedFunction(FunctionIdentifier(name, None), children, isDistinct)
  }
}

/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...". A [[Star]] gets automatically expanded during analysis.
 */
// select * 对应的基类
abstract class Star extends LeafExpression with NamedExpression {

  override def name: String = throw new UnresolvedException(this, "name")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")
  override lazy val resolved = false

  def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression]
}


/**
 * Represents all of the input attributes to a given relational operator, for example in
 * "SELECT * FROM ...".
 *
 * This is also used to expand structs. For example:
 * "SELECT record.* from (SELECT struct(a,b,c) as record ...)
 *
 * @param target an optional name that should be the target of the expansion.  If omitted all
 *              targets' columns are produced. This can either be a table name or struct name. This
 *              is a list of identifiers that is the path of the expansion.
 */
// 在构建  UnresolvedStar 只有一个属性
// target：Option[Seq[String]]，就是要展开的table
// 如果是 select * ,那么 target 就是 None
// 如果是 select table1.* 那么target 就是 Option[Seq["table1"]
// 如果是 select table1.*, table2.field2.*, table3.field3 from XXXX, 那么这里就会有两个 Star Expression 以及一个 Attribute Expression
// 对于 table2.field2.* 这种，target 就是 ["table2", "field2"]
case class UnresolvedStar(target: Option[Seq[String]]) extends Star with Unevaluable {

  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = {
    // If there is no table specified, use all input attributes.
    // 如果target == None，那么直接把子LogicalPlan（input）的 所有 output 就是展开
    if (target.isEmpty) return input.output

    val expandedAttributes =
      // 只有一个 target，这个 target 肯定是 table，那么也是把这个 table 的属性都拿出来
      if (target.get.size == 1) {
        // If there is a table, pick out attributes that are part of this table.
        input.output.filter(_.qualifier.exists(resolver(_, target.get.head)))
      } else {
        List()
      }
    if (expandedAttributes.nonEmpty) return expandedAttributes
    // 下面解决多个 target

    // Try to resolve it as a struct expansion. If there is a conflict and both are possible,
    // (i.e. [name].* is both a table and a struct), the struct path can always be qualified.
    // 把 target 扔到 孩子（input）里头去查找，看看能不能找到某一个 attribute
    // 如果找到了，肯定是 类似 table2.field2.* 这种的
    val attribute = input.resolve(target.get, resolver)
    if (attribute.isDefined) {
      // This target resolved to an attribute in child. It must be a struct. Expand it.
      attribute.get.dataType match {
        case s: StructType => s.zipWithIndex.map {
          case (f, i) =>
            val extract = GetStructField(attribute.get, i)
            Alias(extract, f.name)()
        }

        case _ =>
          throw new AnalysisException("Can only star expand struct data types. Attribute: `" +
            target.get + "`")
      }
    } else {
      val from = input.inputSet.map(_.name).mkString(", ")
      val targetString = target.get.mkString(".")
      throw new AnalysisException(s"cannot resolve '$targetString.*' give input columns '$from'")
    }
  }

  override def toString: String = target.map(_ + ".").getOrElse("") + "*"
}

/**
 * Used to assign new names to Generator's output, such as hive udtf.
 * For example the SQL expression "stack(2, key, value, key, value) as (a, b)" could be represented
 * as follows:
 *  MultiAlias(stack_function, Seq(a, b))
 *

 * @param child the computation being performed
 * @param names the names to be associated with each output of computing [[child]].
 */
// 给构造出来的 新列 使用
// 例如上面这个例子  stack(2, key, value, key, value) as (a, b)，增加两个新列 a 和 b
// child 就是 stack(2, key, value, key, value) 这个 Expression
// names 就是 a 和 b
case class MultiAlias(child: Expression, names: Seq[String])
  extends UnaryExpression with NamedExpression with CodegenFallback {

  override def name: String = throw new UnresolvedException(this, "name")

  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")

  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")

  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")

  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false

  override def toString: String = s"$child AS $names"

}

/**
 * Represents all the resolved input attributes to a given relational operator. This is used
 * in the data frame DSL.
 *
 * @param expressions Expressions to expand.
 */
// 已经 resolved 的 * 表达式
// expressions 就是要展开的 attribute
case class ResolvedStar(expressions: Seq[NamedExpression]) extends Star with Unevaluable {
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")
  override def expand(input: LogicalPlan, resolver: Resolver): Seq[NamedExpression] = expressions
  override def toString: String = expressions.mkString("ResolvedStar(", ", ", ")")
}

/**
 * Extracts a value or values from an Expression
 *
 * @param child The expression to extract value from,
 *              can be Map, Array, Struct or array of Structs.
 * @param extraction The expression to describe the extraction,
 *                   can be key of Map, index of Array, field name of Struct.
 */
// 抽取特定值的 Expression
//  child ：要被抽取 value 的 Expression
// extraction: 如何抽取value 的 Expression ，可以是一个 Map 的 key，Array 的下标，以及 一个 Struct 的 field
// 例如
// UnresolvedExtractValue(Literal.create(Seq(1), ArrayType(IntegerType)), Literal.create(0, IntegerType))
// children 就是 Literal.create(Seq(1), ArrayType(IntegerType))，是一个 Literal 的 Expression，表示 Seq(1)
// extraction 传入的是一个下标 0，表示哟啊提取 Seq(1) 中 0 号下标的数据
case class UnresolvedExtractValue(child: Expression, extraction: Expression)
  extends UnaryExpression with Unevaluable {

  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false

  override def toString: String = s"$child[$extraction]"
  override def sql: String = s"${child.sql}[${extraction.sql}]"
}

/**
 * Holds the expression that has yet to be aliased.
 *
 * @param child The computation that is needs to be resolved during analysis.
 * @param aliasFunc The function if specified to be called to generate an alias to associate
 *                  with the result of computing [[child]]
 *
 */
// 我理解就是一个 套壳用的  Expression
// child，就是一个要被包进去的 Expression
// aliasFunc: Option的，是一个方法，最终用来生成这个 Expression 的结果的 alias
//
// 比如 select id, avg(score) from table group by id;
// id 这种的是 UnsolvedAttribute
// avg(score) 就是一个 Expression，但是没有 as, 所以再套一层壳，做成 UnresolvedAlias。
case class UnresolvedAlias(
    child: Expression,
    aliasFunc: Option[Expression => String] = None)
  extends UnaryExpression with NamedExpression with Unevaluable {

  override def toAttribute: Attribute = throw new UnresolvedException(this, "toAttribute")
  override def qualifier: Option[String] = throw new UnresolvedException(this, "qualifier")
  override def exprId: ExprId = throw new UnresolvedException(this, "exprId")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def name: String = throw new UnresolvedException(this, "name")
  override def newInstance(): NamedExpression = throw new UnresolvedException(this, "newInstance")

  override lazy val resolved = false
}

/**
 * Holds the deserializer expression and the attributes that are available during the resolution
 * for it.  Deserializer expression is a special kind of expression that is not always resolved by
 * children output, but by given attributes, e.g. the `keyDeserializer` in `MapGroups` should be
 * resolved by `groupingAttributes` instead of children output.
 *
 * @param deserializer The unresolved deserializer expression
 * @param inputAttributes The input attributes used to resolve deserializer expression, can be empty
 *                        if we want to resolve deserializer by children output.
 */
 // 这个没有细看
case class UnresolvedDeserializer(deserializer: Expression, inputAttributes: Seq[Attribute] = Nil)
  extends UnaryExpression with Unevaluable with NonSQLExpression {
  // The input attributes used to resolve deserializer expression must be all resolved.
  require(inputAttributes.forall(_.resolved), "Input attributes must all be resolved.")

  override def child: Expression = deserializer
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}

case class GetColumnByOrdinal(ordinal: Int, dataType: DataType) extends LeafExpression
  with Unevaluable with NonSQLExpression {
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}

/**
 * Represents unresolved ordinal used in order by or group by.
 *
 * For example:
 * {{{
 *   select a from table order by 1
 *   select a   from table group by 1
 * }}}
 * @param ordinal ordinal starts from 1, instead of 0
 */
 // 按照 数字 进行 order 或者 group 的 Expression，而且是 unresolved 的那种
 // 例如 select a, b from table order by 1, 2
//  UnresolvedOrdinal 中只有一个参数，就是一个 index
case class UnresolvedOrdinal(ordinal: Int)
    extends LeafExpression with Unevaluable with NonSQLExpression {
  override def dataType: DataType = throw new UnresolvedException(this, "dataType")
  override def foldable: Boolean = throw new UnresolvedException(this, "foldable")
  override def nullable: Boolean = throw new UnresolvedException(this, "nullable")
  override lazy val resolved = false
}
