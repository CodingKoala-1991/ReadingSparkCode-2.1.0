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

package org.apache.spark.sql.catalyst.parser

import java.sql.{Date, Timestamp}
import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.antlr.v4.runtime.{ParserRuleContext, Token}
import org.antlr.v4.runtime.tree.{ParseTree, RuleNode, TerminalNode}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.random.RandomSampler

/**
 * The AstBuilder converts an ANTLR4 ParseTree into a catalyst Expression, LogicalPlan or
 * TableIdentifier.
 */
class AstBuilder extends SqlBaseBaseVisitor[AnyRef] with Logging {
  import ParserUtils._

  protected def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }

  /**
   * Override the default behavior for all visit methods. This will only return a non-null result
   * when the context has only one child. This is done because there is no generic method to
   * combine the results of the context children. In all other cases null is returned.
   */
  override def visitChildren(node: RuleNode): AnyRef = {
    if (node.getChildCount == 1) {
      node.getChild(0).accept(this)
    } else {
      null
    }
  }


  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    // ctx 代表 SingleStatementContext 这个 object
    // 然后 statement 是这个 SingleStatementContext 的一个方法
    // 在构造AST之后，AST 之中的每一个节点都是一个 Context
    // 在这里，statement 是 singleStatement 的子语法规则，通过 statement() 方法就可以返回
    // SingleStatementContext 的孩子之一 StatementContext 对象
    // 然后扔到 visit 方法里，继续递归访问

    // 每一个 语法规则 以及 规则的分支 都会对应一个 Context
    // 但是在每一个 Context 内部的方法中，只有语法规则才有相应的方法，分支没有

    // 例如 singleStatement 这个语法规则，statement语法规则 是他的一部分，因此 SingleStatementContext 内部存在 public StatementContext statement() 方法
    // statement语法规则 的分支 #explain，也包含 statement语法规则，因此 ExplainContext 内部也有 public StatementContext statement() 方法
    // 但是 context 内部都不存在 explain() 方法，因为 #explain 只是一个分支

    // 在 visitor 中，每一个语法规则 或者 分支 都有自己的 visitXXX 方法

    // 这种比较高level 的context 都是直接访问孩子，继续向下的
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  override def visitSingleExpression(ctx: SingleExpressionContext): Expression = withOrigin(ctx) {
  // 入口之一？
    visitNamedExpression(ctx.namedExpression)
  }

  override def visitSingleTableIdentifier(
      ctx: SingleTableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    // 入口之一？
    visitTableIdentifier(ctx.tableIdentifier)
  }

  override def visitSingleDataType(ctx: SingleDataTypeContext): DataType = withOrigin(ctx) {
    // 入口之一？
    visit(ctx.dataType).asInstanceOf[DataType]
  }

  /* ********************************************************************************************
   * Plan parsing
   * ******************************************************************************************** */
   // 接受一个 Context 对象，构建以这个 Context 为 root 的 LogicalPlan Tree
   // 类似的还有一个   protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)
   // 行面说这个是用来生成 expression 的
   // 因为给定一个AST，可能返回 LogicalPlan tree 也可能返回 Expression tree
  protected def plan(tree: ParserRuleContext): LogicalPlan = typedVisit(tree)

  /**
   * Create a top-level plan with Common Table Expressions.
   */
  // AstBuilder 继承了 SqlBaseBaseVisitor
  // 但是并没有重载 visitStatementDefault
  // 也即是说 SqlBaseBaseVisitor 中的方法，AstBuilder 不是每一个都要重载，有一些还是用原来的递归下去的逻辑就行
  // 直接从 visitQuery 开始继承
  // visitQuery 对应的语法规则
  // query
  //    : ctes? queryNoWith
  //    ;
  // 其中 ctes 是可选部分，CTE 的意思是 公用表达式
  // ctes
  //    : WITH namedQuery (',' namedQuery)*
  //    ;
  //
  // namedQuery
  //    : name=identifier AS? '(' query ')'

  // queryNoWith 最终对应两个分支：singleInsertQuery 和 multiInsertQuery
  // queryNoWith
  //    : insertInto? queryTerm queryOrganization                                              #singleInsertQuery
  //    | fromClause multiInsertQueryBody+                                                     #multiInsertQuery
  //    ;

  // with 语句的样例
  // WITH table_1 AS (
  // SELECT GENERATE_SERIES('2012-06-29', '2012-07-03', '1 day'::INTERVAL) AS date
  // ),
  // table_2 AS (
  // SELECT GENERATE_SERIES('2012-06-30', '2012-07-13', '1 day'::INTERVAL) AS date
  // )
  // SELECT * FROM table_1
  // WHERE date IN table_2
  // WITH 语句就是把一些query，命名成特定名称，后续可以反复使用
  override def visitQuery(ctx: QueryContext): LogicalPlan = withOrigin(ctx) {
    // queryNoWith 就是说 sql 语句中没有 with 语句
    val query = plan(ctx.queryNoWith)

    // Apply CTEs
    // g4 文件中，CTES 部分是可选的（带问号？），所以这里是 optional
    query.optional(ctx.ctes) {
      // ctx.ctes.namedQuery 返回的是一个 List<NamedQueryContext>
      val ctes = ctx.ctes.namedQuery.asScala.map { nCtx =>
        val namedQuery = visitNamedQuery(nCtx)
        // 用 with 语句的 table 名作为 alias，也就是key
        (namedQuery.alias, namedQuery)
      }
      // Check for duplicate names.
      checkDuplicateKeys(ctes, ctx)
      // 最终 ctes 是一个 Map，key 是with 里的 table 名，value 是对应的 namedQuery（LogicalPlan）
      // 然后 构建 With 这个LogicalPlan
      With(query, ctes)
    }
  }

  /**
   * Create a named logical plan.
   *
   * This is only used for Common Table Expressions.
   */
  // 构建一个 SubqueryAlias 的 LogicalPlan
  override def visitNamedQuery(ctx: NamedQueryContext): SubqueryAlias = withOrigin(ctx) {
    SubqueryAlias(ctx.name.getText, plan(ctx.query), None)
  }

  /**
   * Create a logical plan which allows for multiple inserts using one 'from' statement. These
   * queries have the following SQL form:
   * {{{
   *   [WITH cte...]?
   *   FROM src
   *   [INSERT INTO tbl1 SELECT *]+
   * }}}
   * For example:
   * {{{
   *   FROM db.tbl1 A
   *   INSERT INTO dbo.tbl1 SELECT * WHERE A.value = 10 LIMIT 5
   *   INSERT INTO dbo.tbl2 SELECT * WHERE A.value = 12
   * }}}
   * This (Hive) feature cannot be combined with set-operators.
   */
  // 执行多条 insert
  // queryNoWith
  //         : insertInto? queryTerm queryOrganization                                              #singleInsertQuery
  //         | fromClause multiInsertQueryBody+                                                     #multiInsertQuery
  //         ;
  //
  // fromClause
  //    : FROM relation (',' relation)* lateralView*
  //    ;
  //
  // multiInsertQueryBody
  //    : insertInto?
  //      querySpecification
  //      queryOrganization
  //    ;
  //
  // 从上面语法规则可以看出，fromClause 中可以有多个 relation 或者 view
  override def visitMultiInsertQuery(ctx: MultiInsertQueryContext): LogicalPlan = withOrigin(ctx) {
  // 这里的 from 我理解 和 select * from 里的from 是一个意思
    val from = visitFromClause(ctx.fromClause)
    // Build the insert clauses.
    // 然后多条 insert 语句的解析就是单条insert 的组合
    // 因此流程 和 单条的一致
    // 唯一不同的是，如果真的是有多条，最后用一个 Union 的 LogicalPlan 包住
    val inserts = ctx.multiInsertQueryBody.asScala.map {
      body =>
        validate(body.querySpecification.fromClause == null,
          "Multi-Insert queries cannot have a FROM clause in their individual SELECT statements",
          body)

        withQuerySpecification(body.querySpecification, from).
          // Add organization statements.
          optionalMap(body.queryOrganization)(withQueryResultClauses).
          // Add insert.
          optionalMap(body.insertInto())(withInsertInto)
    }

    // If there are multiple INSERTS just UNION them together into one query.
    inserts match {
      case Seq(query) => query
      case queries => Union(queries)
    }
  }

  /**
   * Create a logical plan for a regular (single-insert) query.
   */
  // queryNoWith 的分支之一 singleInsertQuery
  // 对应的语法规则
  // queryNoWith
  //    : insertInto? queryTerm queryOrganization                                              #singleInsertQuery
  //    | fromClause multiInsertQueryBody+                                                     #multiInsertQuery
  //    ;
  override def visitSingleInsertQuery(
      ctx: SingleInsertQueryContext): LogicalPlan = withOrigin(ctx) {
    // 先构建 基于 queryTerm 的 LogicalPlan
    plan(ctx.queryTerm).
      // optionalMap 的功能就是 把一个 LogicalPlan 转化成另一类 LogicalPlan
      // Add organization statements.
      // organization statements 说的是 ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS 这一类的操作
      // 这些操作基于 plan(ctx.queryTerm) 返回的 LogicalPlan 去执行的
      optionalMap(ctx.queryOrganization)(withQueryResultClauses).
      // Add insert.
      //  ctx.insertInto() 返回的是 一个 InsertIntoContext 对象
      // withInsertInto 又是一个内部方法
      // insert 如果有的话，肯定是最后执行的plan
      optionalMap(ctx.insertInto())(withInsertInto)
  }

  /**
   * Add an INSERT INTO [TABLE]/INSERT OVERWRITE TABLE operation to the logical plan.
   */
  // 已经看了
  private def withInsertInto(
      ctx: InsertIntoContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
      // 结合上下文，看方法
      // ctx 是 InsertIntoContext 对象
      // query 是一个 LogicalPlan，是 visitSingleInsertQuery 里 plan(ctx.queryTerm) 方法生成的 LogicalPlan

    // 从   ctx 中获取 table name 和 partition keys
    // 还有是否 overwrite 这些
    val tableIdent = visitTableIdentifier(ctx.tableIdentifier)
    val partitionKeys = Option(ctx.partitionSpec).map(visitPartitionSpec).getOrElse(Map.empty)

    val dynamicPartitionKeys: Map[String, Option[String]] = partitionKeys.filter(_._2.isEmpty)
    if (ctx.EXISTS != null && dynamicPartitionKeys.nonEmpty) {
      throw new ParseException(s"Dynamic partitions do not support IF NOT EXISTS. Specified " +
        "partitions with value: " + dynamicPartitionKeys.keys.mkString("[", ",", "]"), ctx)
    }
    val overwrite = ctx.OVERWRITE != null
    val staticPartitionKeys: Map[String, String] =
      partitionKeys.filter(_._2.nonEmpty).map(t => (t._1, t._2.get))
    // 也就是说，如果 insertInto 不是空，最终     plan(ctx.queryTerm) 生成的 LogicalPlan 会被转换成 InsertIntoTable 的LogicalPlan
    InsertIntoTable(
      UnresolvedRelation(tableIdent, None),
      partitionKeys,
      query,
      OverwriteOptions(overwrite, if (overwrite) staticPartitionKeys else Map.empty),
      ctx.EXISTS != null)
  }

  /**
   * Create a partition specification map.
   */
   // key 是 partition 的名称
   // value 是 这个 partition 的
  override def visitPartitionSpec(
      ctx: PartitionSpecContext): Map[String, Option[String]] = withOrigin(ctx) {
    val parts = ctx.partitionVal.asScala.map { pVal =>
      val name = pVal.identifier.getText  // partition 的名称
      val value = Option(pVal.constant).map(visitStringConstant)  // partition 的某个具体值
      name -> value
    }
    // Before calling `toMap`, we check duplicated keys to avoid silently ignore partition values
    // in partition spec like PARTITION(a='1', b='2', a='3'). The real semantical check for
    // partition columns will be done in analyzer.
    checkDuplicateKeys(parts, ctx)
    parts.toMap
  }

  /**
   * Create a partition specification map without optional values.
   */
  // 这个visit 不是真的 visitXXXX
  // 正宗的 visitor 中的 visit 方法都是  override def 的
  protected def visitNonOptionalPartitionSpec(
      ctx: PartitionSpecContext): Map[String, String] = withOrigin(ctx) {
    visitPartitionSpec(ctx).mapValues(_.orNull).map(identity)
  }

  /**
   * Convert a constant of any type into a string. This is typically used in DDL commands, and its
   * main purpose is to prevent slight differences due to back to back conversions i.e.:
   * String -> Literal -> String.
   */
   // 不用细看
  protected def visitStringConstant(ctx: ConstantContext): String = withOrigin(ctx) {
    ctx match {
      case s: StringLiteralContext => createString(s)
      case o => o.getText
    }
  }

  /**
   * Add ORDER BY/SORT BY/CLUSTER BY/DISTRIBUTE BY/LIMIT/WINDOWS clauses to the logical plan. These
   * clauses determine the shape (ordering/partitioning/rows) of the query result.
   */
  // 比如 select * from table1 order by id desc；
  // 在这里 query，就是 select * from table1 对应的 LogicalPlan
  // QueryOrganizationContext 对应的是 后面 order 的这一部分
  private def withQueryResultClauses(
      ctx: QueryOrganizationContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // Handle ORDER BY, SORT BY, DISTRIBUTE BY, and CLUSTER BY clause.
    // 在 g4 文法中，相应的语法规则为：
    // queryOrganization
    //    : (ORDER BY order+=sortItem (',' order+=sortItem)*)?
    //      (CLUSTER BY clusterBy+=expression (',' clusterBy+=expression)*)?
    //      (DISTRIBUTE BY distributeBy+=expression (',' distributeBy+=expression)*)?
    //      (SORT BY sort+=sortItem (',' sort+=sortItem)*)?
    //      windows?
    //      (LIMIT limit=expression)?
    //    ;
    // 使用了 += ，因此在 ANTLR4 生成的 Context 中，如果 sql 语句中有 order by，那么对应的 order 字段必然不空
    // 所以下面使用 字段判空的方式，来看是哪一类 QueryOrganizationContext
    val withOrder = if (
      !order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // ORDER BY ...
      // 构造 Sort 这个 LogicalPlan
      // order.asScala.map(visitSortItem) 生成的 是 Seq[SortOrder]
      // SortOrder 又是一个 Expression
      // 比如 select * from table1 order by id desc；
      // 在这里 query，就是 select * from table1 对应的 LogicalPlan
      Sort(order.asScala.map(visitSortItem), global = true, query)
    } else if (order.isEmpty && !sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ...
      Sort(sort.asScala.map(visitSortItem), global = false, query)
    } else if (order.isEmpty && sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // DISTRIBUTE BY ...
      // DISTRIBUTE BY 根据给定的字段list，进行分区输出，但是每一个分区内的数据是无序的
      // 这里说明一下 expressionList 这个方法
      //   private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
      //    trees.asScala.map(expression)
      //  }
      // 这里的 expression 是说下面的这个方法
      // protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)
      // 即对每一个 Context 都调用 expression方法，生成 Expression 对象
      // 参数是一个 distributeBy，其实就是 List<ExpressionContext> 类型
      // expressionList 然后就是把 这个 List<ExpressionContext> 类型的对象遍历一遍
      // 把每一个 ExpressionContext 类型对象的 expressions 返回，生成一个list
      RepartitionByExpression(expressionList(distributeBy), query)
    } else if (order.isEmpty && !sort.isEmpty && !distributeBy.isEmpty && clusterBy.isEmpty) {
      // SORT BY ... DISTRIBUTE BY ...
      Sort(
        sort.asScala.map(visitSortItem),
        global = false,
        RepartitionByExpression(expressionList(distributeBy), query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && !clusterBy.isEmpty) {
      // CLUSTER BY ...
      // CLUSTER BY 也是 根据 给定的字段list，进行分区输出，但是每一个分区内的数据是有序的，这点和 DISTRIBUTE BY 不一样
      val expressions = expressionList(clusterBy)
      Sort(
        expressions.map(SortOrder(_, Ascending)),
        global = false,
        RepartitionByExpression(expressions, query))
    } else if (order.isEmpty && sort.isEmpty && distributeBy.isEmpty && clusterBy.isEmpty) {
      // [EMPTY]
      query
    } else {
      throw new ParseException(
        "Combination of ORDER BY/SORT BY/DISTRIBUTE BY/CLUSTER BY is not supported", ctx)
    }

    // WINDOWS
    // WINDOW 子句的定义可以参考
    // http://dcx.sybase.com/1201/zh/dbreference/window-statement.html
    // 个人感觉有点像 窗口函数 + OVER 的用法
    // 如果有 window 子句，在 前面的query 的 LogicalPlan 的基础上，套一个 with 对应的 LogicalPlan 的父节点
    val withWindow = withOrder.optionalMap(windows)(withWindows)

    // LIMIT
    withWindow.optional(limit) {
      // 如果有limit 就生成Limit LogicalPlan，其实就是在 原来的 LogicalPlan 基础上加了一个 父节点，这个就是 optional 这个方法的作用
      // typedVisit(limit) 生成 limit 对应的 Expression
      Limit(typedVisit(limit), withWindow)
    }
  }

  /**
   * Create a logical plan using a query specification.
   */
  // 语法规则如下：
  // querySpecification
  //    : (((SELECT kind=TRANSFORM '(' namedExpressionSeq ')'
  //        | kind=MAP namedExpressionSeq
  //        | kind=REDUCE namedExpressionSeq))
  //       inRowFormat=rowFormat?
  //       (RECORDWRITER recordWriter=STRING)?
  //       USING script=STRING
  //       (AS (identifierSeq | colTypeList | ('(' (identifierSeq | colTypeList) ')')))?
  //       outRowFormat=rowFormat?
  //       (RECORDREADER recordReader=STRING)?
  //       fromClause?
  //       (WHERE where=booleanExpression)?)
  //    | ((kind=SELECT setQuantifier? namedExpressionSeq fromClause?
  //       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
  //       lateralView*
  //       (WHERE where=booleanExpression)?
  //       aggregation?
  //       (HAVING having=booleanExpression)?
  //       windows?)
  //    ;
  // 对应 select 语句的操作
  override def visitQuerySpecification(
      ctx: QuerySpecificationContext): LogicalPlan = withOrigin(ctx) {
    // 首先解析 from 部分
    // select 语句如果没有 from 部分，那么就是一个   OneRowRelation
    // 要么就调用 visitFromClause 得到 from 部分的 LogicalPlan
    val from = OneRowRelation.optional(ctx.fromClause) {
      visitFromClause(ctx.fromClause)
    }
    // 转换除了 from 之外的部分
    withQuerySpecification(ctx, from)
  }

  /**
   * Add a query specification to a logical plan. The query specification is the core of the logical
   * plan, this is where sourcing (FROM clause), transforming (SELECT TRANSFORM/MAP/REDUCE),
   * projection (SELECT), aggregation (GROUP BY ... HAVING ...) and filtering (WHERE) takes place.
   *
   * Note that query hints are ignored (both by the parser and the builder).
   */
  // 最核心的部分
  // 转换除了 from 之外的部分
  // 比如 select，where，aggregation 等等
  private def withQuerySpecification(
      ctx: QuerySpecificationContext,
      relation: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._

    // WHERE
    // 内置方法，传入 where部分的 context以及 对应 的 expression
    // 构建 Filter 这个 LogicalPlan，Filter 是 原来的 pan 这个 LogicalPlan 的 父亲节点
    def filter(ctx: BooleanExpressionContext, plan: LogicalPlan): LogicalPlan = {
      Filter(expression(ctx), plan)
    }

    // Expressions.
    // 对应的 语法规则如下：
    // namedExpressionSeq
    //    : namedExpression (',' namedExpression)*
    //    ;
    // 这里调用的 是 QuerySpecificationContext 的 namedExpressionSeq 方法
    // 其实就是对应 select 中的 select a,b,c,d 等各个被select 出来的字段
    // 他们也是 Expression
    val expressions = Option(namedExpressionSeq).toSeq
      .flatMap(_.namedExpression.asScala)
      .map(typedVisit[Expression])

    // Create either a transform or a regular query.
    val specType = Option(kind).map(_.getType).getOrElse(SqlBaseParser.SELECT)
    specType match {
      // TRANSFORM 这种形式
      // 样例：
      // SELECT
      //    TRANSFORM (col1, col2)
      //    USING './test.py'
      //    AS (new1, new2)
      // FORM
      //    test;
      // 用 MR 的方式 执行特定 脚本 去执行执行SparkSQL
      // 因此这种形式 的 转换 没有细看
      case SqlBaseParser.MAP | SqlBaseParser.REDUCE | SqlBaseParser.TRANSFORM =>
        // Transform

        // Add where.
        val withFilter = relation.optionalMap(where)(filter)

        // Create the attributes.
        val (attributes, schemaLess) = if (colTypeList != null) {
          // Typed return columns.
          (createSchema(colTypeList).toAttributes, false)
        } else if (identifierSeq != null) {
          // Untyped return columns.
          val attrs = visitIdentifierSeq(identifierSeq).map { name =>
            AttributeReference(name, StringType, nullable = true)()
          }
          (attrs, false)
        } else {
          (Seq(AttributeReference("key", StringType)(),
            AttributeReference("value", StringType)()), true)
        }

        // Create the transform.
        ScriptTransformation(
          expressions,
          string(script),
          attributes,
          withFilter,
          withScriptIOSchema(
            ctx, inRowFormat, recordWriter, outRowFormat, recordReader, schemaLess))

      // 常规的 select 操作语句，主要细看这种常规的转换
      // 对应的 语法规则如下：
      //     | ((kind=SELECT setQuantifier? namedExpressionSeq fromClause?
      //       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
      //       lateralView*
      //       (WHERE where=booleanExpression)?
      //       aggregation?
      //       (HAVING having=booleanExpression)?
      //       windows?)
      case SqlBaseParser.SELECT =>
        // Regular select

        // Add lateral views.
        // 首先在 FROM 对应的 LogicalPlan 套上 lateral views 对应的 LogicalPlan
        val withLateralView = ctx.lateralView.asScala.foldLeft(relation)(withGenerate)

        // Add where.
        // 先调用 内部方法filter 生成 Filter这个LogicalPlan
        // 然后Filter 就称为旧LogicalPlan 的 父亲节点
        val withFilter = withLateralView.optionalMap(where)(filter)

        // Add aggregation or a project.
        // 再套一层，例如 group by 这种 aggregation
        val namedExpressions = expressions.map {
          case e: NamedExpression => e
          case e: Expression => UnresolvedAlias(e)
        }
        val withProject = if (aggregation != null) {
          // 三个参数
          // aggregation，group by 部分的语句
          // namedExpressions， select a, count(b) 部分的语句
          // withFilter 当前 LogicalPlan 的 root
          // select a, count(b) 需要 aggregation 部分的配合
          // 这种情况套上的 LogicalPlan 是 Aggregation，而不是 Project
          withAggregation(aggregation, namedExpressions, withFilter)
        } else if (namedExpressions.nonEmpty) {
          // 如果没有 aggregation，就是 select a,b,c 这种，没有聚合操作
          // 直接套上 Project 就可以了
          Project(namedExpressions, withFilter)
        } else {
          withFilter
        }

        // Having
        // 对应的 规则
        //        (HAVING having=booleanExpression)?
        // 这里 optional 是一个柯里化函数
        // 传两个参数，having 和 一个具体的 转换方法
        // having 就是一个 BooleanExpressionContext，就是 having 关键子后面那一坨
        val withHaving = withProject.optional(having) {
          // Note that we add a cast to non-predicate expressions. If the expression itself is
          // already boolean, the optimizer will get rid of the unnecessary cast.
          // expression(having) 就是把 having 这个 BooleanExpressionContext 转换成一个 Expression
          val predicate = expression(having) match {
            case p: Predicate => p
            case e => Cast(e, BooleanType)
          }
          // 最终 having 其实就是一个 Filter
          Filter(predicate, withProject)
        }

        // Distinct
        // 语法规则
        //     | ((kind=SELECT setQuantifier? namedExpressionSeq fromClause?
        //       | fromClause (kind=SELECT setQuantifier? namedExpressionSeq)?)
        //       lateralView*
        //       (WHERE where=booleanExpression)?
        //       aggregation?
        //       (HAVING having=booleanExpression)?
        //       windows?)
        //
        // setQuantifier() 就是对应 DISTINCT 的部分
        // 就是说，在select 的时候，要么 select a,b,c   要么 select distinct a,b,c 这种
        val withDistinct = if (setQuantifier() != null && setQuantifier().DISTINCT() != null) {
          Distinct(withHaving)
        } else {
          withHaving
        }

        // Window
        // windows 就是 WindowsContext
        withDistinct.optionalMap(windows)(withWindows)
    }
  }

  /**
   * Create a (Hive based) [[ScriptInputOutputSchema]].
   */
   // 就是 select 的时候依赖 一个 脚本的时候使用的
  protected def withScriptIOSchema(
      ctx: QuerySpecificationContext,
      inRowFormat: RowFormatContext,
      recordWriter: Token,
      outRowFormat: RowFormatContext,
      recordReader: Token,
      schemaLess: Boolean): ScriptInputOutputSchema = {
    throw new ParseException("Script Transform is not supported", ctx)
  }

  /**
   * Create a logical plan for a given 'FROM' clause. Note that we support multiple (comma
   * separated) relations here, these get converted into a single plan by condition-less inner join.
   */
   // visitFromClause 子句，里面对应的 relation list 和 view list 都要join 起来
  override def visitFromClause(ctx: FromClauseContext): LogicalPlan = withOrigin(ctx) {
    val from = ctx.relation.asScala.foldLeft(null: LogicalPlan) { (left, relation) =>
      // 这里 relation.relationPrimary 返回一个 RelationPrimaryContext
      // relation 是一个 Context
      // 然后生成 LogicalPlan
      val right = plan(relation.relationPrimary)
      // Join 的 LogicalPlan 不断叠加
      val join = right.optionalMap(left)(Join(_, _, Inner, None))
      // 最终返回一个 withJoinRelations 的 LogicalPlan
      withJoinRelations(join, relation)
    }
    // 最后还要叠加 view
    ctx.lateralView.asScala.foldLeft(from)(withGenerate)
  }

  /**
   * Connect two queries by a Set operator.
   *
   * Supported Set operators are:
   * - UNION [DISTINCT]
   * - UNION ALL
   * - EXCEPT [DISTINCT]
   * - MINUS [DISTINCT]
   * - INTERSECT [DISTINCT]
   */
  // 集合操作的 转换
  // 都有 left 和 right 两个 LogicalPlan
  // 然后根据不同的 集合操作类型
  // 生成不同的 LogicalPlan
  override def visitSetOperation(ctx: SetOperationContext): LogicalPlan = withOrigin(ctx) {
    val left = plan(ctx.left)
    val right = plan(ctx.right)
    val all = Option(ctx.setQuantifier()).exists(_.ALL != null)
    ctx.operator.getType match {
      case SqlBaseParser.UNION if all =>
        Union(left, right)
      case SqlBaseParser.UNION =>
        Distinct(Union(left, right))
      case SqlBaseParser.INTERSECT if all =>
        throw new ParseException("INTERSECT ALL is not supported.", ctx)
      case SqlBaseParser.INTERSECT =>
        Intersect(left, right)
      case SqlBaseParser.EXCEPT if all =>
        throw new ParseException("EXCEPT ALL is not supported.", ctx)
      case SqlBaseParser.EXCEPT =>
        Except(left, right)
      case SqlBaseParser.SETMINUS if all =>
        throw new ParseException("MINUS ALL is not supported.", ctx)
      case SqlBaseParser.SETMINUS =>
        Except(left, right)
    }
  }

  /**
   * Add a [[WithWindowDefinition]] operator to a logical plan.
   */
  private def withWindows(
      ctx: WindowsContext,
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Collect all window specifications defined in the WINDOW clause.
    // 一个 sql 语句中可能有多个 window 子句，因此这里是一个 list
    // 同时还要构建 map
    val baseWindowMap = ctx.namedWindow.asScala.map {
      wCtx =>
        (wCtx.identifier.getText, typedVisit[WindowSpec](wCtx.windowSpec))
    }.toMap

    // Handle cases like
    // window w1 as (partition by p_mfgr order by p_name
    //               range between 2 preceding and 2 following),
    //        w2 as w1
    val windowMapView = baseWindowMap.mapValues {
      case WindowSpecReference(name) =>
        baseWindowMap.get(name) match {
          case Some(spec: WindowSpecDefinition) =>
            spec
          case Some(ref) =>
            throw new ParseException(s"Window reference '$name' is not a window specification", ctx)
          case None =>
            throw new ParseException(s"Cannot resolve window reference '$name'", ctx)
        }
      case spec: WindowSpecDefinition => spec
    }

    // Note that mapValues creates a view instead of materialized map. We force materialization by
    // mapping over identity.
    // query 依然是前面的一大坨操作，这里成为了 WithWindowDefinition 这个 LogicalPlan 的 child
    WithWindowDefinition(windowMapView.map(identity), query)
  }

  /**
   * Add an [[Aggregate]] to a logical plan.
   */
  // 三个参数
  // ctx，aggregation部分，group by 部分的语句
  // selectExpressions， select a, count(b) 部分的语句
  // query 当前 LogicalPlan 的 root，这里是加上了 Filter 的
  // select a, count(b) 需要 aggregation 部分的配合
  private def withAggregation(
      ctx: AggregationContext,
      selectExpressions: Seq[NamedExpression],
      query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    import ctx._
    // 对应的语法规则
    // aggregation
    //    : GROUP BY groupingExpressions+=expression (',' groupingExpressions+=expression)* (
    //      WITH kind=ROLLUP
    //    | WITH kind=CUBE
    //    | kind=GROUPING SETS '(' groupingSet (',' groupingSet)* ')')?
    //    ;
    //
    // 所以首先还是把 group by 后面的 expression 部分 真正转成 Expression
    // groupingExpressions 就是 AggregationContext 的一个属性，类型是 List<ExpressionContext>
    val groupByExpressions = expressionList(groupingExpressions)

    // 这里调用 AggregationContext 的 public TerminalNode GROUPING() 方法
    // https://www.cnblogs.com/Allen-rg/p/10648231.html
    // 两种不同的GROUP BY 形式 参考上面的 URL
    // 1 GROUP BY .... GROUPING SETS (...)
        // 例如 grouping sets (month,day)，按照 month 和 day group by 之后的结果进行 union
    // 2 GROUP BY .... (WITH CUBE | WITH ROLLUP)?
        // CUBE，group by 的所有字段交叉组合进行分析
        // ROLLUP，从左到右，带层级关系的组合分析
    if (GROUPING != null) {
      //
      val expressionMap = groupByExpressions.zipWithIndex.toMap
      val numExpressions = expressionMap.size
      val mask = (1 << numExpressions) - 1
      val masks = ctx.groupingSet.asScala.map {
        _.expression.asScala.foldLeft(mask) {
          case (bitmap, eCtx) =>
            // Find the index of the expression.
            val e = typedVisit[Expression](eCtx)
            val index = expressionMap.find(_._1.semanticEquals(e)).map(_._2).getOrElse(
              throw new ParseException(
                s"$e doesn't show up in the GROUP BY list", ctx))
            // 0 means that the column at the given index is a grouping column, 1 means it is not,
            // so we unset the bit in bitmap.
            bitmap & ~(1 << (numExpressions - 1 - index))
        }
      }
      // GroupingSets 也是一个 LogicalPlan
      GroupingSets(masks, groupByExpressions, query, selectExpressions)
    } else {
      // GROUP BY .... (WITH CUBE | WITH ROLLUP)?
      // Cube 和 Rollup 都是 Expression
      // 构建好了 Expression，组装进 Aggregate 这个 LogicalPlan 里
      val mappedGroupByExpressions = if (CUBE != null) {
        Seq(Cube(groupByExpressions))
      } else if (ROLLUP != null) {
        Seq(Rollup(groupByExpressions))
      } else {
        groupByExpressions
      }
      Aggregate(mappedGroupByExpressions, selectExpressions, query)
    }
  }

  /**
   * Add a [[Generate]] (Lateral View) to a logical plan.
   */
  // 把  View 转成 LogicalPlan
  // val df = spark.sql(
  //      """
  //        |select
  //        |insideLayer2.json as a2
  //        |from (select '{"layer1": {"layer2": "text inside layer 2"}}' json) test
  //        |lateral view json_tuple(json, 'layer1') insideLayer1 as json
  //        |lateral view json_tuple(insideLayer1.json, 'layer2') insideLayer2 as json
  //      """.stripMargin
  //
  // lateral view 的样例2
  // select name, sum(calorie)
  // from
  //     (select t1.name
  //            ,fd
  //            ,t2.calorie
  //     from p_food t1 lateral view explode(split(food,'、')) as fd
  //     left join f_calorie t2 on t1.food = t2.food)
  // group by name
  //
  // lateral view 部分的规则如下：
  // lateralView
  //    : LATERAL VIEW (OUTER)? qualifiedName '(' (expression (',' expression)*)? ')' tblName=identifier (AS? colName+=identifier (',' colName+=identifier)*)?
  //    ;
  // 依次解释一下
  // qualifiedName 中文含义是 合格的名字，这里意味着一个UDF，例如行转列的 explode() 或者 样例1中的 json_tuple
  // 在 qualifiedName 后面紧跟着 (expression, expression...) 这样的形式，其实就是传给 UDF 的 参数 或者 expression，可能有1个，也可能多个
  // tblName 就是 view 的名称，例如样例1中的 insideLayer1 和 insideLayer2，如果有多个view，在select 的时候，要加上 view 的名称的
  // 然后 as colName1, colName2...对应着 UDF 返回的每一列的名称
  //
  //
  //
  // 一个 从 官方 doc 抄来的 example
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
  private def withGenerate(
      query: LogicalPlan,  // 现在已经叠加了的 relations 和 views 对应的 LogicalPlan
      ctx: LateralViewContext): LogicalPlan = withOrigin(ctx) {
      // ctx.expression 返回的是 List<ExpressionContext>
      // expressionList 传入 List<ExpressionContext>
      // 返回 List<Expression>
    val expressions = expressionList(ctx.expression)
    Generate(
      // visitFunctionName(ctx.qualifiedName) 返回的是 FunctionIdentifier 对象，这是一个 class（不是Expression 也不是 LogicalPlan）
      // 然后构建 UnresolvedGenerator 这个 Expression
      // 然后这个 Expression 对象，作为一个参数，参与了 Generate 这个 LogicalPlan 的构建
      UnresolvedGenerator(visitFunctionName(ctx.qualifiedName), expressions),
      join = true,
      outer = ctx.OUTER != null,
      Some(ctx.tblName.getText.toLowerCase),
      ctx.colName.asScala.map(_.getText).map(UnresolvedAttribute.apply),
      query)
  }

  /**
   * Create a single relation referenced in a FROM claused. This method is used when a part of the
   * join condition is nested, for example:
   * {{{
   *   select * from t1 join (t2 cross join t3) on col1 = col2
   * }}}
   */
   // 样例如上
   // 匹配的规则
   // relation
   //    : relationPrimary joinRelation*
   //    ;
  override def visitRelation(ctx: RelationContext): LogicalPlan = withOrigin(ctx) {
    // RelationContext 的 object 可能有多个 子 context，有 1个 RelationPrimaryContext 和 若干个 JoinRelationContext
    // 这里 plan(ctx.relationPrimary)，就是先递归调用，把 RelationContext 这个context 转为 LogicalPlan
    withJoinRelations(plan(ctx.relationPrimary), ctx)
  }

  /**
   * Join one more [[LogicalPlan]]s to the current logical plan.
   */
  private def withJoinRelations(base: LogicalPlan, ctx: RelationContext): LogicalPlan = {
  // 上面分析过，ctx.joinRelation 返回的是若干个 JoinRelationContext
  // 然后用 foldLeft ，依次和 base 这个 LogicalPlan 进行叠加，返回最终的 LogicalPlan
  // left 就是 一个 LogicalPlan，初始化为 base
  // join 是一个 JoinRelationContext 对象
    ctx.joinRelation.asScala.foldLeft(base) { (left, join) =>
      withOrigin(join) {
        // join.joinType 就是 JoinRelationContext 对象调用自身的 joinType() 获取 JoinTypeContext
        // 然后 case 里头的 jt 其实就是 JoinTypeContext 对象
        // CROSS() 方法获取的是一个 TerminalNode 对象，已经分析到叶子了
        // 具体 joinType 的定义在 org/apache/spark/sql/catalyst/plans/joinTypes.scala
        val baseJoinType = join.joinType match {
          case null => Inner
          case jt if jt.CROSS != null => Cross
          case jt if jt.FULL != null => FullOuter
          case jt if jt.SEMI != null => LeftSemi
          case jt if jt.ANTI != null => LeftAnti
          case jt if jt.LEFT != null => LeftOuter
          case jt if jt.RIGHT != null => RightOuter
          case _ => Inner
        }

        // Resolve the join type and join condition
        // joinCriteria 是指 ON 或者 USING 后面的那一部分
        val (joinType, condition) = Option(join.joinCriteria) match {
          case Some(c) if c.USING != null =>
            // 例如 USING(a, b, c)
            // A表是 (a,b,c,d)
            // B表是 (a,b,c,e)
            // using join 的结果就是(a,b,c,d,e)
            // 普通join 的诶结果是 (a,b,c,d,a,b,c,e)
            // c.identifier 重的 c 是 JoinCriteriaContext 对象
            // USING 这个情况下，expression 为 None
            (UsingJoin(baseJoinType, c.identifier.asScala.map(_.getText)), None)
          case Some(c) if c.booleanExpression != null =>
            // 带 ON 关键字的 join
            // 直接调用 expression 方法，传入 BooleanExpressionContext 对象，去构建一个 expression
            (baseJoinType, Option(expression(c.booleanExpression)))
          case None if join.NATURAL != null =>
            if (baseJoinType == Cross) {
              throw new ParseException("NATURAL CROSS JOIN is not supported", ctx)
            }
            (NaturalJoin(baseJoinType), None)
          case None =>
            (baseJoinType, None)
        }
        // 然后新建一个 Join 的 LogicalPlan
        // 这里 condition 是一个 expression
        Join(left, plan(join.right), joinType, condition)
      }
    }
  }

  /**
   * Add a [[Sample]] to a logical plan.
   *
   * This currently supports the following sampling methods:
   * - TABLESAMPLE(x ROWS): Sample the table down to the given number of rows.
   * - TABLESAMPLE(x PERCENT): Sample the table down to the given percentage. Note that percentages
   * are defined as a number between 0 and 100.
   * - TABLESAMPLE(BUCKET x OUT OF y): Sample the table down to a 'x' divided by 'y' fraction.
   */
  // 语法规则如下：
  // sample
  //    : TABLESAMPLE '('
  //      ( (percentage=(INTEGER_VALUE | DECIMAL_VALUE) sampleType=PERCENTLIT)
  //      | (expression sampleType=ROWS)
  //      | sampleType=BYTELENGTH_LITERAL
  //      | (sampleType=BUCKET numerator=INTEGER_VALUE OUT OF denominator=INTEGER_VALUE (ON (identifier | qualifiedName '(' ')'))?))
  //      ')'
  //    ;
  //
  // 抽样，又是一个 with 类型的方法
  // 在 query 这个  LogicalPlan 再加一个 Sample 或者 Limit 的 logicalPlan
  private def withSample(ctx: SampleContext, query: LogicalPlan): LogicalPlan = withOrigin(ctx) {
    // Create a sampled plan if we need one.
    def sample(fraction: Double): Sample = {
      // The range of fraction accepted by Sample is [0, 1]. Because Hive's block sampling
      // function takes X PERCENT as the input and the range of X is [0, 100], we need to
      // adjust the fraction.
      val eps = RandomSampler.roundingEpsilon
      validate(fraction >= 0.0 - eps && fraction <= 1.0 + eps,
        s"Sampling fraction ($fraction) must be on interval [0, 1]",
        ctx)
      Sample(0.0, fraction, withReplacement = false, (math.random * 1000).toInt, query)(true)
    }

    ctx.sampleType.getType match {
      case SqlBaseParser.ROWS =>
        // 如果是按行数抽样，就是 Limit 这类 LogicalPlan
        Limit(expression(ctx.expression), query)

      case SqlBaseParser.PERCENTLIT =>
        val fraction = ctx.percentage.getText.toDouble
        // 按照百分比抽样，调用自身的 sample 方法，然后 Sample 这个 LogicalPlan
        sample(fraction / 100.0d)

      case SqlBaseParser.BYTELENGTH_LITERAL =>
        throw new ParseException(
          "TABLESAMPLE(byteLengthLiteral) is not supported", ctx)

      case SqlBaseParser.BUCKET if ctx.ON != null =>
        if (ctx.identifier != null) {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON colname) is not supported", ctx)
        } else {
          throw new ParseException(
            "TABLESAMPLE(BUCKET x OUT OF y ON function) is not supported", ctx)
        }

      case SqlBaseParser.BUCKET =>
        // 按桶抽样，也是 Sample
        sample(ctx.numerator.getText.toDouble / ctx.denominator.getText.toDouble)
    }
  }

  /**
   * Create a logical plan for a sub-query.
   */
   // 没啥好看的
  override def visitSubquery(ctx: SubqueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryNoWith)
  }

  /**
   * Create an un-aliased table reference. This is typically used for top-level table references,
   * for example:
   * {{{
   *   INSERT INTO db.tbl2
   *   TABLE db.tbl1
   * }}}
   */
  override def visitTable(ctx: TableContext): LogicalPlan = withOrigin(ctx) {
  // TABLE 关键字后面，紧跟着的是一个表，构造 一个 UnresolvedRelation 的 LogicalPlan
    UnresolvedRelation(visitTableIdentifier(ctx.tableIdentifier), None)
  }

  // 接下来几个都是关于 relation 的
  // relation 相关的语法规则为
  // relationPrimary
  //    : tableIdentifier sample? (AS? strictIdentifier)?               #tableName
  //    | '(' queryNoWith ')' sample? (AS? strictIdentifier)?           #aliasedQuery
  //    | '(' relation ')' sample? (AS? strictIdentifier)?              #aliasedRelation
  //    | inlineTable                                                   #inlineTableDefault2
  //    | identifier '(' (expression (',' expression)*)? ')'            #tableValuedFunction
  //    ;
  /**
   * Create an aliased table reference. This is typically used in FROM clauses.
   */
  // 最常见的一种形式，先拿到 table 名称，构建一个  UnresolvedRelation
  // 然后 如果要抽样。那就在 这个 UnresolvedRelation 的基础上，再加一个Sample 节点
  override def visitTableName(ctx: TableNameContext): LogicalPlan = withOrigin(ctx) {
    val table = UnresolvedRelation(
      visitTableIdentifier(ctx.tableIdentifier),
      Option(ctx.strictIdentifier).map(_.getText))
    table.optionalMap(ctx.sample)(withSample)
  }

  /**
   * Create a table-valued function call with arguments, e.g. range(1000)
   */
  // 个人理解是 select * from func(parameter1, parameter2) 这种，func 是一个函数名
  override def visitTableValuedFunction(ctx: TableValuedFunctionContext)
      : LogicalPlan = withOrigin(ctx) {
    UnresolvedTableValuedFunction(ctx.identifier.getText, ctx.expression.asScala.map(expression))
  }

  /**
   * Create an inline table (a virtual table in Hive parlance).
   */
   // 没太细看
  override def visitInlineTable(ctx: InlineTableContext): LogicalPlan = withOrigin(ctx) {
    // Get the backing expressions.
    val rows = ctx.expression.asScala.map { e =>
      expression(e) match {
        // inline table comes in two styles:
        // style 1: values (1), (2), (3)  -- multiple columns are supported
        // style 2: values 1, 2, 3  -- only a single column is supported here
        case struct: CreateNamedStruct => struct.valExprs // style 1
        case child => Seq(child)                          // style 2
      }
    }

    val aliases = if (ctx.identifierList != null) {
      visitIdentifierList(ctx.identifierList)
    } else {
      Seq.tabulate(rows.head.size)(i => s"col${i + 1}")
    }

    val table = UnresolvedInlineTable(aliases, rows)
    table.optionalMap(ctx.identifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a join relation. This is practically the same as
   * visitAliasedQuery and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
   // 个人理解是 select * from (table1 join table2) 这种
  override def visitAliasedRelation(ctx: AliasedRelationContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.relation)
      .optionalMap(ctx.sample)(withSample)
      .optionalMap(ctx.strictIdentifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a sub-query. This is practically the same as
   * visitAliasedRelation and visitNamedExpression, ANTLR4 however requires us to use 3 different
   * hooks.
   */
  // 个人理解是 select * from (select a1, a2 from table1) 这种
  override def visitAliasedQuery(ctx: AliasedQueryContext): LogicalPlan = withOrigin(ctx) {
    plan(ctx.queryNoWith)
      .optionalMap(ctx.sample)(withSample)
      .optionalMap(ctx.strictIdentifier)(aliasPlan)
  }

  /**
   * Create an alias (SubqueryAlias) for a LogicalPlan.
   */
   // relation 的别名，所以要再加一层
  private def aliasPlan(alias: ParserRuleContext, plan: LogicalPlan): LogicalPlan = {
    SubqueryAlias(alias.getText, plan, None)
  }

  /**
   * Create a Sequence of Strings for a parenthesis enclosed alias list.
   */
   // 没太细看
  override def visitIdentifierList(ctx: IdentifierListContext): Seq[String] = withOrigin(ctx) {
    visitIdentifierSeq(ctx.identifierSeq)
  }

  /**
   * Create a Sequence of Strings for an identifier list.
   */
  // 没太细看
  override def visitIdentifierSeq(ctx: IdentifierSeqContext): Seq[String] = withOrigin(ctx) {
    ctx.identifier.asScala.map(_.getText)
  }

  /* ********************************************************************************************
   * Table Identifier parsing
   * ******************************************************************************************** */
  /**
   * Create a [[TableIdentifier]] from a 'tableName' or 'databaseName'.'tableName' pattern.
   */
   // 其实也没啥看的，构造的 TableIdentifier 既不是 LogicalPlan 也不是 Expression
  override def visitTableIdentifier(
      ctx: TableIdentifierContext): TableIdentifier = withOrigin(ctx) {
    TableIdentifier(ctx.table.getText, Option(ctx.db).map(_.getText))
  }

  /* ********************************************************************************************
   * Expression parsing
   * ******************************************************************************************** */
  /**
   * Create an expression from the given context. This method just passes the context on to the
   * visitor and only takes care of typing (We assume that the visitor returns an Expression here).
   */
   // 接受一个 Context，构建以这个 context 为 root 的 expression
  protected def expression(ctx: ParserRuleContext): Expression = typedVisit(ctx)

  /**
   * Create sequence of expressions from the given sequence of contexts.
   */
   // 给定一批context 生成一批 expression
  private def expressionList(trees: java.util.List[ExpressionContext]): Seq[Expression] = {
    trees.asScala.map(expression)
  }

  /**
   * Create a star (i.e. all) expression; this selects all elements (in the specified object).
   * Both un-targeted (global) and targeted aliases are supported.
   */
  override def visitStar(ctx: StarContext): Expression = withOrigin(ctx) {
    // select * from
    // 或者 select table1.*
    // 反正select * 最终转换成 UnresolvedStar 这个 Expression
    UnresolvedStar(Option(ctx.qualifiedName()).map(_.identifier.asScala.map(_.getText)))
  }

  /**
   * Create an aliased expression if an alias is specified. Both single and multi-aliases are
   * supported.
   */
   // 语法规则如下：
   // namedExpression
   //    : expression (AS? (identifier | identifierList))?
   //    ;
  override def visitNamedExpression(ctx: NamedExpressionContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.expression)
    if (ctx.identifier != null) {
      // identifier 不为空，就是 Alias Expression
      // 例如 1 + 1 AS a
      Alias(e, ctx.identifier.getText)()
    } else if (ctx.identifierList != null) {
      // identifierList 不为空，就是 MultiAlias Expression
      // 例如 stack(2, key, value, key, value) as (a, b)
      MultiAlias(e, visitIdentifierList(ctx.identifierList))
    } else {
      // 如果没有 AS 关键字，那么就是原封不动的 Expression
      e
    }
  }

  /**
   * Combine a number of boolean expressions into a balanced expression tree. These expressions are
   * either combined by a logical [[And]] or a logical [[Or]].
   *
   * A balanced binary tree is created because regular left recursive trees cause considerable
   * performance degradations and can cause stack overflows.
   */
  // booleanExpression
  //    : NOT booleanExpression                                        #logicalNot
  //    | predicated                                                   #booleanDefault
  //    | left=booleanExpression operator=AND right=booleanExpression  #logicalBinary
  //    | left=booleanExpression operator=OR right=booleanExpression   #logicalBinary
  //    | EXISTS '(' query ')'                                         #exists
  //    ;
  //
  // 对应 #logicalBinary
  // 也就是 expression AND / OR expression
  //
  // 这里要把这一系列的 AND / OR 的 expression 转为平衡的二叉树，因为这样可以带来性能上的优化，不会产生 stack overflow
  override def visitLogicalBinary(ctx: LogicalBinaryContext): Expression = withOrigin(ctx) {
    val expressionType = ctx.operator.getType
    val expressionCombiner = expressionType match {
      case SqlBaseParser.AND => And.apply _
      case SqlBaseParser.OR => Or.apply _
    }

    // Collect all similar left hand contexts.
    val contexts = ArrayBuffer(ctx.right)
    var current = ctx.left
    def collectContexts: Boolean = current match {
      case lbc: LogicalBinaryContext if lbc.operator.getType == expressionType =>
        contexts += lbc.right
        current = lbc.left
        true
      case _ =>
        contexts += current
        false
    }
    while (collectContexts) {
      // No body - all updates take place in the collectContexts.
    }

    // Reverse the contexts to have them in the same sequence as in the SQL statement & turn them
    // into expressions.
    val expressions = contexts.reverse.map(expression)

    // Create a balanced tree.
    def reduceToExpressionTree(low: Int, high: Int): Expression = high - low match {
      case 0 =>
        expressions(low)
      case 1 =>
        expressionCombiner(expressions(low), expressions(high))
      case x =>
        val mid = low + x / 2
        expressionCombiner(
          reduceToExpressionTree(low, mid),
          reduceToExpressionTree(mid + 1, high))
    }
    reduceToExpressionTree(0, expressions.size - 1)
  }

  /**
   * Invert a boolean expression.
   */
  override def visitLogicalNot(ctx: LogicalNotContext): Expression = withOrigin(ctx) {
    // 非表达式，在正常 Expression 上再套一个 Not 表达式
    Not(expression(ctx.booleanExpression()))
  }

  /**
   * Create a filtering correlated sub-query (EXISTS).
   */
   //     | EXISTS '(' query ')'                                         #exists
  override def visitExists(ctx: ExistsContext): Expression = {
    // 所以 Exists 包住的是一个 LogicalPlan
    Exists(plan(ctx.query))
  }

  /**
   * Create a comparison expression. This compares two expressions. The following comparison
   * operators are supported:
   * - Equal: '=' or '=='
   * - Null-safe Equal: '<=>'
   * - Not Equal: '<>' or '!='
   * - Less than: '<'
   * - Less then or Equal: '<='
   * - Greater than: '>'
   * - Greater then or Equal: '>='
   */
   // 各种比较表达式
  override def visitComparison(ctx: ComparisonContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    val operator = ctx.comparisonOperator().getChild(0).asInstanceOf[TerminalNode]
    operator.getSymbol.getType match {
      case SqlBaseParser.EQ =>
        EqualTo(left, right)
      case SqlBaseParser.NSEQ =>
        EqualNullSafe(left, right)
      case SqlBaseParser.NEQ | SqlBaseParser.NEQJ =>
        Not(EqualTo(left, right))
      case SqlBaseParser.LT =>
        LessThan(left, right)
      case SqlBaseParser.LTE =>
        LessThanOrEqual(left, right)
      case SqlBaseParser.GT =>
        GreaterThan(left, right)
      case SqlBaseParser.GTE =>
        GreaterThanOrEqual(left, right)
    }
  }

  /**
   * Create a predicated expression. A predicated expression is a normal expression with a
   * predicate attached to it, for example:
   * {{{
   *    a + 1 IS NULL
   * }}}
   */
   // 语法规则如下：
   // predicated
   //    : valueExpression predicate?
   //    ;
   // 其实就是一个 正常的 Expression 和 一个 谓词的结合
  override def visitPredicated(ctx: PredicatedContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.valueExpression)
    // 因此最终的套路，就是先搞一个 正常的 Expression
    // 然后在这个的基础上套一个谓词 Expression
    if (ctx.predicate != null) {
      withPredicate(e, ctx.predicate)
    } else {
      e
    }
  }

  /**
   * Add a predicate to the given expression. Supported expressions are:
   * - (NOT) BETWEEN
   * - (NOT) IN
   * - (NOT) LIKE
   * - (NOT) RLIKE
   * - IS (NOT) NULL.
   */
   // 这一类的谓词，其实就是若干 Expression 的结合组成
  private def withPredicate(e: Expression, ctx: PredicateContext): Expression = withOrigin(ctx) {
    // Invert a predicate if it has a valid NOT clause.
    def invertIfNotDefined(e: Expression): Expression = ctx.NOT match {
      case null => e
      case not => Not(e)
    }

    // Create the predicate.
    ctx.kind.getType match {
      case SqlBaseParser.BETWEEN =>
        // BETWEEN is translated to lower <= e && e <= upper
        // BETWEEN 是好几个 Expression 的嵌套结合，包括 GreaterThanOrEqual LessThanOrEqual And
        invertIfNotDefined(And(
          GreaterThanOrEqual(e, expression(ctx.lower)),
          LessThanOrEqual(e, expression(ctx.upper))))
      case SqlBaseParser.IN if ctx.query != null =>
        invertIfNotDefined(In(e, Seq(ListQuery(plan(ctx.query)))))
      case SqlBaseParser.IN =>
        invertIfNotDefined(In(e, ctx.expression.asScala.map(expression)))
      case SqlBaseParser.LIKE =>
        invertIfNotDefined(Like(e, expression(ctx.pattern)))
      case SqlBaseParser.RLIKE =>
        invertIfNotDefined(RLike(e, expression(ctx.pattern)))
      case SqlBaseParser.NULL if ctx.NOT != null =>
        IsNotNull(e)
      case SqlBaseParser.NULL =>
        IsNull(e)
    }
  }

  /**
   * Create a binary arithmetic expression. The following arithmetic operators are supported:
   * - Multiplication: '*'
   * - Division: '/'
   * - Hive Long Division: 'DIV'
   * - Modulo: '%'
   * - Addition: '+'
   * - Subtraction: '-'
   * - Binary AND: '&'
   * - Binary XOR
   * - Binary OR: '|'
   */
   // 各类算术表达式
  override def visitArithmeticBinary(ctx: ArithmeticBinaryContext): Expression = withOrigin(ctx) {
    val left = expression(ctx.left)
    val right = expression(ctx.right)
    ctx.operator.getType match {
      case SqlBaseParser.ASTERISK =>
        Multiply(left, right)
      case SqlBaseParser.SLASH =>
        Divide(left, right)
      case SqlBaseParser.PERCENT =>
        Remainder(left, right)
      case SqlBaseParser.DIV =>
        Cast(Divide(left, right), LongType)
      case SqlBaseParser.PLUS =>
        Add(left, right)
      case SqlBaseParser.MINUS =>
        Subtract(left, right)
      case SqlBaseParser.AMPERSAND =>
        BitwiseAnd(left, right)
      case SqlBaseParser.HAT =>
        BitwiseXor(left, right)
      case SqlBaseParser.PIPE =>
        BitwiseOr(left, right)
    }
  }

  /**
   * Create a unary arithmetic expression. The following arithmetic operators are supported:
   * - Plus: '+'
   * - Minus: '-'
   * - Bitwise Not: '~'
   */
   // 单个节点的 + -？？？？？其实没想明白什么情况下使用
  override def visitArithmeticUnary(ctx: ArithmeticUnaryContext): Expression = withOrigin(ctx) {
    val value = expression(ctx.valueExpression)
    ctx.operator.getType match {
      case SqlBaseParser.PLUS =>
        value
      case SqlBaseParser.MINUS =>
        UnaryMinus(value)
      case SqlBaseParser.TILDE =>
        BitwiseNot(value)
    }
  }

  /**
   * Create a [[Cast]] expression.
   */
   // 强制转换
  override def visitCast(ctx: CastContext): Expression = withOrigin(ctx) {
    Cast(expression(ctx.expression), typedVisit(ctx.dataType))
  }

  /**
   * Create a (windowed) Function expression.
   */
   // 就是窗口函数，和之前分析的 窗口LogicalPlan 有点类似
   // 语法规则如下：
   //     qualifiedName '(' (setQuantifier? expression (',' expression)*)? ')' (OVER windowSpec)?  #functionCall
   // 没有细看
  override def visitFunctionCall(ctx: FunctionCallContext): Expression = withOrigin(ctx) {
    // Create the function call.
    val name = ctx.qualifiedName.getText
    val isDistinct = Option(ctx.setQuantifier()).exists(_.DISTINCT != null)
    val arguments = ctx.expression().asScala.map(expression) match {
      case Seq(UnresolvedStar(None)) if name.toLowerCase == "count" && !isDistinct =>
        // Transform COUNT(*) into COUNT(1).
        Seq(Literal(1))
      case expressions =>
        expressions
    }
    val function = UnresolvedFunction(visitFunctionName(ctx.qualifiedName), arguments, isDistinct)

    // Check if the function is evaluated in a windowed context.
    ctx.windowSpec match {
      case spec: WindowRefContext =>
        UnresolvedWindowExpression(function, visitWindowRef(spec))
      case spec: WindowDefContext =>
        WindowExpression(function, visitWindowDef(spec))
      case _ => function
    }
  }

  /**
   * Create a current timestamp/date expression. These are different from regular function because
   * they do not require the user to specify braces when calling them.
   */
   // 调用时间方法，也转换成了一个 Expression
  override def visitTimeFunctionCall(ctx: TimeFunctionCallContext): Expression = withOrigin(ctx) {
    ctx.name.getType match {
      case SqlBaseParser.CURRENT_DATE =>
        CurrentDate()
      case SqlBaseParser.CURRENT_TIMESTAMP =>
        CurrentTimestamp()
    }
  }

  /**
   * Create a function database (optional) and name pair.
   */
  protected def visitFunctionName(ctx: QualifiedNameContext): FunctionIdentifier = {
    ctx.identifier().asScala.map(_.getText) match {
      case Seq(db, fn) => FunctionIdentifier(fn, Option(db))  // 如果 ctx.identifier() 返回的Seq 长度是2，说明第一个元素是DB，第二个才是 函数名称
      case Seq(fn) => FunctionIdentifier(fn, None)  // 如果ctx.identifier() 返回的 Seq 长度是1，只需要用当前 DB 去构造 FunctionIdentifier
      case other => throw new ParseException(s"Unsupported function name '${ctx.getText}'", ctx)
    }
  }

  /**
   * Create a reference to a window frame, i.e. [[WindowSpecReference]].
   */
   // 没有细看
  override def visitWindowRef(ctx: WindowRefContext): WindowSpecReference = withOrigin(ctx) {
    WindowSpecReference(ctx.identifier.getText)
  }

  /**
   * Create a window definition, i.e. [[WindowSpecDefinition]].
   */
   // windowSpec
   //    : name=identifier  #windowRef
   //    | '('
   //      ( CLUSTER BY partition+=expression (',' partition+=expression)*
   //      | ((PARTITION | DISTRIBUTE) BY partition+=expression (',' partition+=expression)*)?
   //        ((ORDER | SORT) BY sortItem (',' sortItem)*)?)
   //      windowFrame?
   //      ')'              #windowDef
   //    ;
   //
   // 其实就是 窗口函数的具体定义
  override def visitWindowDef(ctx: WindowDefContext): WindowSpecDefinition = withOrigin(ctx) {
    // CLUSTER BY ... | PARTITION BY ... ORDER BY ...
    // 首先会生成 partition 和 order 的 Expression
    val partition = ctx.partition.asScala.map(expression)
    val order = ctx.sortItem.asScala.map(visitSortItem)

    // RANGE/ROWS BETWEEN ...
    // 然后生成 SpecifiedWindowFrame，不是Expression 也不是 LogicalPlan
    val frameSpecOption = Option(ctx.windowFrame).map { frame =>
      val frameType = frame.frameType.getType match {
        case SqlBaseParser.RANGE => RangeFrame
        case SqlBaseParser.ROWS => RowFrame
      }

      SpecifiedWindowFrame(
        frameType,
        visitFrameBound(frame.start),
        Option(frame.end).map(visitFrameBound).getOrElse(CurrentRow))
    }

    // 然后把上面这些 class 组合成为 窗口函数的 Expression
    WindowSpecDefinition(
      partition,
      order,
      frameSpecOption.getOrElse(UnspecifiedFrame))
  }

  /**
   * Create or resolve a [[FrameBoundary]]. Simple math expressions are allowed for Value
   * Preceding/Following boundaries. These expressions must be constant (foldable) and return an
   * integer value.
   */
   // 窗口函数里的上下限
  override def visitFrameBound(ctx: FrameBoundContext): FrameBoundary = withOrigin(ctx) {
    // We currently only allow foldable integers.
    def value: Int = {
      val e = expression(ctx.expression)
      validate(e.resolved && e.foldable && e.dataType == IntegerType,
        "Frame bound value must be a constant integer.",
        ctx)
      e.eval().asInstanceOf[Int]
    }

    // Create the FrameBoundary
    ctx.boundType.getType match {
      case SqlBaseParser.PRECEDING if ctx.UNBOUNDED != null =>
        UnboundedPreceding
      case SqlBaseParser.PRECEDING =>
        ValuePreceding(value)
      case SqlBaseParser.CURRENT =>
        CurrentRow
      case SqlBaseParser.FOLLOWING if ctx.UNBOUNDED != null =>
        UnboundedFollowing
      case SqlBaseParser.FOLLOWING =>
        ValueFollowing(value)
    }
  }

  /**
   * Create a [[CreateStruct]] expression.
   */
   // 没细看
  override def visitRowConstructor(ctx: RowConstructorContext): Expression = withOrigin(ctx) {
    CreateStruct(ctx.expression.asScala.map(expression))
  }

  /**
   * Create a [[ScalarSubquery]] expression.
   */
  // 没细看
  override def visitSubqueryExpression(
      ctx: SubqueryExpressionContext): Expression = withOrigin(ctx) {
      //
    ScalarSubquery(plan(ctx.query))
  }

  /**
   * Create a value based [[CaseWhen]] expression. This has the following SQL form:
   * {{{
   *   CASE [expression]
   *    WHEN [value] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   */
   // 又是一个组合的 Expression
   // 样式看注释
  override def visitSimpleCase(ctx: SimpleCaseContext): Expression = withOrigin(ctx) {
    val e = expression(ctx.value)
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (EqualTo(e, expression(wCtx.condition)), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Create a condition based [[CaseWhen]] expression. This has the following SQL syntax:
   * {{{
   *   CASE
   *    WHEN [predicate] THEN [expression]
   *    ...
   *    ELSE [expression]
   *   END
   * }}}
   *
   * @param ctx the parse tree
   *    */
   // 看注释很容易明白
  override def visitSearchedCase(ctx: SearchedCaseContext): Expression = withOrigin(ctx) {
    val branches = ctx.whenClause.asScala.map { wCtx =>
      (expression(wCtx.condition), expression(wCtx.result))
    }
    CaseWhen(branches, Option(ctx.elseExpression).map(expression))
  }

  /**
   * Create a dereference expression. The return type depends on the type of the parent, this can
   * either be a [[UnresolvedAttribute]] (if the parent is an [[UnresolvedAttribute]]), or an
   * [[UnresolvedExtractValue]] if the parent is some expression.
   */
   // 没看懂？？？？？？？
  override def visitDereference(ctx: DereferenceContext): Expression = withOrigin(ctx) {
    val attr = ctx.fieldName.getText
    expression(ctx.base) match {
      case UnresolvedAttribute(nameParts) =>
        UnresolvedAttribute(nameParts :+ attr)
      case e =>
        UnresolvedExtractValue(e, Literal(attr))
    }
  }

  /**
   * Create an [[UnresolvedAttribute]] expression.
   */
   //     | identifier     #columnReference
  override def visitColumnReference(ctx: ColumnReferenceContext): Expression = withOrigin(ctx) {
    UnresolvedAttribute.quoted(ctx.getText)
  }

  /**
   * Create an [[UnresolvedExtractValue]] expression, this is used for subscript access to an array.
   */
   // 似乎是说，某个值通过 数组下标去获取
  override def visitSubscript(ctx: SubscriptContext): Expression = withOrigin(ctx) {
    UnresolvedExtractValue(expression(ctx.value), expression(ctx.index))
  }

  /**
   * Create an expression for an expression between parentheses. This is need because the ANTLR
   * visitor cannot automatically convert the nested context into an expression.
   */
   // 没看懂
  override def visitParenthesizedExpression(
     ctx: ParenthesizedExpressionContext): Expression = withOrigin(ctx) {
    expression(ctx.expression)
  }

  /**
   * Create a [[SortOrder]] expression.
   */
   // 看具体实现实现，比较容易理解
  override def visitSortItem(ctx: SortItemContext): SortOrder = withOrigin(ctx) {
    val direction = if (ctx.DESC != null) {
      Descending
    } else {
      Ascending
    }
    val nullOrdering = if (ctx.FIRST != null) {
      NullsFirst
    } else if (ctx.LAST != null) {
      NullsLast
    } else {
      direction.defaultNullOrdering
    }
    SortOrder(expression(ctx.expression), direction, nullOrdering)
  }

  /**
   * Create a typed Literal expression. A typed literal has the following SQL syntax:
   * {{{
   *   [TYPE] '[VALUE]'
   * }}}
   * Currently Date, Timestamp and Binary typed literals are supported.
   */
   // 看起来也不是很重要，没看懂
  override def visitTypeConstructor(ctx: TypeConstructorContext): Literal = withOrigin(ctx) {
    val value = string(ctx.STRING)
    val valueType = ctx.identifier.getText.toUpperCase
    try {
      valueType match {
        case "DATE" =>
          Literal(Date.valueOf(value))
        case "TIMESTAMP" =>
          Literal(Timestamp.valueOf(value))
        case "X" =>
          val padding = if (value.length % 2 == 1) "0" else ""
          Literal(DatatypeConverter.parseHexBinary(padding + value))
        case other =>
          throw new ParseException(s"Literals of type '$other' are currently not supported.", ctx)
      }
    } catch {
      case e: IllegalArgumentException =>
        val message = Option(e.getMessage).getOrElse(s"Exception parsing $valueType")
        throw new ParseException(message, ctx)
    }
  }

  /**
   * Create a NULL literal expression.
   */
   // 空值
  override def visitNullLiteral(ctx: NullLiteralContext): Literal = withOrigin(ctx) {
    Literal(null)
  }

  /**
   * Create a Boolean literal expression.
   */
   // 要么 TRUE 要么 FALSE
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
   // 整形字面值
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue())
      case v if v.isValidLong =>
        Literal(v.longValue())
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
   // 也是字面值
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /** Create a numeric literal expression. */
  // 字面值
  private def numericLiteral
      (ctx: NumberContext, minValue: BigDecimal, maxValue: BigDecimal, typeName: String)
      (converter: String => Any): Literal = withOrigin(ctx) {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    try {
      val rawBigDecimal = BigDecimal(rawStrippedQualifier)
      if (rawBigDecimal < minValue || rawBigDecimal > maxValue) {
        throw new ParseException(s"Numeric literal ${rawStrippedQualifier} does not " +
          s"fit in range [${minValue}, ${maxValue}] for type ${typeName}", ctx)
      }
      Literal(converter(rawStrippedQualifier))
    } catch {
      case e: NumberFormatException =>
        throw new ParseException(e.getMessage, ctx)
    }
  }

  /**
   * Create a Byte Literal expression.
   */
   // 字面值
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    numericLiteral(ctx, Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
  // 字面值
    numericLiteral(ctx, Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
   //字面值
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    numericLiteral(ctx, Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Double Literal expression.
   */
  //字面值
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    numericLiteral(ctx, Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  //字面值
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /**
   * Create a String literal expression.
   */
  //字面值
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  /**
   * Create a String from a string literal context. This supports multiple consecutive string
   * literals, these are concatenated, for example this expression "'hello' 'world'" will be
   * converted into "helloworld".
   *
   * Special characters can be escaped by using Hive/C-style escaping.
   */
   // 内部调用
  private def createString(ctx: StringLiteralContext): String = {
    ctx.STRING().asScala.map(string).mkString
  }

  /**
   * Create a [[CalendarInterval]] literal expression. An interval expression can contain multiple
   * unit value pairs, for instance: interval 2 months 2 days.
   */
   // 未看
  override def visitInterval(ctx: IntervalContext): Literal = withOrigin(ctx) {
    val intervals = ctx.intervalField.asScala.map(visitIntervalField)
    validate(intervals.nonEmpty, "at least one time unit should be given for interval literal", ctx)
    Literal(intervals.reduce(_.add(_)))
  }

  /**
   * Create a [[CalendarInterval]] for a unit value pair. Two unit configuration types are
   * supported:
   * - Single unit.
   * - From-To unit (only 'YEAR TO MONTH' and 'DAY TO SECOND' are supported).
   */
   // 未看
  override def visitIntervalField(ctx: IntervalFieldContext): CalendarInterval = withOrigin(ctx) {
    import ctx._
    val s = value.getText
    try {
      val interval = (unit.getText.toLowerCase, Option(to).map(_.getText.toLowerCase)) match {
        case (u, None) if u.endsWith("s") =>
          // Handle plural forms, e.g: yearS/monthS/weekS/dayS/hourS/minuteS/hourS/...
          CalendarInterval.fromSingleUnitString(u.substring(0, u.length - 1), s)
        case (u, None) =>
          CalendarInterval.fromSingleUnitString(u, s)
        case ("year", Some("month")) =>
          CalendarInterval.fromYearMonthString(s)
        case ("day", Some("second")) =>
          CalendarInterval.fromDayTimeString(s)
        case (from, Some(t)) =>
          throw new ParseException(s"Intervals FROM $from TO $t are not supported.", ctx)
      }
      validate(interval != null, "No interval can be constructed", ctx)
      interval
    } catch {
      // Handle Exceptions thrown by CalendarInterval
      case e: IllegalArgumentException =>
        val pe = new ParseException(e.getMessage, ctx)
        pe.setStackTrace(e.getStackTrace)
        throw pe
    }
  }

  /* ********************************************************************************************
   * DataType parsing
   * ******************************************************************************************** */
  /**
   * Resolve/create a primitive type.
   */
   // 数据类型
   // 数据类型 不是 Expression 也不是 LogicalPlan
  override def visitPrimitiveDataType(ctx: PrimitiveDataTypeContext): DataType = withOrigin(ctx) {
    (ctx.identifier.getText.toLowerCase, ctx.INTEGER_VALUE().asScala.toList) match {
      case ("boolean", Nil) => BooleanType
      case ("tinyint" | "byte", Nil) => ByteType
      case ("smallint" | "short", Nil) => ShortType
      case ("int" | "integer", Nil) => IntegerType
      case ("bigint" | "long", Nil) => LongType
      case ("float", Nil) => FloatType
      case ("double", Nil) => DoubleType
      case ("date", Nil) => DateType
      case ("timestamp", Nil) => TimestampType
      case ("char" | "varchar" | "string", Nil) => StringType
      case ("char" | "varchar", _ :: Nil) => StringType
      case ("binary", Nil) => BinaryType
      case ("decimal", Nil) => DecimalType.USER_DEFAULT
      case ("decimal", precision :: Nil) => DecimalType(precision.getText.toInt, 0)
      case ("decimal", precision :: scale :: Nil) =>
        DecimalType(precision.getText.toInt, scale.getText.toInt)
      case (dt, params) =>
        throw new ParseException(
          s"DataType $dt${params.mkString("(", ",", ")")} is not supported.", ctx)
    }
  }

  /**
   * Create a complex DataType. Arrays, Maps and Structures are supported.
   */
   // 复杂数据类型
  override def visitComplexDataType(ctx: ComplexDataTypeContext): DataType = withOrigin(ctx) {
    ctx.complex.getType match {
      case SqlBaseParser.ARRAY =>
        ArrayType(typedVisit(ctx.dataType(0)))
      case SqlBaseParser.MAP =>
        MapType(typedVisit(ctx.dataType(0)), typedVisit(ctx.dataType(1)))
      case SqlBaseParser.STRUCT =>
        createStructType(ctx.complexColTypeList())
    }
  }

  /**
   * Create top level table schema.
   */
  protected def createSchema(ctx: ColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitColTypeList(ctx: ColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.colType().asScala.map(visitColType)
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitColType(ctx: ColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(identifier.getText, typedVisit(dataType), nullable = true)
    if (STRING == null) structField else structField.withComment(string(STRING))
  }

  /**
   * Create a [[StructType]] from a sequence of [[StructField]]s.
   */
  protected def createStructType(ctx: ComplexColTypeListContext): StructType = {
    StructType(Option(ctx).toSeq.flatMap(visitComplexColTypeList))
  }

  /**
   * Create a [[StructType]] from a number of column definitions.
   */
  override def visitComplexColTypeList(
      ctx: ComplexColTypeListContext): Seq[StructField] = withOrigin(ctx) {
    ctx.complexColType().asScala.map(visitComplexColType)
  }

  /**
   * Create a [[StructField]] from a column definition.
   */
  override def visitComplexColType(ctx: ComplexColTypeContext): StructField = withOrigin(ctx) {
    import ctx._
    val structField = StructField(identifier.getText, typedVisit(dataType), nullable = true)
    if (STRING == null) structField else structField.withComment(string(STRING))
  }
}
