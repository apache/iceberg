/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.catalyst.parser.extensions

import org.antlr.v4.runtime._
import org.antlr.v4.runtime.tree.ParseTree
import org.antlr.v4.runtime.tree.TerminalNode
import org.apache.iceberg.NullOrder
import org.apache.iceberg.SortDirection
import org.apache.iceberg.expressions.Term
import org.apache.iceberg.spark.Spark3Util
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical.AddPartitionField
import org.apache.spark.sql.catalyst.plans.logical.CallArgument
import org.apache.spark.sql.catalyst.plans.logical.CallStatement
import org.apache.spark.sql.catalyst.plans.logical.DropPartitionField
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.NamedArgument
import org.apache.spark.sql.catalyst.plans.logical.PositionalArgument
import org.apache.spark.sql.catalyst.plans.logical.SetWriteOrder
import org.apache.spark.sql.connector.expressions
import org.apache.spark.sql.connector.expressions.ApplyTransform
import org.apache.spark.sql.connector.expressions.FieldReference
import org.apache.spark.sql.connector.expressions.IdentityTransform
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.connector.expressions.Transform
import scala.collection.JavaConverters._

class IcebergSqlExtensionsAstBuilder(delegate: ParserInterface) extends IcebergSqlExtensionsBaseVisitor[AnyRef] {

  /**
   * Create a [[CallStatement]] for a stored procedure call.
   */
  override def visitCall(ctx: CallContext): CallStatement = withOrigin(ctx) {
    val name = ctx.multipartIdentifier.parts.asScala.map(_.getText)
    val args = ctx.callArgument.asScala.map(typedVisit[CallArgument])
    CallStatement(name, args)
  }

  /**
   * Create an ADD PARTITION FIELD logical command.
   */
  override def visitAddPartitionField(ctx: AddPartitionFieldContext): AddPartitionField = withOrigin(ctx) {
    AddPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform),
      Option(ctx.name).map(_.getText))
  }

  /**
   * Create a DROP PARTITION FIELD logical command.
   */
  override def visitDropPartitionField(ctx: DropPartitionFieldContext): DropPartitionField = withOrigin(ctx) {
    DropPartitionField(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      typedVisit[Transform](ctx.transform))
  }

  /**
   * Create a WRITE ORDERED BY logical command.
   */
  override def visitSetTableOrder(ctx: SetTableOrderContext): SetWriteOrder = withOrigin(ctx) {
    SetWriteOrder(
      typedVisit[Seq[String]](ctx.multipartIdentifier),
      ctx.order.fields.asScala.map(typedVisit[(Term, SortDirection, NullOrder)]).toArray)
  }

  /**
   * Create an order field.
   */
  override def visitOrderField(ctx: OrderFieldContext): (Term, SortDirection, NullOrder) = {
    val term = Spark3Util.toIcebergTerm(typedVisit[Transform](ctx.transform))
    val direction = Option(ctx.ASC).map(_ => SortDirection.ASC)
        .orElse(Option(ctx.DESC).map(_ => SortDirection.DESC))
        .getOrElse(SortDirection.ASC)
    val nullOrder = Option(ctx.FIRST).map(_ => NullOrder.NULLS_FIRST)
        .orElse(Option(ctx.LAST).map(_ => NullOrder.NULLS_LAST))
        .getOrElse(if (direction == SortDirection.ASC) NullOrder.NULLS_FIRST else NullOrder.NULLS_LAST)
    (term, direction, nullOrder)
  }

  /**
   * Create an IdentityTransform for a column reference.
   */
  override def visitIdentityTransform(ctx: IdentityTransformContext): Transform = withOrigin(ctx) {
    IdentityTransform(FieldReference(typedVisit[Seq[String]](ctx.multipartIdentifier())))
  }

  /**
   * Create a named Transform from argument expressions.
   */
  override def visitApplyTransform(ctx: ApplyTransformContext): Transform = withOrigin(ctx) {
    val args = ctx.arguments.asScala.map(typedVisit[expressions.Expression])
    ApplyTransform(ctx.transformName.getText, args)
  }

  /**
   * Create a transform argument from a column reference or a constant.
   */
  override def visitTransformArgument(ctx: TransformArgumentContext): expressions.Expression = withOrigin(ctx) {
    val reference = Option(ctx.multipartIdentifier())
        .map(typedVisit[Seq[String]])
        .map(FieldReference(_))
    val literal = Option(ctx.constant)
        .map(visitConstant)
        .map(lit => LiteralValue(lit.value, lit.dataType))
    reference.orElse(literal)
        .getOrElse(throw new ParseException(s"Invalid transform argument", ctx))
  }

  /**
   * Return a multi-part identifier as Seq[String].
   */
  override def visitMultipartIdentifier(ctx: MultipartIdentifierContext): Seq[String] = withOrigin(ctx) {
    ctx.parts.asScala.map(_.getText)
  }

  /**
   * Create a positional argument in a stored procedure call.
   */
  override def visitPositionalArgument(ctx: PositionalArgumentContext): CallArgument = withOrigin(ctx) {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }

  /**
   * Create a named argument in a stored procedure call.
   */
  override def visitNamedArgument(ctx: NamedArgumentContext): CallArgument = withOrigin(ctx) {
    val name = ctx.identifier.getText
    val expr = typedVisit[Expression](ctx.expression)
    NamedArgument(name, expr)
  }

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  def visitConstant(ctx: ConstantContext): Literal = {
    delegate.parseExpression(ctx.getText).asInstanceOf[Literal]
  }

  override def visitExpression(ctx: ExpressionContext): Expression = {
    // reconstruct the SQL string and parse it using the main Spark parser
    // while we can avoid the logic to build Spark expressions, we still have to parse them
    // we cannot call ctx.getText directly since it will not render spaces correctly
    // that's why we need to recurse down the tree in reconstructSqlString
    val sqlString = reconstructSqlString(ctx)
    delegate.parseExpression(sqlString)
  }

  private def reconstructSqlString(ctx: ParserRuleContext): String = {
    ctx.children.asScala.map {
      case c: ParserRuleContext => reconstructSqlString(c)
      case t: TerminalNode => t.getText
    }.mkString(" ")
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
}
