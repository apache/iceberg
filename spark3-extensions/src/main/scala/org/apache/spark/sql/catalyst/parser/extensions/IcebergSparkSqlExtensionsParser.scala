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
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.{Interval, ParseCancellationException}
import org.antlr.v4.runtime.tree.{ParseTree, TerminalNodeImpl}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.parser.{ParseErrorListener, ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.parser.ParserUtils._
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser._
import org.apache.spark.sql.catalyst.plans.logical.{CallArgument, CallStatement, LogicalPlan, NamedArgument, PositionalArgument}
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.types.{ByteType, DataType, DoubleType, FloatType, LongType, ShortType, StructType}
import scala.collection.JavaConverters._

class IcebergSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface {

  private val astBuilder = new IcebergSqlExtensionsAstBuilder()

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
   * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
   */
  override def parseRawDataType(sqlText: String): DataType = {
    delegate.parseRawDataType(sqlText)
  }

  /**
   * Parse a string to an Expression.
   */
  override def parseExpression(sqlText: String): Expression = {
    delegate.parseExpression(sqlText)
  }

  /**
   * Parse a string to a TableIdentifier.
   */
  override def parseTableIdentifier(sqlText: String): TableIdentifier = {
    delegate.parseTableIdentifier(sqlText)
  }

  /**
   * Parse a string to a FunctionIdentifier.
   */
  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier = {
    delegate.parseFunctionIdentifier(sqlText)
  }

  /**
   * Parse a string to a multi-part identifier.
   */
  override def parseMultipartIdentifier(sqlText: String): Seq[String] = {
    delegate.parseMultipartIdentifier(sqlText)
  }

  /**
   * Creates StructType for a given SQL string, which is a comma separated list of field
   * definitions which will preserve the correct Hive metadata.
   */
  override def parseTableSchema(sqlText: String): StructType = {
    delegate.parseTableSchema(sqlText)
  }

  /**
   * Parse a string to a LogicalPlan.
   */
  override def parsePlan(sqlText: String): LogicalPlan = parse(sqlText) { parser =>
    astBuilder.visit(parser.singleStatement()) match {
      case plan: LogicalPlan => plan
      case _ => delegate.parsePlan(sqlText)
    }
  }

  protected def parse[T](command: String)(toResult: IcebergSqlExtensionsParser => T): T = {
    val lexer = new IcebergSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(ParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new IcebergSqlExtensionsParser(tokenStream)
    parser.addParseListener(PostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(ParseErrorListener)

    try {
      try {
        // first, try parsing with potentially faster SLL mode
        parser.getInterpreter.setPredictionMode(PredictionMode.SLL)
        toResult(parser)
      }
      catch {
        case _: ParseCancellationException =>
          // if we fail, parse with LL mode
          tokenStream.seek(0) // rewind input stream
          parser.reset()

          // Try Again.
          parser.getInterpreter.setPredictionMode(PredictionMode.LL)
          toResult(parser)
      }
    }
    catch {
      case e: ParseException if e.command.isDefined =>
        throw e
      case e: ParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new ParseException(Option(command), e.message, position, position)
    }
  }
}

// literal parsing is taken from Spark's AstBuilder
class IcebergSqlExtensionsAstBuilder extends IcebergSqlExtensionsBaseVisitor[AnyRef] {

  override def visitCall(ctx: CallContext): LogicalPlan = {
    val name = ctx.multipartIdentifier.parts.asScala.map(_.getText)
    val args = ctx.callArgument.asScala.map(typedVisit[CallArgument])
    CallStatement(name, args)
  }

  override def visitPositionalArgument(ctx: PositionalArgumentContext): CallArgument = {
    val expr = typedVisit[Expression](ctx.expression)
    PositionalArgument(expr)
  }

  override def visitNamedArgument(ctx: NamedArgumentContext): CallArgument = {
    val name = ctx.identifier.getText
    val expr = typedVisit[Expression](ctx.expression)
    NamedArgument(name, expr)
  }

  override def visitNonIcebergCommand(ctx: NonIcebergCommandContext): LogicalPlan = null

  override def visitSingleStatement(ctx: SingleStatementContext): LogicalPlan = withOrigin(ctx) {
    visit(ctx.statement).asInstanceOf[LogicalPlan]
  }

  /**
   * Create an integral literal expression. The code selects the most narrow integral type
   * possible, either a BigDecimal, a Long or an Integer is returned.
   */
  override def visitIntegerLiteral(ctx: IntegerLiteralContext): Literal = withOrigin(ctx) {
    BigDecimal(ctx.getText) match {
      case v if v.isValidInt =>
        Literal(v.intValue)
      case v if v.isValidLong =>
        Literal(v.longValue)
      case v => Literal(v.underlying())
    }
  }

  /**
   * Create a Byte Literal expression.
   */
  override def visitTinyIntLiteral(ctx: TinyIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier, Byte.MinValue, Byte.MaxValue, ByteType.simpleString)(_.toByte)
  }

  /**
   * Create a Short Literal expression.
   */
  override def visitSmallIntLiteral(ctx: SmallIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier, Short.MinValue, Short.MaxValue, ShortType.simpleString)(_.toShort)
  }

  /**
   * Create a Long Literal expression.
   */
  override def visitBigIntLiteral(ctx: BigIntLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier, Long.MinValue, Long.MaxValue, LongType.simpleString)(_.toLong)
  }

  /**
   * Create a Float Literal expression.
   */
  override def visitFloatLiteral(ctx: FloatLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier, Float.MinValue, Float.MaxValue, FloatType.simpleString)(_.toFloat)
  }

  /**
   * Create a Double Literal expression.
   */
  override def visitDoubleLiteral(ctx: DoubleLiteralContext): Literal = {
    val rawStrippedQualifier = ctx.getText.substring(0, ctx.getText.length - 1)
    numericLiteral(ctx, rawStrippedQualifier, Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a double literal for number with an exponent, e.g. 1E-30
   */
  override def visitExponentLiteral(ctx: ExponentLiteralContext): Literal = {
    numericLiteral(ctx, ctx.getText, /* exponent values don't have a suffix */
      Double.MinValue, Double.MaxValue, DoubleType.simpleString)(_.toDouble)
  }

  /**
   * Create a decimal literal for a regular decimal number.
   */
  override def visitDecimalLiteral(ctx: DecimalLiteralContext): Literal = withOrigin(ctx) {
    Literal(BigDecimal(ctx.getText).underlying())
  }

  /**
   * Create a BigDecimal Literal expression.
   */
  override def visitBigDecimalLiteral(ctx: BigDecimalLiteralContext): Literal = {
    val raw = ctx.getText.substring(0, ctx.getText.length - 2)
    try {
      Literal(BigDecimal(raw).underlying())
    } catch {
      case e: AnalysisException =>
        throw new ParseException(e.message, ctx)
    }
  }

  /** Create a numeric literal expression. */
  private def numericLiteral(
      ctx: NumberContext,
      rawStrippedQualifier: String,
      minValue: BigDecimal,
      maxValue: BigDecimal,
      typeName: String)(converter: String => Any): Literal = withOrigin(ctx) {
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
   * Create a Boolean literal expression.
   */
  override def visitBooleanLiteral(ctx: BooleanLiteralContext): Literal = withOrigin(ctx) {
    if (ctx.getText.toBoolean) {
      Literal.TrueLiteral
    } else {
      Literal.FalseLiteral
    }
  }

  /**
   * Create a String literal expression.
   */
  override def visitStringLiteral(ctx: StringLiteralContext): Literal = withOrigin(ctx) {
    Literal(createString(ctx))
  }

  // we ignore legacy spark.sql.parser.escapedStringLiterals to avoid a dependency on SQLConf in the parser
  private def createString(ctx: StringLiteralContext): String = {
    ctx.STRING().asScala.map(string).mkString
  }

  private def typedVisit[T](ctx: ParseTree): T = {
    ctx.accept(this).asInstanceOf[T]
  }
}

// copied from Spark as the original definition is private
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume()
  override def getSourceName: String = wrapped.getSourceName
  override def index: Int = wrapped.index
  override def mark: Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size: Int = wrapped.size

  override def getText(interval: Interval): String = {
    // ANTLR 4.7's CodePointCharStream implementations have bugs when
    // getText() is called with an empty stream, or intervals where
    // the start > end. See
    // https://github.com/antlr/antlr4/commit/ac9f7530 for one fix
    // that is not yet in a released ANTLR artifact.
    if (size() > 0 && (interval.b - interval.a >= 0)) {
      wrapped.getText(interval)
    } else {
      ""
    }
  }

  // scalastyle:off
  override def LA(i: Int): Int = {
    val la = wrapped.LA(i)
    if (la == 0 || la == IntStream.EOF) la
    else Character.toUpperCase(la)
  }
  // scalastyle:on
}

/**
 * The post-processor validates & cleans-up the parse tree during the parse process.
 */
// while we reuse ParseErrorListener and ParseException, we have to copy and modify PostProcessor
case object PostProcessor extends IcebergSqlExtensionsBaseListener {

  /** Remove the back ticks from an Identifier. */
  override def exitQuotedIdentifier(ctx: QuotedIdentifierContext): Unit = {
    replaceTokenByIdentifier(ctx, 1) { token =>
      // Remove the double back ticks in the string.
      token.setText(token.getText.replace("``", "`"))
      token
    }
  }

  /** Treat non-reserved keywords as Identifiers. */
  override def exitNonReserved(ctx: NonReservedContext): Unit = {
    replaceTokenByIdentifier(ctx, 0)(identity)
  }

  private def replaceTokenByIdentifier(
      ctx: ParserRuleContext,
      stripMargins: Int)(
      f: CommonToken => CommonToken = identity): Unit = {
    val parent = ctx.getParent
    parent.removeLastChild()
    val token = ctx.getChild(0).getPayload.asInstanceOf[Token]
    val newToken = new CommonToken(
      new org.antlr.v4.runtime.misc.Pair(token.getTokenSource, token.getInputStream),
      IcebergSqlExtensionsParser.IDENTIFIER,
      token.getChannel,
      token.getStartIndex + stripMargins,
      token.getStopIndex - stripMargins)
    parent.addChild(new TerminalNodeImpl(f(newToken)))
  }
}
