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

import java.util.Locale
import org.antlr.v4.runtime._
import org.antlr.v4.runtime.atn.PredictionMode
import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.misc.ParseCancellationException
import org.antlr.v4.runtime.tree.TerminalNodeImpl
import org.apache.iceberg.common.DynConstructors
import org.apache.iceberg.spark.ExtendedParser
import org.apache.iceberg.spark.ExtendedParser.RawOrderField
import org.apache.iceberg.spark.Spark3Util
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.analysis.RewriteViewCommands
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.NonReservedContext
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.QuotedIdentifierContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._
import scala.util.Try

class IcebergSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface with ExtendedParser {

  import IcebergSparkSqlExtensionsParser._

  private lazy val substitutor = substitutorCtor.newInstance(SQLConf.get)
  private lazy val astBuilder = new IcebergSqlExtensionsAstBuilder(delegate)

  /**
   * Parse a string to a DataType.
   */
  override def parseDataType(sqlText: String): DataType = {
    delegate.parseDataType(sqlText)
  }

  /**
   * Parse a string to a raw DataType without CHAR/VARCHAR replacement.
   */
  def parseRawDataType(sqlText: String): DataType = throw new UnsupportedOperationException()

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

  override def parseSortOrder(sqlText: String): java.util.List[RawOrderField] = {
    val fields = parse(sqlText) { parser => astBuilder.visitSingleOrder(parser.singleOrder()) }
    fields.map { field =>
      val (term, direction, order) = field
      new RawOrderField(term, direction, order)
    }.asJava
  }

  /**
   * Parse a string to a LogicalPlan.
   */
  override def parsePlan(sqlText: String): LogicalPlan = {
    val sqlTextAfterSubstitution = substitutor.substitute(sqlText)
    if (isIcebergCommand(sqlTextAfterSubstitution)) {
      parse(sqlTextAfterSubstitution) { parser => astBuilder.visit(parser.singleStatement()) }.asInstanceOf[LogicalPlan]
    } else {
      RewriteViewCommands(SparkSession.active).apply(delegate.parsePlan(sqlText))
    }
  }

  private def isIcebergCommand(sqlText: String): Boolean = {
    val normalized = sqlText.toLowerCase(Locale.ROOT).trim()
      // Strip simple SQL comments that terminate a line, e.g. comments starting with `--` .
      .replaceAll("--.*?\\n", " ")
      // Strip newlines.
      .replaceAll("\\s+", " ")
      // Strip comments of the form  /* ... */. This must come after stripping newlines so that
      // comments that span multiple lines are caught.
      .replaceAll("/\\*.*?\\*/", " ")
      .trim()
    normalized.startsWith("call") || (
        normalized.startsWith("alter table") && (
            normalized.contains("add partition field") ||
            normalized.contains("drop partition field") ||
            normalized.contains("replace partition field") ||
            normalized.contains("write ordered by") ||
            normalized.contains("write locally ordered by") ||
            normalized.contains("write distributed by") ||
            normalized.contains("write unordered") ||
            normalized.contains("set identifier fields") ||
            normalized.contains("drop identifier fields") ||
            isSnapshotRefDdl(normalized)))
  }

  private def isSnapshotRefDdl(normalized: String): Boolean = {
    normalized.contains("create branch") ||
      normalized.contains("replace branch") ||
      normalized.contains("create tag") ||
      normalized.contains("replace tag") ||
      normalized.contains("drop branch") ||
      normalized.contains("drop tag")
  }

  protected def parse[T](command: String)(toResult: IcebergSqlExtensionsParser => T): T = {
    val lexer = new IcebergSqlExtensionsLexer(new UpperCaseCharStream(CharStreams.fromString(command)))
    lexer.removeErrorListeners()
    lexer.addErrorListener(IcebergParseErrorListener)

    val tokenStream = new CommonTokenStream(lexer)
    val parser = new IcebergSqlExtensionsParser(tokenStream)
    parser.addParseListener(IcebergSqlExtensionsPostProcessor)
    parser.removeErrorListeners()
    parser.addErrorListener(IcebergParseErrorListener)

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
      case e: IcebergParseException if e.command.isDefined =>
        throw e
      case e: IcebergParseException =>
        throw e.withCommand(command)
      case e: AnalysisException =>
        val position = Origin(e.line, e.startPosition)
        throw new IcebergParseException(Option(command), e.message, position, position)
    }
  }

  override def parseQuery(sqlText: String): LogicalPlan = {
    parsePlan(sqlText)
  }
}

object IcebergSparkSqlExtensionsParser {
  private val substitutorCtor: DynConstructors.Ctor[VariableSubstitution] =
    DynConstructors.builder()
      .impl(classOf[VariableSubstitution])
      .impl(classOf[VariableSubstitution], classOf[SQLConf])
      .build()
}

/* Copied from Apache Spark's to avoid dependency on Spark Internals */
class UpperCaseCharStream(wrapped: CodePointCharStream) extends CharStream {
  override def consume(): Unit = wrapped.consume
  override def getSourceName(): String = wrapped.getSourceName
  override def index(): Int = wrapped.index
  override def mark(): Int = wrapped.mark
  override def release(marker: Int): Unit = wrapped.release(marker)
  override def seek(where: Int): Unit = wrapped.seek(where)
  override def size(): Int = wrapped.size

  override def getText(interval: Interval): String = wrapped.getText(interval)

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
case object IcebergSqlExtensionsPostProcessor extends IcebergSqlExtensionsBaseListener {

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

/* Partially copied from Apache Spark's Parser to avoid dependency on Spark Internals */
case object IcebergParseErrorListener extends BaseErrorListener {
  override def syntaxError(
      recognizer: Recognizer[_, _],
      offendingSymbol: scala.Any,
      line: Int,
      charPositionInLine: Int,
      msg: String,
      e: RecognitionException): Unit = {
    val (start, stop) = offendingSymbol match {
      case token: CommonToken =>
        val start = Origin(Some(line), Some(token.getCharPositionInLine))
        val length = token.getStopIndex - token.getStartIndex + 1
        val stop = Origin(Some(line), Some(token.getCharPositionInLine + length))
        (start, stop)
      case _ =>
        val start = Origin(Some(line), Some(charPositionInLine))
        (start, start)
    }
    throw new IcebergParseException(None, msg, start, stop)
  }
}

/**
 * Copied from Apache Spark
 * A [[ParseException]] is an [[AnalysisException]] that is thrown during the parse process. It
 * contains fields and an extended error message that make reporting and diagnosing errors easier.
 */
class IcebergParseException(
    val command: Option[String],
    message: String,
    val start: Origin,
    val stop: Origin) extends AnalysisException(message, start.line, start.startPosition) {

  def this(message: String, ctx: ParserRuleContext) = {
    this(Option(IcebergParserUtils.command(ctx)),
      message,
      IcebergParserUtils.position(ctx.getStart),
      IcebergParserUtils.position(ctx.getStop))
  }

  override def getMessage: String = {
    val builder = new StringBuilder
    builder ++= "\n" ++= message
    start match {
      case Origin(
          Some(l), Some(p), Some(startIndex), Some(stopIndex), Some(sqlText), Some(objectType),
          Some(objectName), _, _) =>
        builder ++= s"(line $l, pos $p)\n"
        command.foreach { cmd =>
          val (above, below) = cmd.split("\n").splitAt(l)
          builder ++= "\n== SQL ==\n"
          above.foreach(builder ++= _ += '\n')
          builder ++= (0 until p).map(_ => "-").mkString("") ++= "^^^\n"
          below.foreach(builder ++= _ += '\n')
        }
      case _ =>
        command.foreach { cmd =>
          builder ++= "\n== SQL ==\n" ++= cmd
        }
    }
    builder.toString
  }

  def withCommand(cmd: String): IcebergParseException = {
    new IcebergParseException(Option(cmd), message, start, stop)
  }
}