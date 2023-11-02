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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException
import org.apache.spark.sql.catalyst.analysis.ResolvedPersistentView
import org.apache.spark.sql.catalyst.analysis.ResolvedTable
import org.apache.spark.sql.catalyst.analysis.ResolvedTempView
import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer.resolver
import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedNamespace
import org.apache.spark.sql.catalyst.analysis.UnresolvedTableOrView
import org.apache.spark.sql.catalyst.analysis.UnresolvedView
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.catalog.TemporaryViewRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.NonReservedContext
import org.apache.spark.sql.catalyst.parser.extensions.IcebergSqlExtensionsParser.QuotedIdentifierContext
import org.apache.spark.sql.catalyst.plans.logical.CreateView
import org.apache.spark.sql.catalyst.plans.logical.DropView
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.ShowViews
import org.apache.spark.sql.catalyst.plans.logical.views.CreateIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.DropIcebergView
import org.apache.spark.sql.catalyst.plans.logical.views.ResolvedV2View
import org.apache.spark.sql.catalyst.plans.logical.views.ShowIcebergViews
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.trees.Origin
import org.apache.spark.sql.connector.catalog.{View => V2View}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.CatalogV2Util
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.V1Table
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.VariableSubstitution
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.types.StructType
import scala.jdk.CollectionConverters._

class IcebergSparkSqlExtensionsParser(delegate: ParserInterface) extends ParserInterface with ExtendedParser {

  import IcebergSparkSqlExtensionsParser._

  private lazy val substitutor = substitutorCtor.newInstance(SQLConf.get)
  private lazy val maxIterations = SQLConf.get.analyzerMaxIterations
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
      ViewSubstitutionExecutor.execute(delegate.parsePlan(sqlText))
    }
  }

  private object ViewSubstitutionExecutor extends RuleExecutor[LogicalPlan] {
    private val fixedPoint = FixedPoint(
      maxIterations,
      errorOnExceed = true,
      maxIterationsSetting = SQLConf.ANALYZER_MAX_ITERATIONS.key)

    override protected def batches: Seq[Batch] = Seq(Batch("pre-substitution", fixedPoint, V2ViewSubstitution))
  }

  private object V2ViewSubstitution extends Rule[LogicalPlan] {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

    // the reason for handling these cases here is because ResolveSessionCatalog exits early for v2 commands
    override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
      case u@UnresolvedView(identifier, _, _, _) =>
        lookupTableOrView(identifier, viewOnly = true).getOrElse(u)

      case u@UnresolvedTableOrView(identifier, _, _) =>
        lookupTableOrView(identifier).getOrElse(u)

      case CreateView(UnresolvedIdentifier(nameParts, allowTemp), userSpecifiedColumns,
      comment, properties, originalText, query, allowExisting, replace) =>
        CreateIcebergView(UnresolvedIdentifier(nameParts, allowTemp), userSpecifiedColumns,
          comment, properties, originalText, query, allowExisting, replace)

      case ShowViews(UnresolvedNamespace(multipartIdentifier), pattern, output) =>
        ShowIcebergViews(UnresolvedNamespace(multipartIdentifier), pattern, output)

      case DropView(UnresolvedIdentifier(nameParts, allowTemp), ifExists) =>
        DropIcebergView(UnresolvedIdentifier(nameParts, allowTemp), ifExists)
    }

    private def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
      if (!isResolvingView || isReferredTempViewName(nameParts)) return nameParts

      if (nameParts.length == 1) {
        AnalysisContext.get.catalogAndNamespace :+ nameParts.head
      } else if (SparkSession.active.sessionState.catalogManager.isCatalogRegistered(nameParts.head)) {
        nameParts
      } else {
        AnalysisContext.get.catalogAndNamespace.head +: nameParts
      }
    }

    /**
     * Resolves relations to `ResolvedTable` or `Resolved[Temp/Persistent]View`. This is
     * for resolving DDL and misc commands. Code is copied from Spark's Analyzer, but performs
     * a view lookup before performing a table lookup.
     */
    private def lookupTableOrView(
                                   identifier: Seq[String],
                                   viewOnly: Boolean = false): Option[LogicalPlan] = {
      lookupTempView(identifier).map { tempView =>
        ResolvedTempView(identifier.asIdentifier, tempView.tableMeta.schema)
      }.orElse {
        val multipartIdent = expandIdentifier(identifier)
        val catalogAndIdentifier = Spark3Util.catalogAndIdentifier(SparkSession.active, multipartIdent.asJava)
        if (null != catalogAndIdentifier) {
          lookupView(SparkSession.active.sessionState.catalogManager.currentCatalog,
            catalogAndIdentifier.identifier())
            .orElse(lookupTable(SparkSession.active.sessionState.catalogManager.currentCatalog,
              catalogAndIdentifier.identifier()))
        } else {
          None
        }
      }
    }

    private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty

    private def isReferredTempViewName(nameParts: Seq[String]): Boolean = {
      AnalysisContext.get.referredTempViewNames.exists { n =>
        (n.length == nameParts.length) && n.zip(nameParts).forall {
          case (a, b) => resolver(a, b)
        }
      }
    }

    private def lookupTempView(identifier: Seq[String]): Option[TemporaryViewRelation] = {
      // We are resolving a view and this name is not a temp view when that view was created. We
      // return None earlier here.
      if (isResolvingView && !isReferredTempViewName(identifier)) return None
      SparkSession.active.sessionState.catalogManager.v1SessionCatalog.getRawLocalOrGlobalTempView(identifier)
    }


    private def lookupView(catalog: CatalogPlugin, ident: Identifier): Option[LogicalPlan] =
      loadView(catalog, ident).map {
        case view if CatalogV2Util.isSessionCatalog(catalog) =>
          ResolvedPersistentView(catalog, ident, view.schema)
        case view =>
          ResolvedV2View(ViewHelper(catalog).asViewCatalog, ident, view)
      }

    private def lookupTable(catalog: CatalogPlugin, ident: Identifier): Option[LogicalPlan] =
      CatalogV2Util.loadTable(catalog, ident).map {
        case v1Table: V1Table if CatalogV2Util.isSessionCatalog(catalog) &&
          v1Table.v1Table.tableType == CatalogTableType.VIEW =>
          val v1Ident = v1Table.catalogTable.identifier
          val v2Ident = Identifier.of(v1Ident.database.toArray, v1Ident.identifier)
          analysis.ResolvedPersistentView(catalog, v2Ident, v1Table.catalogTable.schema)
        case table =>
          ResolvedTable.create(catalog.asTableCatalog, ident, table)
      }

    def loadView(catalog: CatalogPlugin, ident: Identifier): Option[V2View] = catalog match {
      case viewCatalog: ViewCatalog =>
        try {
          Option(viewCatalog.loadView(ident))
        } catch {
          case _: NoSuchViewException => None
        }
      case _ => None
    }
  }

  implicit class ViewHelper(plugin: CatalogPlugin) {
    def asViewCatalog: ViewCatalog = plugin match {
      case viewCatalog: ViewCatalog =>
        viewCatalog
      case _ =>
        throw QueryCompilationErrors.missingCatalogAbilityError(plugin, "views")
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
          Some(l), Some(p), Some(startIndex), Some(stopIndex), Some(sqlText), Some(objectType), Some(objectName)) =>
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