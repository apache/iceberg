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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.planning.RewrittenRowLevelCommand
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.NoStatsUnaryNode
import org.apache.spark.sql.catalyst.plans.logical.ReplaceIcebergData
import org.apache.spark.sql.catalyst.plans.logical.RowLevelCommand
import org.apache.spark.sql.catalyst.plans.logical.WriteIcebergDelta
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

object RowLevelCommandScanRelationPushDown extends Rule[LogicalPlan] with PredicateHelper {
  import ExtendedDataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    // use native Spark planning for delta-based plans
    // unlike other commands, these plans have filters that can be pushed down directly
    case RewrittenRowLevelCommand(
          command,
          _: DataSourceV2Relation,
          rewritePlan: WriteIcebergDelta) =>
      val newRewritePlan = V2ScanRelationPushDown.apply(rewritePlan)
      command.withNewRewritePlan(newRewritePlan)

    // group-based MERGE operations are rewritten as joins and may be planned in a special way
    // the join condition is the MERGE condition and can be pushed into the source
    // this allows us to remove completely pushed down predicates from the join condition
    case UnplannedGroupBasedMergeOperation(
          command,
          rd: ReplaceIcebergData,
          join @ Join(_, _, _, Some(joinCond), _),
          relation: DataSourceV2Relation) =>

      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val (pushedFilters, newJoinCond) = pushMergeFilters(joinCond, relation, scanBuilder)
      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.left.get.mkString(", ")
      } else {
        pushedFilters.right.get.mkString(", ")
      }

      val (scan, output) = PushDownUtils.pruneColumns(scanBuilder, relation, relation.output, Nil)

      logInfo(s"""
           |Pushing MERGE operators to ${relation.name}
           |Pushed filters: $pushedFiltersStr
           |Original JOIN condition: $joinCond
           |New JOIN condition: $newJoinCond
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      val newRewritePlan = rd transformDown {
        case j: Join if j eq join =>
          j.copy(condition = newJoinCond)
        case r: DataSourceV2Relation if r.table eq table =>
          DataSourceV2ScanRelation(r, scan, PushDownUtils.toOutputAttrs(scan.readSchema(), r))
      }

      command.withNewRewritePlan(newRewritePlan)

    // push down the filter from the command condition instead of the filter in the rewrite plan,
    // which may be negated for copy-on-write DELETE and UPDATE operations
    case RewrittenRowLevelCommand(command, relation: DataSourceV2Relation, rewritePlan) =>
      val table = relation.table.asRowLevelOperationTable
      val scanBuilder = table.newScanBuilder(relation.options)

      val (pushedFilters, remainingFilters) = command.condition match {
        case Some(cond) => pushFilters(cond, scanBuilder, relation.output)
        case None => (Left(Nil), Nil)
      }

      val pushedFiltersStr = if (pushedFilters.isLeft) {
        pushedFilters.left.get.mkString(", ")
      } else {
        pushedFilters.right.get.mkString(", ")
      }

      val (scan, output) = PushDownUtils.pruneColumns(scanBuilder, relation, relation.output, Nil)

      logInfo(s"""
           |Pushing operators to ${relation.name}
           |Pushed filters: $pushedFiltersStr
           |Filters that were not pushed: ${remainingFilters.mkString(",")}
           |Output: ${output.mkString(", ")}
         """.stripMargin)

      // replace DataSourceV2Relation with DataSourceV2ScanRelation for the row operation table
      // there may be multiple read relations for UPDATEs that rely on the UNION approach
      val newRewritePlan = rewritePlan transform {
        case r: DataSourceV2Relation if r.table eq table =>
          DataSourceV2ScanRelation(r, scan, toOutputAttrs(scan.readSchema(), r))
      }

      command.withNewRewritePlan(newRewritePlan)
  }

  private def pushFilters(
      cond: Expression,
      scanBuilder: ScanBuilder,
      tableAttrs: Seq[AttributeReference])
      : (Either[Seq[Filter], Seq[Predicate]], Seq[Expression]) = {

    val tableAttrSet = AttributeSet(tableAttrs)
    val filters = splitConjunctivePredicates(cond).filter(_.references.subsetOf(tableAttrSet))
    val normalizedFilters = DataSourceStrategy.normalizeExprs(filters, tableAttrs)
    val (_, normalizedFiltersWithoutSubquery) =
      normalizedFilters.partition(SubqueryExpression.hasSubquery)

    PushDownUtils.pushFilters(scanBuilder, normalizedFiltersWithoutSubquery)
  }

  // splits the join condition into predicates and tries to push down each predicate into the scan
  // completely pushed down predicates are removed from the join condition
  // joinCond can't have subqueries as it is validated by the rule that rewrites MERGE as a join
  private def pushMergeFilters(
      joinCond: Expression,
      relation: DataSourceV2Relation,
      scanBuilder: ScanBuilder): (Either[Seq[Filter], Seq[Predicate]], Option[Expression]) = {

    val (tableFilters, commonFilters) =
      splitConjunctivePredicates(joinCond).partition(_.references.subsetOf(relation.outputSet))
    val normalizedTableFilters = DataSourceStrategy.normalizeExprs(tableFilters, relation.output)
    val (pushedFilters, postScanFilters) =
      PushDownUtils.pushFilters(scanBuilder, normalizedTableFilters)
    val newJoinCond = (commonFilters ++ postScanFilters).reduceLeftOption(And)

    (pushedFilters, newJoinCond)
  }

  private def toOutputAttrs(
      schema: StructType,
      relation: DataSourceV2Relation): Seq[AttributeReference] = {
    val nameToAttr = relation.output.map(_.name).zip(relation.output).toMap
    val cleaned = CharVarcharUtils.replaceCharVarcharWithStringInSchema(schema)
    cleaned.toAttributes.map {
      // keep the attribute id during transformation
      a => a.withExprId(nameToAttr(a.name).exprId)
    }
  }
}

object UnplannedGroupBasedMergeOperation {
  type ReturnType = (RowLevelCommand, ReplaceIcebergData, Join, DataSourceV2Relation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case m @ MergeIntoIcebergTable(_, _, _, _, _, Some(rewritePlan)) =>
      rewritePlan match {
        case rd @ ReplaceIcebergData(DataSourceV2Relation(table, _, _, _, _), query, _, _) =>
          val joinsAndRelations = query.collect {
            case j @ Join(
                  NoStatsUnaryNode(
                    ScanOperation(_, pushDownFilters, pushUpFilters, r: DataSourceV2Relation)),
                  _,
                  _,
                  _,
                  _) if pushUpFilters.isEmpty && pushDownFilters.isEmpty && r.table.eq(table) =>
              j -> r
          }

          joinsAndRelations match {
            case Seq((join, relation)) =>
              Some(m, rd, join, relation)
            case _ =>
              None
          }

        case _ =>
          None
      }

    case _ =>
      None
  }
}
