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

package org.apache.iceberg.spark.extensions

import org.apache.spark.sql.Column
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Assignment
import org.apache.spark.sql.catalyst.plans.logical.DeleteAction
import org.apache.spark.sql.catalyst.plans.logical.InsertAction
import org.apache.spark.sql.catalyst.plans.logical.InsertStarAction
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.MergeAction
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoContext
import org.apache.spark.sql.catalyst.plans.logical.UnresolvedMergeIntoIcebergTable
import org.apache.spark.sql.catalyst.plans.logical.UpdateAction
import org.apache.spark.sql.catalyst.plans.logical.UpdateStarAction
import org.apache.spark.sql.functions.expr
import scala.collection.JavaConverters


object IcebergMergeInto {
  /**
   * Initialize an [[IcebergMergeIntoBuilder]]. A Builder to specify an IcebergMergeInto action.
   * It could be possible to provide any number of `whenMatched` and `whenNotMatched` actions.
   *
   * Scala Examples:
   * {{{
   *   val ds = ...
   *   IcebergMergeInto
   *      .table("icebergTable")
   *      .using(ds.as("source")
   *      .when("source.id = icebergTable.id")
   *      .whenMatched("source.op = U")
   *      .updateAll()
   *      .whenMatched("source.op = D)
   *      .delete()
   *      .whenNotMatched()
   *      .insertAll()
   * }}}
   *
   * JavaExamples:
   * {{{
   *   Dataset<Row> ds = ...
   *   IcebergMergeInto
   *      .table("icebergTable")
   *      .using(ds.as("source")
   *      .when("source.id = icebergTable.id")
   *      .whenMatched("source.op = U")
   *      .updateAll()
   *      .whenMatched("source.op = D)
   *      .delete()
   *      .whenNotMatched()
   *      .insertAll()
   * }}}
   *
   * @param table : Target table name of the merge action
   * @return [[IcebergMergeIntoBuilder]]
   */
  def table(table: String): IcebergMergeIntoBuilder =
    new IcebergMergeIntoBuilder(table, None, None, Seq.empty[MergeAction], Seq.empty[MergeAction])

  /**
   * Builder class to properly create an IcebergMergeInto action.
   *
   * @param targetTable           : Target table name for the merge action
   * @param source                : Source [[Dataset]] for the merge action
   * @param onCondition           : `on` condition of the merge action
   * @param whenMatchedActions    : A list of [[MergeAction]] applied when a certain condition is matched
   * @param whenNotMatchedActions : A list of  [[MergeAction]] applied when a certain condition is not matched
   */
  class IcebergMergeIntoBuilder(
                                 private val targetTable: String,
                                 private val source: Option[Dataset[Row]],
                                 private val onCondition: Option[Expression],
                                 private val whenMatchedActions: Seq[MergeAction],
                                 private val whenNotMatchedActions: Seq[MergeAction]) {

    /**
     * Set the source dataset used during merge actions.
     *
     * @param source : Dataset[Row]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def using(source: Dataset[Row]): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(
        targetTable,
        Some(source),
        onCondition,
        whenMatchedActions,
        whenNotMatchedActions
      )

    /**
     * Set the `on` condition of the merge action from a [[Column]]
     *
     * @param condition : [[Column]] expression
     * @return [[IcebergMergeIntoBuilder]]
     */
    def on(condition: Column): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(targetTable, source, Some(condition.expr), whenMatchedActions, whenNotMatchedActions)

    /**
     * Set the `on` condition of the merge action from a [[String]] expression.
     *
     * @param condition : String
     * @return [[IcebergMergeIntoBuilder]]
     */
    def on(condition: String): IcebergMergeIntoBuilder =
      on(expr(condition))

    /**
     * Set a `whenMatched` action.
     *
     * @return [[IcebergMergeWhenMatchedBuilder]]
     */
    def whenMatched(): IcebergMergeWhenMatchedBuilder =
      new IcebergMergeWhenMatchedBuilder(this, None)

    /**
     * Set a conditionally `whenMatched` action.
     *
     * @param condition : [[Column]]
     * @return IcebergMergeWhenMatchedBuilder
     */
    def whenMatched(condition: Column): IcebergMergeWhenMatchedBuilder =
      new IcebergMergeWhenMatchedBuilder(this, Some(condition.expr))

    /**
     * Set a conditionally `whenMatched` action.
     *
     * @param condition : [[String]]
     * @return [[IcebergMergeWhenMatchedBuilder]]
     */
    def whenMatched(condition: String): IcebergMergeWhenMatchedBuilder =
      whenMatched(expr(condition))

    /**
     * Set a `whenNotMatched` action.
     *
     * @return [[IcebergMergeWhenNotMatchedBuilder]]
     */
    def whenNotMatched(): IcebergMergeWhenNotMatchedBuilder =
      new IcebergMergeWhenNotMatchedBuilder(this, None)

    /**
     * Set a conditionally `whenNotMatched` action
     *
     * @param condition : [[Column]]
     * @return [[IcebergMergeWhenNotMatchedBuilder]]
     */
    def whenNotMatched(condition: Column): IcebergMergeWhenNotMatchedBuilder =
      new IcebergMergeWhenNotMatchedBuilder(this, Some(condition.expr))

    /**
     * Set a conditionally `whenNotMatched` action
     *
     * @param condition : [[Column]]
     * @return [[IcebergMergeWhenNotMatchedBuilder]]
     */
    def whenNotMatched(condition: String): IcebergMergeWhenNotMatchedBuilder =
      whenNotMatched(expr(condition))

    /**
     * Internal method to append a NotMatchedExpressions
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    private[iceberg] def withNotMatchedAction(mergeAction: MergeAction): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(
        this.targetTable,
        this.source,
        this.onCondition,
        this.whenMatchedActions,
        this.whenNotMatchedActions :+ mergeAction
      )

    /**
     * Internal method to append a MatchedExpressions
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    private[iceberg] def withMatchedAction(mergeAction: MergeAction): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(
        this.targetTable,
        this.source,
        this.onCondition,
        this.whenMatchedActions :+ mergeAction,
        this.whenNotMatchedActions
      )

    /**
     * Execute the merge action
     */
    def merge(): Unit = {
      val mergeSourceDs = source
        .getOrElse(throw new IllegalArgumentException("Merge statement require source Dataset"))

      val mergeWhenCondition = onCondition
        .getOrElse(throw new IllegalArgumentException("Merge statement require whenCondition"))

      val sparkSession = mergeSourceDs.sparkSession
      val mergePlan = UnresolvedMergeIntoIcebergTable(
        UnresolvedRelation(sparkSession.sessionState.sqlParser.parseMultipartIdentifier(targetTable)),
        sparkSession.sessionState.analyzer.ResolveRelations(mergeSourceDs.queryExecution.logical),
        MergeIntoContext(mergeWhenCondition, whenMatchedActions, whenNotMatchedActions)
      )
      runCommand(sparkSession, mergePlan)
    }

    private def runCommand(sparkSession: SparkSession, plan: LogicalPlan): Unit = {
      val qe = sparkSession.sessionState.executePlan(plan)
      qe.assertCommandExecuted()
    }
  }

  /**
   * Builder to specify IcebergMergeWhenMatched actions
   *
   */
  class IcebergMergeWhenMatchedBuilder(
                                        private val icebergMergeIntoBuilder: IcebergMergeIntoBuilder,
                                        private val condition: Option[Expression]) {

    /**
     * Set an updateAll action.
     * It will update the target records with all the column on the source dataset.
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def updateAll(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(
        UpdateStarAction(condition)
      )
    }

    /**
     * Set an update action with a map of references between source and target table.
     *
     * It will update the target records accordingly with the assignment map provided as input.
     *
     * @param set : Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def update(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      updateAction(set)
    }

    /**
     * Set an update action with a map of references between source and target table.
     *
     * It will update the target records accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def update(set: java.util.Map[String, Column]): IcebergMergeIntoBuilder = {
      updateAction(JavaConverters.mapAsScalaMap(set).toMap)
    }

    /**
     * Set an update action with a map of references between source and target table.
     *
     * It will update the target records accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def updateExpr(set: Map[String, String]): IcebergMergeIntoBuilder = {
      updateAction(set.mapValues(expr).toMap)
    }

    /**
     * Set an update action with a map of references between source and target table.
     *
     * It will update the target records accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def updateExpr(set: java.util.Map[String, String]): IcebergMergeIntoBuilder = {
      updateAction(JavaConverters.mapAsScalaMap(set).mapValues(expr).toMap)
    }

    /**
     * Set a delete action.
     * It will delete the target
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def delete(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(
        DeleteAction(condition)
      )
    }

    private def updateAction(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(
        UpdateAction(
          condition,
          set.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
    }
  }

  /**
   * Builder to specify IcebergMergeWhenNotMatched actions
   */
  class IcebergMergeWhenNotMatchedBuilder(
                                           private val icebergMergeIntoBuilder: IcebergMergeIntoBuilder,
                                           private val condition: Option[Expression]
                                         ) {

    /**
     * Set an insert action.
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insertAll(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withNotMatchedAction(
        InsertStarAction(condition)
      )
    }

    /**
     * Set an insert action with a map of String Expression references.
     *
     * It will insert a new record on the the target table accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insert(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      insertAction(set)
    }

    /**
     * Set an insert action with a map of String Expression references.
     *
     * It will insert a new record on the the target table accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insert(set: java.util.Map[String, Column]): IcebergMergeIntoBuilder = {
      insertAction(JavaConverters.mapAsScalaMap(set).toMap)
    }

    /**
     * Set an insert action with a map of String Column references.
     *
     * It will insert a new record on the the target table accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insertExpr(set: Map[String, String]): IcebergMergeIntoBuilder = {
      insertAction(set.mapValues(expr).toMap)
    }

    /**
     * Set an insert action with a map of String Expression references.
     *
     * It will insert a new record on the the target table accordingly with the assignment map provided as input.
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insertExpr(set: java.util.Map[String, String]): IcebergMergeIntoBuilder = {
      insertAction(JavaConverters.mapAsScalaMap(set).mapValues(expr).toMap)
    }

    private def insertAction(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withNotMatchedAction(
        InsertAction(
          condition,
          set.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
    }

  }
}
