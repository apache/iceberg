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
   *
   * It could be possible to provide any number of `whenMatched` and `whenNotMatched` actions.
   *
   * Scala Examples:
   * {{{
   *   val ds = ...
   *   IcebergMergeInto
   *      .table("icebergTable")
   *      .using(ds.as("source"))
   *      .on("source.id = icebergTable.id")
   *      .whenMatched("source.op = U")
   *      .updateAll()
   *      .whenMatched("source.op = D)
   *      .delete()
   *      .whenNotMatched()
   *      .insertAll()
   *      .merge()
   * }}}
   *
   * JavaExamples:
   * {{{
   *   Dataset<Row> ds = ...
   *   IcebergMergeInto
   *      .table("icebergTable")
   *      .using(ds.as("source"))
   *      .on("source.id = icebergTable.id")
   *      .whenMatched("source.op = U")
   *      .updateAll()
   *      .whenMatched("source.op = D)
   *      .delete()
   *      .whenNotMatched()
   *      .insertAll()
   *      .merge();
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
     * @param source : [[Dataset]] of [[Row]]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def using(source: Dataset[Row]): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(
        targetTable,
        Some(source),
        onCondition,
        whenMatchedActions,
        whenNotMatchedActions)

    /**
     * Set the `on` condition of the merge action from a [[Column]] expression.
     *
     * Examples:
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on(col("source.id).equalTo(col("icebergTable.id"))
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on(col("source.id).equalTo(col("icebergTable.id"))
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * @param condition : [[Column]] expression
     * @return [[IcebergMergeIntoBuilder]]
     */
    def on(condition: Column): IcebergMergeIntoBuilder =
      new IcebergMergeIntoBuilder(
        targetTable,
        source,
        Some(condition.expr),
        whenMatchedActions,
        whenNotMatchedActions)

    /**
     * Set the `on` condition of the merge action from a [[String]] expression.
     *
     * Examples:
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge();
     * }}}
     *
     * @param condition : String
     * @return [[IcebergMergeIntoBuilder]]
     */
    def on(condition: String): IcebergMergeIntoBuilder =
      on(expr(condition))

    /**
     * Initialize a `whenMatched` builder without any condition.
     *
     * This `whenMatched` action will be executed if and only if the `on` condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * updateAll: update all the target table fields with source dataset fields
     * * updateSet(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment
     * * updateExpr(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment expression.
     * * delete: delete all the target table records
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge();
     * }}}
     *
     * @return [[IcebergMergeWhenMatchedBuilder]]
     */
    def whenMatched(): IcebergMergeWhenMatchedBuilder =
      new IcebergMergeWhenMatchedBuilder(this, None)

    /**
     * Initialize a `whenMatched` builder with a specific  [[Column]] expression condition.
     *
     * This `whenMatched` action will be executed if and only if the `on` condition AND the `whenMatched`
     * condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * updateAll: update all the target table fields with source dataset fields
     * * updateSet(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment
     * * updateExpr(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment expression.
     * * delete: delete all the target table records
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .delete()
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .delete()
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .merge();
     * }}}
     *
     * @param condition : [[Column]] based condition expression.
     *                  It should be used to discriminate merge action based on some advanced condition
     * @return [[IcebergMergeWhenMatchedBuilder]]
     */
    def whenMatched(condition: Column): IcebergMergeWhenMatchedBuilder =
      new IcebergMergeWhenMatchedBuilder(this, Some(condition.expr))

    /**
     * Initialize a `whenMatched` builder with a specific  [[String]] expression condition.
     *
     * This `whenMatched` action will be executed if and only if the `on` condition AND the `whenMatched`
     * condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * updateAll: update all the target table fields with source dataset fields
     * * updateSet(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment
     * * updateExpr(Map): update all the target table records while change only a subset of fields
     * based on the provided assignment expression.
     * * delete: delete all the target table records
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .delete()
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .delete()
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .merge();
     * }}}
     *
     * @param condition : [[String]] based condition expression.
     *                  It should be used to discriminate merge action based on some advanced condition
     * @return [[IcebergMergeWhenMatchedBuilder]]
     */
    def whenMatched(condition: String): IcebergMergeWhenMatchedBuilder =
      whenMatched(expr(condition))

    /**
     * Initialize a `whenNotMatched` builder without any condition.
     *
     * This `whenNotMatched` action will be executed if and only if the `on` condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * insertAll: update all the target table with source dataset records
     * * insert(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment
     * * insertExpr(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment expression.
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .updateAll()
     *      .whenNotMatched()
     *      .insertAll()
     *      .merge();
     * }}}
     *
     * @return [[IcebergMergeWhenNotMatchedBuilder]]
     */
    def whenNotMatched(): IcebergMergeWhenNotMatchedBuilder =
      new IcebergMergeWhenNotMatchedBuilder(this, None)

    /**
     * Initialize a `whenNotMatched` builder with a [[Column]] expression condition
     *
     * This `whenNotMatched` action will be executed if and only if the `on` condition AND the provided
     * condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * insertAll: update all the target table with source dataset records
     * * insert(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment
     * * insertExpr(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment expression.
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .whenNotMatched(col("source.timestamp").gt("icebergTable.timestamp"))
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .updateAll()
     *      .whenNotMatched(col("source.timestamp").gt("icebergTable.timestamp"))
     *      .insertAll()
     *      .merge();
     * }}}
     *
     * @param condition : [[Column]] based condition expression.
     *                  It should be used to discriminate merge action based on some advanced condition
     * @return [[IcebergMergeWhenNotMatchedBuilder]]
     */
    def whenNotMatched(condition: Column): IcebergMergeWhenNotMatchedBuilder =
      new IcebergMergeWhenNotMatchedBuilder(this, Some(condition.expr))

    /**
     * Initialize a `whenNotMatched` builder with a [[String]] expression condition
     *
     * This `whenNotMatched` action will be executed if and only if the `on` condition AND the provided
     * condition are satisfied.
     *
     * It could be possible to configure one of the following merge actions:
     * * insertAll: update all the target table with source dataset records
     * * insert(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment
     * * insertExpr(Map): insert all the target table records while set value only to a subset of fields
     * based on the provided assignment expression.
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'U')
     *      .updateAll()
     *      .whenNotMatched("source.timestamp > icebergTable.timestamp")
     *      .insertAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched("source.op" == 'D')
     *      .updateAll()
     *      .whenNotMatched("source.timestamp > icebergTable.timestamp")
     *      .insertAll()
     *      .merge();
     * }}}
     *
     * @param condition : [[String]] based condition expression.
     *                  It should be used to discriminate merge action based on some advanced condition
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
        this.whenNotMatchedActions :+ mergeAction)

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
        this.whenNotMatchedActions)

    /**
     * Execute the merge action
     */
    def merge(): Unit = {
      val mergeSourceDs = source
        .getOrElse(throw new IllegalArgumentException("Merge statement require source Dataset"))

      val mergeWhenCondition = onCondition
        .getOrElse(throw new IllegalArgumentException("Merge statement require whenCondition"))

      val sparkSession = mergeSourceDs.sparkSession
      val sparkSqlParser = sparkSession.sessionState.sqlParser
      val sparkAnalyzer = sparkSession.sessionState.analyzer

      val mergePlan = UnresolvedMergeIntoIcebergTable(
        UnresolvedRelation(sparkSqlParser.parseMultipartIdentifier(targetTable)),
        sparkAnalyzer.ResolveRelations(mergeSourceDs.queryExecution.logical),
        MergeIntoContext(mergeWhenCondition, whenMatchedActions, whenNotMatchedActions))
      runCommand(sparkSession, mergePlan)
    }

    private def runCommand(sparkSession: SparkSession, plan: LogicalPlan): Unit = {
      val qe = sparkSession.sessionState.executePlan(plan)
      qe.assertCommandExecuted()
    }
  }

  /**
   * Builder to specifiy the IcebergMergeWhenMatched actions
   */
  class IcebergMergeWhenMatchedBuilder(
      private val icebergMergeIntoBuilder: IcebergMergeIntoBuilder,
      private val condition: Option[Expression]) {

    /**
     * Set an updateAll action.
     * It will update the target records with all the column on the source dataset.
     * *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateAll()
     *      .merge();
     * }}}
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def updateAll(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(UpdateStarAction(condition))
    }

    /**
     * Set an update action with a map of references between source and target table.
     *
     * It will update the target records accordingly with the assignment map provided as input.
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .update(Map(
     *        "id" -> col("source.id"),
     *        "name" -> col("source.name"),
     *        "salary" -> lit(0)
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .update(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", col("source.id"));
     *            put("name", col("source.name"));
     *            put("salary", lit(0));
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
     *
     * @param set : Map[String,Column]
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .update(Map(
     *        "id" -> col("source.id"),
     *        "name" -> col("source.name"),
     *        "salary" -> lit(0)
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .update(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", col("source.id"));
     *            put("name", col("source.name"));
     *            put("salary", lit(0));
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateExpr(Map(
     *        "id" -> "source.id",
     *        "name" -> "source.name"
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateExpr(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", "source.id");
     *            put("name", "source.name");
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateExpr(Map(
     *        "id" -> "source.id",
     *        "name" -> "source.name"
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .updateExpr(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", "source.id");
     *            put("name", "source.name");
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def updateExpr(set: java.util.Map[String, String]): IcebergMergeIntoBuilder = {
      updateAction(JavaConverters.mapAsScalaMap(set).mapValues(expr).toMap)
    }

    /**
     * Set a delete action.
     * It will delete the target record
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def delete(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(DeleteAction(condition))
    }

    private def updateAction(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withMatchedAction(
        UpdateAction(condition, set.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
    }
  }

  /**
   * Builder to specify IcebergMergeWhenNotMatched actions
   */
  class IcebergMergeWhenNotMatchedBuilder(
      private val icebergMergeIntoBuilder: IcebergMergeIntoBuilder,
      private val condition: Option[Expression]) {

    /**
     * Set an insert action.
     *
     * It will insert all the records into the target table
     *
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insertAll(): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withNotMatchedAction(InsertStarAction(condition))
    }

    /**
     * Set an insert action with a map of String Expression references.
     *
     * It will insert a new record on the the target table accordingly with the assignment map provided as input. O
     * their values will be null
     *
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(Map(
     *        "id" -> "col(source.id)",
     *        "name" -> "col(source.name)",
     *        "salary" -> "lit(0)",
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", col("source.id"));
     *            put("name", col("source.name"));
     *            put("salary", lit(0));
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(Map(
     *        "id" -> "col(source.id)",
     *        "name" -> "col(source.name)",
     *        "salary" -> "lit(0)",
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", col("source.id"));
     *            put("name", col("source.name"));
     *            put("salary", lit(0));
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
     *
     * @param set : java.util.Map[String,Column]
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(Map(
     *        "id" -> "source.id",
     *        "name" -> "source.name",
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insertExpr(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", "source.id");
     *            put("name","source.name");
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
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
     * * Scala Examples:
     * {{{
     *   val ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insert(Map(
     *        "id" -> "source.id",
     *        "name" -> "source.name",
     *      ))
     *      .merge()
     * }}}
     *
     * JavaExamples:
     * {{{
     *   Dataset<Row> ds = ...
     *   IcebergMergeInto
     *      .table("icebergTable")
     *      .using(ds.as("source"))
     *      .on("source.id == icebergTable.id")
     *      .whenMatched()
     *      .insertExpr(
     *        new HashMap<String, Column>() {
     *          {
     *            put("id", "source.id");
     *            put("name","source.name");
     *          }
     *        }
     *      )
     *      .merge();
     * }}}
     *
     * @param set : java.util.Map[String,String]
     * @return [[IcebergMergeIntoBuilder]]
     */
    def insertExpr(set: java.util.Map[String, String]): IcebergMergeIntoBuilder = {
      insertAction(JavaConverters.mapAsScalaMap(set).mapValues(expr).toMap)
    }

    private def insertAction(set: Map[String, Column]): IcebergMergeIntoBuilder = {
      icebergMergeIntoBuilder.withNotMatchedAction(
        InsertAction(condition, set.map(x => Assignment(expr(x._1).expr, x._2.expr)).toSeq))
    }

  }
}
