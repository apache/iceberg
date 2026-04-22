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
package org.apache.spark.sql.catalyst.analysis

import java.security.SecureRandom
import org.apache.iceberg.restrictions.Action
import org.apache.iceberg.restrictions.Actions
import org.apache.iceberg.restrictions.ReadRestrictions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergRestricted
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergRowFilterExpr
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergToSparkExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import scala.jdk.CollectionConverters._

/**
 * Enforce server-provided [[ReadRestrictions]] by rewriting the logical plan.
 *
 * For each [[DataSourceV2Relation]] whose table is a [[SparkTable]] carrying non-empty
 * restrictions, the rule rewrites the relation node into `Filter(rowFilter, Project(masks,
 * Relation))`. Masked column outputs preserve the original `ExprId` so downstream references
 * still resolve.
 *
 * Masking functions are bound via [[Actions.bind]] which returns engine-agnostic
 * [[org.apache.iceberg.util.SerializableFunction]]s. The Spark-side
 * [[IcebergRestricted]] expression handles type bridging (UTF8String, ByteBuffer, etc.).
 */
case class ApplyReadRestrictions(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case r @ DataSourceV2Relation(table: SparkTable, _, _, _, _, _)
        if table.readRestrictions.isPresent =>
      rewrite(r, table.readRestrictions.get)
  }

  private def rewrite(
      relation: DataSourceV2Relation,
      restrictions: ReadRestrictions): LogicalPlan = {
    val table = relation.table.asInstanceOf[SparkTable]
    val icebergSchema = table.table().schema()

    val actionByFieldId: Map[Int, Action] =
      restrictions.columnProjections.asScala.iterator.map(a => a.fieldId -> a).toMap

    // The spec permits actions on any fieldId including nested fields, but this
    // rule currently only rewrites top-level columns. Fail closed on nested or
    // unknown fieldIds so masks are never silently bypassed; lift this when nested
    // projection through struct paths is implemented.
    val topLevelFieldIds: Set[Int] =
      icebergSchema.asStruct.fields.asScala.iterator.map(_.fieldId).toSet
    actionByFieldId.keys.foreach { fid =>
      if (!topLevelFieldIds.contains(fid)) {
        val path = icebergSchema.findColumnName(fid)
        throw new IllegalStateException(
          if (path == null) s"ReadRestrictions references unknown fieldId $fid"
          else
            s"ReadRestrictions on nested fields are not yet supported " +
              s"(fieldId=$fid, path='$path')")
      }
    }

    val projectList: Seq[NamedExpression] =
      if (actionByFieldId.isEmpty) {
        relation.output
      } else {
        relation.output.map { attr =>
          val icebergField = icebergSchema.findField(attr.name)
          if (icebergField == null) {
            attr
          } else {
            actionByFieldId.get(icebergField.fieldId) match {
              case Some(action) =>
                val icebergType = icebergField.`type`()
                val salt = action match {
                  case _: Action.Sha256QueryLocal => ApplyReadRestrictions.generateSalt()
                  case _ => null
                }
                val boundFn = Actions.bind(action, icebergType, salt)
                val masked = IcebergRestricted(attr, boundFn)
                Alias(masked, attr.name)(exprId = attr.exprId, qualifier = attr.qualifier)
              case None => attr
            }
          }
        }
      }

    val filtered: LogicalPlan =
      if (restrictions.rowFilter == null) {
        relation
      } else {
        val catalystFilter =
          IcebergRowFilterExpr(
            IcebergToSparkExpression.convert(restrictions.rowFilter, relation.output))
        Filter(catalystFilter, relation)
      }

    if (actionByFieldId.isEmpty) filtered else Project(projectList, filtered)
  }
}

object ApplyReadRestrictions {
  private val RANDOM = new SecureRandom()
  private val SALT_LENGTH = 16

  def generateSalt(): Array[Byte] = {
    val salt = new Array[Byte](SALT_LENGTH)
    RANDOM.nextBytes(salt)
    salt
  }
}
