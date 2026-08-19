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
import org.apache.iceberg.functions.IcebergFunction
import org.apache.iceberg.functions.MaskAlphanum
import org.apache.iceberg.functions.ReplaceWithNull
import org.apache.iceberg.functions.SaltedFunction
import org.apache.iceberg.rest.restrictions.ReadRestrictions
import org.apache.iceberg.spark.functions.MaskAlphanumFunction
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.util.SerializableFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.ApplyFunctionExpression
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergRestricted
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergRowFilterExpr
import org.apache.spark.sql.catalyst.expressions.iceberg.IcebergToSparkExpression
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/**
 * Enforce server-provided [[ReadRestrictions]] by rewriting the logical plan.
 *
 * For each [[DataSourceV2Relation]] whose table is a [[SparkTable]] carrying non-empty
 * restrictions, the rule rewrites the relation node into `Project(masks, Filter(rowFilter,
 * Relation))` so the row filter sees the original column values before any mask is applied
 * (spec: "A reader evaluates the row filter against original, untransformed column values, then
 * applies required-column-projections to the surviving rows."). Each masked column is aliased to a
 * new `ExprId` and references above the relation are rewritten to it, so the masking `Project`
 * cannot be mistaken for a no-op and removed.
 *
 * Masking functions are bound via [[IcebergFunction#bind]] which returns engine-agnostic
 * [[org.apache.iceberg.util.SerializableFunction]]s. The Spark-side
 * [[IcebergRestricted]] expression handles type bridging (UTF8String, ByteBuffer, etc.).
 */
case class ApplyReadRestrictions(spark: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    // Generate the Sha256QueryLocal salt once per rule invocation. The tag guard
    // below ensures this rule fires at most once per DataSourceV2Relation, so the
    // salt is effectively per-query: subsequent fixed-point passes see the tag and
    // skip the already-rewritten relation.
    val querySalt = ApplyReadRestrictions.generateSalt()
    // resolveOperatorsUpWithNewOutput (rather than resolveOperators) because each masked column is
    // aliased to a *new* ExprId, and ancestor nodes still reference the relation's original ones.
    // Returning the old -> new mapping lets Catalyst rewrite those references. Reusing the original
    // ExprId instead would make the masking Project look like a no-op to the optimizer
    // (RemoveNoopOperators drops any Project whose output matches its child's), silently discarding
    // every mask.
    plan resolveOperatorsUpWithNewOutput {
      case r @ DataSourceV2Relation(table: SparkTable, _, _, _, _, _)
          if table.readRestrictions.isPresent
            && r.getTagValue(ApplyReadRestrictions.RESTRICTIONS_APPLIED).isEmpty =>
        r.setTagValue(ApplyReadRestrictions.RESTRICTIONS_APPLIED, ())
        rewrite(r, table.readRestrictions.get, querySalt)
    }
  }

  private def rewrite(
      relation: DataSourceV2Relation,
      restrictions: ReadRestrictions,
      querySalt: Array[Byte]): (LogicalPlan, Seq[(Attribute, Attribute)]) = {
    val table = relation.table.asInstanceOf[SparkTable]
    val icebergSchema = table.table().schema()

    val actionByFieldId: Map[Int, IcebergFunction[_, _]] =
      restrictions.columnProjections.asScala.iterator.map(a => a.fieldId -> a).toMap

    // The spec permits actions on any fieldId including nested fields, but this rule currently only
    // rewrites top-level columns. Fail closed on nested fieldIds so masks are never silently
    // bypassed; lift this when nested projection through struct paths is implemented.
    val topLevelFieldIds: Set[Int] =
      icebergSchema.asStruct.fields.asScala.iterator.map(_.fieldId).toSet
    actionByFieldId.keys.foreach { fieldId =>
      if (!topLevelFieldIds.contains(fieldId)) {
        val nestedPath = icebergSchema.findColumnName(fieldId)
        if (nestedPath != null) {
          throw new IllegalStateException(
            s"ReadRestrictions on nested fields are not yet supported " +
              s"(fieldId=$fieldId, path='$nestedPath')")
        }
        // The column is not in the schema being read. Per spec, projections referencing columns
        // that are not being read do not apply. Field ids are validated against the table's schemas
        // when the restrictions are attached to it (ReadRestrictions#validate), so reaching here
        // means the column was dropped or is otherwise not projected, not that it is unknown.
      }
    }

    val attrMapping = mutable.ArrayBuffer.empty[(Attribute, Attribute)]
    val maskedAttrs: Seq[NamedExpression] =
      relation.output.map { attr =>
        val icebergField = icebergSchema.findField(attr.name)
        if (icebergField == null) {
          attr
        } else {
          actionByFieldId.get(icebergField.fieldId) match {
            case Some(action) =>
              // ReplaceWithNull cannot check nullability itself because Type does not carry the
              // field's required/optional flag, so it is rejected here instead.
              if (action.isInstanceOf[ReplaceWithNull] && !icebergField.isOptional) {
                throw new IllegalStateException(
                  s"Cannot apply replace-with-null to required field: ${icebergField.name} " +
                    s"(fieldId=${icebergField.fieldId})")
              }
              val masked = buildMaskExpression(attr, action, icebergField.`type`(), querySalt)
              val alias = Alias(masked, attr.name)(qualifier = attr.qualifier)
              attrMapping += (attr -> alias.toAttribute)
              alias
            case None => attr
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

    // Every projection may have been skipped as not-being-read, which leaves nothing to mask.
    val rewritten = if (attrMapping.isEmpty) filtered else Project(maskedAttrs, filtered)
    (rewritten, attrMapping.toSeq)
  }

  /**
   * Build the masking expression for a single column. Prefers Spark's native
   * [[ApplyFunctionExpression]] backed by an Iceberg
   * [[org.apache.spark.sql.connector.catalog.functions.ScalarFunction]] for actions that have a
   * ScalarFunction implementation (gets whole-stage codegen for free); falls back to
   * [[IcebergRestricted]] for actions still using the hand-rolled expression path.
   */
  private def buildMaskExpression(
      attr: AttributeReference,
      action: IcebergFunction[_, _],
      icebergType: org.apache.iceberg.types.Type,
      querySalt: Array[Byte]): Expression = action match {
    case _: MaskAlphanum =>
      val unbound = new MaskAlphanumFunction()
      val bound = unbound.bind(StructType(Array(StructField(attr.name, attr.dataType))))
      ApplyFunctionExpression(
        bound.asInstanceOf[org.apache.spark.sql.connector.catalog.functions.ScalarFunction[_]],
        Seq(attr))
    case salted: SaltedFunction[_, _] =>
      val boundFn = salted
        .bind(icebergType, querySalt)
        .asInstanceOf[SerializableFunction[Object, Object]]
      IcebergRestricted(attr, boundFn)
    case _ =>
      val boundFn = action
        .bind(icebergType)
        .asInstanceOf[SerializableFunction[Object, Object]]
      IcebergRestricted(attr, boundFn)
  }
}

object ApplyReadRestrictions {
  private val RANDOM = new SecureRandom()
  private val SALT_LENGTH = 16
  private val RESTRICTIONS_APPLIED = new TreeNodeTag[Unit]("readRestrictionsApplied")

  def generateSalt(): Array[Byte] = {
    val salt = new Array[Byte](SALT_LENGTH)
    RANDOM.nextBytes(salt)
    salt
  }
}
