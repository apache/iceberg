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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.expressions.V2ExpressionUtils
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.RowDeltaUtils._
import org.apache.spark.sql.catalyst.util.WriteDeltaProjections
import org.apache.spark.sql.connector.write.RowLevelOperation
import org.apache.spark.sql.connector.write.SupportsDelta
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType

trait RewriteRowLevelIcebergCommand extends RewriteRowLevelCommand {

  // override as the existing Spark method does not work for UPDATE and MERGE
  protected override def buildWriteDeltaProjections(
      plan: LogicalPlan,
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): WriteDeltaProjections = {

    val rowProjection = if (rowAttrs.nonEmpty) {
      Some(newLazyProjection(plan, rowAttrs))
    } else {
      None
    }

    val rowIdProjection = newLazyProjection(plan, rowIdAttrs)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      Some(newLazyProjection(plan, metadataAttrs))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  // the projection is done by name, ignoring expr IDs
  private def newLazyProjection(
      plan: LogicalPlan,
      projectedAttrs: Seq[Attribute]): ProjectingInternalRow = {

    val projectedOrdinals = projectedAttrs.map(attr => plan.output.indexWhere(_.name == attr.name))
    val schema = StructType.fromAttributes(projectedOrdinals.map(plan.output(_)))
    ProjectingInternalRow(schema, projectedOrdinals)
  }

  protected def buildDeltaProjections(
      plan: LogicalPlan,
      outputs: Seq[Seq[Expression]],
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): WriteDeltaProjections = {

    val insertAndUpdateOutputs = outputs.filterNot(_.head == Literal(DELETE_OPERATION))
    val updateAndDeleteOutputs = outputs.filterNot(_.head == Literal(INSERT_OPERATION))

    val rowProjection = if (rowAttrs.nonEmpty) {
      Some(newLazyProjection(insertAndUpdateOutputs, plan.output, rowAttrs))
    } else {
      None
    }

    val rowIdProjection = newLazyProjection(updateAndDeleteOutputs, plan.output, rowIdAttrs)

    val metadataProjection = if (metadataAttrs.nonEmpty) {
      Some(newLazyProjection(updateAndDeleteOutputs, plan.output, metadataAttrs))
    } else {
      None
    }

    WriteDeltaProjections(rowProjection, rowIdProjection, metadataProjection)
  }

  // the projection is done by name, ignoring expr IDs
  private def newLazyProjection(
      outputs: Seq[Seq[Expression]],
      outputAttrs: Seq[Attribute],
      projectedAttrs: Seq[Attribute]): ProjectingInternalRow = {

    val projectedOrdinals = projectedAttrs.map(attr => outputAttrs.indexWhere(_.name == attr.name))

    val structFields = projectedAttrs.zip(projectedOrdinals).map { case (attr, ordinal) =>
      // output attr is nullable if at least one output projection may produce null for that attr
      // but row ID and metadata attrs are projected only for update/delete records and
      // row attrs are projected only in insert/update records
      // that's why the projection schema must rely only on relevant outputs
      // instead of blindly inheriting the output attr nullability
      val nullable = outputs.exists(output => output(ordinal).nullable)
      StructField(attr.name, attr.dataType, nullable, attr.metadata)
    }
    val schema = StructType(structFields)

    ProjectingInternalRow(schema, projectedOrdinals)
  }

  protected def deltaDeleteOutput(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute],
      metadataAttrs: Seq[Attribute]): Seq[Expression] = {
    val deleteRowValues = buildDeltaDeleteRowValues(rowAttrs, rowIdAttrs)
    Seq(Literal(DELETE_OPERATION)) ++ deleteRowValues ++ metadataAttrs
  }

  protected def deltaInsertOutput(
      rowValues: Seq[Expression],
      metadataAttrs: Seq[Attribute]): Seq[Expression] = {
    val metadataValues = metadataAttrs.map(attr => Literal(null, attr.dataType))
    Seq(Literal(INSERT_OPERATION)) ++ rowValues ++ metadataValues
  }

  private def buildDeltaDeleteRowValues(
      rowAttrs: Seq[Attribute],
      rowIdAttrs: Seq[Attribute]): Seq[Expression] = {

    // nullify all row attrs that are not part of the row ID
    val rowIdAttSet = AttributeSet(rowIdAttrs)
    rowAttrs.map {
      case attr if rowIdAttSet.contains(attr) => attr
      case attr => Literal(null, attr.dataType)
    }
  }

  protected def buildMergingOutput(
      outputs: Seq[Seq[Expression]],
      attrs: Seq[Attribute]): Seq[Attribute] = {

    // build a correct nullability map for output attributes
    // an attribute is nullable if at least one output may produce null
    val nullabilityMap = attrs.indices.map { index =>
      index -> outputs.exists(output => output(index).nullable)
    }.toMap

    attrs.zipWithIndex.map { case (attr, index) =>
      AttributeReference(attr.name, attr.dataType, nullabilityMap(index))()
    }
  }

  protected def resolveRowIdAttrs(
      relation: DataSourceV2Relation,
      operation: RowLevelOperation): Seq[AttributeReference] = {

    operation match {
      case supportsDelta: SupportsDelta =>
        val rowIdAttrs = V2ExpressionUtils.resolveRefs[AttributeReference](
          supportsDelta.rowId.toSeq,
          relation)

        val nullableRowIdAttrs = rowIdAttrs.filter(_.nullable)
        if (nullableRowIdAttrs.nonEmpty) {
          throw new AnalysisException(s"Row ID attrs cannot be nullable: $nullableRowIdAttrs")
        }

        rowIdAttrs

      case other =>
        throw new AnalysisException(s"Operation $other does not support deltas")
    }
  }
}
