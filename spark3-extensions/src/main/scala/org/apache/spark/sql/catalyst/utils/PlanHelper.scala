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
package org.apache.spark.sql.catalyst.utils

import java.util.UUID
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, DynamicFileFilter, LogicalPlan}
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.iceberg.read.SupportsFileFilter
import org.apache.spark.sql.connector.iceberg.write.MergeBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, LogicalWriteInfoImpl}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2ScanRelation}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

trait PlanHelper extends PredicateHelper {
  val FILE_NAME_COL = "_file"
  val ROW_POS_COL = "_pos"

  def buildScanPlan(table: Table,
                    output: Seq[AttributeReference],
                    mergeBuilder: MergeBuilder,
                    prunedTargetPlan: LogicalPlan): LogicalPlan = {

    val scanBuilder = mergeBuilder.asScanBuilder
    val scan = scanBuilder.build()
    val scanRelation = DataSourceV2ScanRelation(table, scan, toOutputAttrs(scan.readSchema(), output))

    scan match {
      case filterable: SupportsFileFilter =>
        val matchingFilePlan = buildFileFilterPlan(prunedTargetPlan)
        val dynamicFileFilter = DynamicFileFilter(scanRelation, matchingFilePlan, filterable)
        dynamicFileFilter
      case _ =>
        scanRelation
    }
  }

  private def buildFileFilterPlan(prunedTargetPlan: LogicalPlan): LogicalPlan = {
    val fileAttr = findOutputAttr(prunedTargetPlan, FILE_NAME_COL)
    Aggregate(Seq(fileAttr), Seq(fileAttr), prunedTargetPlan)
  }

  def findOutputAttr(plan: LogicalPlan, attrName: String): Attribute = {
    val resolver = SQLConf.get.resolver
    plan.output.find(attr => resolver(attr.name, attrName)).getOrElse {
      throw new AnalysisException(s"Cannot find $attrName in ${plan.output}")
    }
  }

  def newWriteInfo(schema: StructType): LogicalWriteInfo = {
    val uuid = UUID.randomUUID()
    LogicalWriteInfoImpl(queryId = uuid.toString, schema, CaseInsensitiveStringMap.empty)
  }

  private def toOutputAttrs(schema: StructType, output: Seq[AttributeReference]): Seq[AttributeReference] = {
    val nameToAttr = output.map(_.name).zip(output).toMap
    schema.toAttributes.map {
      a => nameToAttr.get(a.name) match {
        case Some(ref) =>
          // keep the attribute id if it was present in the relation
          a.withExprId(ref.exprId)
        case _ =>
          // if the field is new, create a new attribute
          AttributeReference(a.name, a.dataType, a.nullable, a.metadata)()
      }
    }
  }
}
