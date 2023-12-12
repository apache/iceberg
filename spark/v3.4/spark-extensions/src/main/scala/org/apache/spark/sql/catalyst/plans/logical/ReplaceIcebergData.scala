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

package org.apache.spark.sql.catalyst.plans.logical

import io.openlineage.client.OpenLineage
import io.openlineage.client.OpenLineage.DatasetFacetsBuilder
import io.openlineage.spark.builtin.scala.v1.ColumnLevelLineageNode
import io.openlineage.spark.builtin.scala.v1.DatasetFieldLineage
import io.openlineage.spark.builtin.scala.v1.ExpressionDependencyWithIdentifier
import io.openlineage.spark.builtin.scala.v1.InputDatasetFieldFromDelegate
import io.openlineage.spark.builtin.scala.v1.OlExprId
import io.openlineage.spark.builtin.scala.v1.OpenLineageContext
import io.openlineage.spark.builtin.scala.v1.OutputDatasetField
import io.openlineage.spark.builtin.scala.v1.OutputDatasetWithDelegate
import io.openlineage.spark.builtin.scala.v1.OutputDatasetWithFacets
import io.openlineage.spark.builtin.scala.v1.OutputLineageNode
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.analysis.NamedRelation
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.DataType

/**
 * Replace data in an existing table.
 */
case class ReplaceIcebergData(
    table: NamedRelation,
    query: LogicalPlan,
    originalTable: NamedRelation,
    write: Option[Write] = None)
  extends V2WriteCommandLike
    with OutputLineageNode
    with ColumnLevelLineageNode {

  override lazy val references: AttributeSet = query.outputSet
  override lazy val stringArgs: Iterator[Any] = Iterator(table, query, write)

  // the incoming query may include metadata columns
  lazy val dataInput: Seq[Attribute] = {
    val tableAttrNames = table.output.map(_.name)
    query.output.filter(attr => tableAttrNames.exists(conf.resolver(_, attr.name)))
  }

  override def outputResolved: Boolean = {
    assert(table.resolved && query.resolved,
      "`outputResolved` can only be called when `table` and `query` are both resolved.")

    // take into account only incoming data columns and ignore metadata columns in the query
    // they will be discarded after the logical write is built in the optimizer
    // metadata columns may be needed to request a correct distribution or ordering
    // but are not passed back to the data source during writes

    table.skipSchemaResolution || (dataInput.size == table.output.size &&
      dataInput.zip(table.output).forall { case (inAttr, outAttr) =>
        val outType = CharVarcharUtils.getRawType(outAttr.metadata).getOrElse(outAttr.dataType)
        // names and types must match, nullability must be compatible
        inAttr.name == outAttr.name &&
          DataType.equalsIgnoreCompatibleNullability(inAttr.dataType, outType) &&
          (outAttr.nullable || !inAttr.nullable)
      })
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): ReplaceIcebergData = {
    copy(query = newChild)
  }

  override def getOutputs(context: OpenLineageContext): List[OutputDatasetWithFacets] = {
    if (!table.isInstanceOf[DataSourceV2Relation]) {
      List()
    } else {
      val relation = table.asInstanceOf[DataSourceV2Relation]
      val datasetFacetsBuilder: DatasetFacetsBuilder = {
        new OpenLineage.DatasetFacetsBuilder()
          .lifecycleStateChange(
          context
            .openLineage
            .newLifecycleStateChangeDatasetFacet(
              OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
              null
            )
        )
      }
      DatasetVersionUtils.getVersionOf(relation) match {
        case Some(version) => datasetFacetsBuilder.version(
          context
            .openLineage
            .newDatasetVersionDatasetFacet(version)
        )
        case None =>
      }

      List(
        OutputDatasetWithDelegate(
          relation,
          datasetFacetsBuilder,
          new OpenLineage.OutputDatasetOutputFacetsBuilder()
        )
      )
    }
  }

  override def columnLevelLineageDependencies(context: OpenLineageContext): List[ExpressionDependencyWithIdentifier] = {
    Option
      .apply(query)
      .filter(query => query.isInstanceOf[Project])
      .toStream
      .flatMap(query =>
        (query.asInstanceOf[Project].projectList zip table.asInstanceOf[LogicalPlan].output).map {
          case (input, output) => ExpressionDependencyWithIdentifier(
            OlExprId(output.exprId.id), List(OlExprId(input.exprId.id))
          )
        }
      ).toList
  }

  override def columnLevelLineageInputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
    List(
      InputDatasetFieldFromDelegate(child),
      InputDatasetFieldFromDelegate(table),
    )
  }

  override def columnLevelLineageOutputs(context: OpenLineageContext): List[DatasetFieldLineage] = {
    table
        .output
        .map(a => OutputDatasetField(a.name, OlExprId(a.exprId.id)))
        .toList
  }
}

object DatasetVersionUtils {
  def getVersionOf(relation: DataSourceV2Relation): Option[String] = {
    if (relation.identifier.isEmpty) {
      return Option.empty
    }
    val identifier = relation.identifier.get

    if (relation.catalog.isEmpty || !relation.catalog.get.isInstanceOf[TableCatalog]) {
      return Option.empty
    }
    val tableCatalog = relation.catalog.get.asInstanceOf[TableCatalog]

    try{
      val table = tableCatalog.loadTable(identifier).asInstanceOf[SparkTable]

      Option.apply(table)
        .flatMap(table => Option.apply(table.table()))
        .flatMap(table => Option.apply(table.currentSnapshot()))
        .map(snapshot => snapshot.snapshotId().toString)
    }
    catch {
      case e: NoSuchTableException => Option.empty
    }
  }
}
