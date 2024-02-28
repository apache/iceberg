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

import java.util.UUID
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg
import org.apache.iceberg.FileFormat
import org.apache.iceberg.PartitionSpec
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.relocated.com.google.common.base.Preconditions
import org.apache.iceberg.spark.MaterializedViewUtil
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.spark.SparkWriteOptions
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.ViewAlreadyExistsException
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.ViewCatalog
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

case class CreateMaterializedViewExec(
                                       catalog: ViewCatalog,
                                       ident: Identifier,
                                       queryText: String,
                                       viewSchema: StructType,
                                       columnAliases: Seq[String],
                                       columnComments: Seq[Option[String]],
                                       queryColumnNames: Seq[String],
                                       comment: Option[String],
                                       properties: Map[String, String],
                                       allowExisting: Boolean,
                                       replace: Boolean) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {

    val viewLocation = properties.get("location")
    Preconditions.checkArgument(viewLocation.isDefined)

    val storageTableLocation = viewLocation + "/storage/v1"

    // Create the storage table in the Hadoop catalog so it is explicitly registered in the Spark catalog
    val tables: HadoopTables = new HadoopTables(new Configuration())
    val icebergSchema = SparkSchemaUtil.convert(viewSchema)
    // TODO: Add support for partitioning the storage table
    val spec: PartitionSpec = PartitionSpec.builderFor(icebergSchema).build

    val table: iceberg.Table = tables.create(icebergSchema, spec, storageTableLocation)

    val baseTables = MaterializedViewUtil.extractBaseTables(queryText).asScala.toList
    val baseTableSnapshots = getBaseTableSnapshots(baseTables)
    val baseTableSnapshotsProperties = baseTableSnapshots.map{
      case (key, value) => (
        MaterializedViewUtil.MATERIALIZED_VIEW_BASE_SNAPSHOT_PROPERTY_KEY_PREFIX + key.toString
        ) -> value.toString
    }

    session.sql(queryText).write.format("iceberg").option(
        SparkWriteOptions.WRITE_FORMAT, FileFormat.PARQUET.toString
    ).mode(SaveMode.Append).save(storageTableLocation)

    val updateProperties = table.updateProperties()
    baseTableSnapshotsProperties.foreach {
      case (key, value) => updateProperties.set(key, value)
    }
    updateProperties.commit()

    table.refresh()

    createMaterializedView(storageTableLocation)
    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"CreateMaterializedViewExec: ${ident}"
  }

  private def createMaterializedView(storageTableLocation: String): Unit = {
    val currentCatalogName = session.sessionState.catalogManager.currentCatalog.name
    val currentCatalog = if (!catalog.name().equals(currentCatalogName)) currentCatalogName else null
    val currentNamespace = session.sessionState.catalogManager.currentNamespace

    val engineVersion = "Spark " + org.apache.spark.SPARK_VERSION
    val newProperties = properties ++
      comment.map(ViewCatalog.PROP_COMMENT -> _) +
      (ViewCatalog.PROP_CREATE_ENGINE_VERSION -> engineVersion,
        ViewCatalog.PROP_ENGINE_VERSION -> engineVersion) +
      (MaterializedViewUtil.MATERIALIZED_VIEW_PROPERTY_KEY -> "true") +
      (MaterializedViewUtil.MATERIALIZED_VIEW_STORAGE_LOCATION_PROPERTY_KEY -> storageTableLocation)

    if (replace) {
      // CREATE OR REPLACE VIEW
      if (catalog.viewExists(ident)) {
        catalog.dropView(ident)
      }
      // FIXME: replaceView API doesn't exist in Spark 3.5
      catalog.createView(
        ident,
        queryText,
        currentCatalog,
        currentNamespace,
        viewSchema,
        queryColumnNames.toArray,
        columnAliases.toArray,
        columnComments.map(c => c.orNull).toArray,
        newProperties.asJava)
    } else {
      try {
        // CREATE VIEW [IF NOT EXISTS]
        catalog.createView(
          ident,
          queryText,
          currentCatalog,
          currentNamespace,
          viewSchema,
          queryColumnNames.toArray,
          columnAliases.toArray,
          columnComments.map(c => c.orNull).toArray,
          newProperties.asJava)
      } catch {
        // TODO: Make sure the existing view is also a materialized view
        case _: ViewAlreadyExistsException if allowExisting => // Ignore
      }
    }
  }

  private def getBaseTableSnapshots(baseTables: List[Table]): Map[UUID, Long] = {
    baseTables.map {
      case sparkTable: SparkTable =>
        val snapshot = Option(sparkTable.table().currentSnapshot())
        val snapshotId = snapshot.map(_.snapshotId().longValue()).getOrElse(0L)
        (sparkTable.table().uuid(), snapshotId)
      case _ =>
        throw new UnsupportedOperationException("Only Spark tables are supported")
    }.toMap
  }
}
