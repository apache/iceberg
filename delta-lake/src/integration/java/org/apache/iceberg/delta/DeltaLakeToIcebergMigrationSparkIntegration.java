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
package org.apache.iceberg.delta;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

/** An example class shows how to use the delta lake migration actions in SparkContext. */
class DeltaLakeToIcebergMigrationSparkIntegration {

  private DeltaLakeToIcebergMigrationSparkIntegration() {}

  /**
   * Example of how to use a {@link SparkSession}, a table identifier and a delta table location to
   * construct an action for snapshotting the delta table to an iceberg table.
   *
   * @param spark a SparkSession with iceberg catalog configured.
   * @param newTableIdentifier can be both 2 parts and 3 parts identifier, if it is 2 parts, the
   *     default spark catalog will be used
   * @param deltaTableLocation the location of the delta table
   * @return an instance of snapshot delta lake table action.
   */
  static SnapshotDeltaLakeTable snapshotDeltaLakeTable(
      SparkSession spark, String newTableIdentifier, String deltaTableLocation) {
    Preconditions.checkArgument(
        spark != null, "The SparkSession cannot be null, please provide a valid SparkSession");
    Preconditions.checkArgument(
        newTableIdentifier != null,
        "The table identifier cannot be null, please provide a valid table identifier for the new iceberg table");
    Preconditions.checkArgument(
        deltaTableLocation != null,
        "The delta lake table location cannot be null, please provide a valid location of the delta lake table to be snapshot");

    String ctx = "delta lake snapshot target";
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdent =
        Spark3Util.catalogAndIdentifier(ctx, spark, newTableIdentifier, defaultCatalog);
    return DeltaLakeToIcebergMigrationActionsProvider.defaultActions()
        .snapshotDeltaLakeTable(deltaTableLocation)
        .as(TableIdentifier.parse(catalogAndIdent.identifier().toString()))
        .deltaLakeConfiguration(spark.sessionState().newHadoopConf())
        .icebergCatalog(Spark3Util.loadIcebergCatalog(spark, catalogAndIdent.catalog().name()));
  }
}
