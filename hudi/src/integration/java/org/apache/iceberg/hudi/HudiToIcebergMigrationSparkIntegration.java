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
package org.apache.iceberg.hudi;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;

public class HudiToIcebergMigrationSparkIntegration {
  private HudiToIcebergMigrationSparkIntegration() {}

  static SnapshotHudiTable snapshotHudiTable(
      SparkSession spark, String hudiTablePath, String newTableIdentifier) {
    Preconditions.checkArgument(
        spark != null, "The SparkSession cannot be null, please provide a valid SparkSession");
    Preconditions.checkArgument(
        newTableIdentifier != null,
        "The table identifier cannot be null, please provide a valid table identifier for the new iceberg table");
    Preconditions.checkArgument(
        hudiTablePath != null,
        "The hudi table location cannot be null, please provide a valid location of the delta lake table to be snapshot");
    String ctx = "hudi snapshot target";
    CatalogPlugin defaultCatalog = spark.sessionState().catalogManager().currentCatalog();
    Spark3Util.CatalogAndIdentifier catalogAndIdentifier =
        Spark3Util.catalogAndIdentifier(ctx, spark, newTableIdentifier, defaultCatalog);
    return HudiToIcebergMigrationActionsProvider.defaultProvider()
        .snapshotHudiTable(hudiTablePath)
        .as(TableIdentifier.parse(catalogAndIdentifier.identifier().toString()))
        .hoodieConfiguration(spark.sessionState().newHadoopConf())
        .icebergCatalog(
            Spark3Util.loadIcebergCatalog(spark, catalogAndIdentifier.catalog().name()));
  }
}
