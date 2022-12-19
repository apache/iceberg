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
package org.apache.iceberg.spark.actions;

import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a Delta Lake table and attempts to transform it into an Iceberg table in the same location
 * with the same identifier. Once complete the identifier which previously referred to a non-Iceberg
 * table will refer to the newly migrated Iceberg table.
 */
public class MigrateDeltaLakeTableSparkAction extends BaseMigrateDeltaLakeTableAction {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateDeltaLakeTableSparkAction.class);

  private final SparkSession spark;
  private final StagingTableCatalog destCatalog;
  private final Identifier newIdentifier;

  MigrateDeltaLakeTableSparkAction(
      SparkSession spark,
      CatalogPlugin destCatalog,
      String deltaTableLocation,
      Identifier newIdentifier) {
    super(
        Spark3Util.loadIcebergCatalog(
            spark, spark.sessionState().catalogManager().currentCatalog().name()),
        deltaTableLocation,
        TableIdentifier.parse(newIdentifier.toString()),
        spark.sessionState().newHadoopConf());
    this.spark = spark;
    this.destCatalog = checkDestinationCatalog(destCatalog);
    this.newIdentifier = newIdentifier;
  }

  protected Metrics getMetricsForFile(Table table, String fullFilePath, FileFormat format) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    return TableMigrationUtil.getParquetMetrics(
        new Path(fullFilePath), spark.sessionState().newHadoopConf(), metricsConfig, nameMapping);
  }

  private StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {

    return (StagingTableCatalog) catalog;
  }
}
