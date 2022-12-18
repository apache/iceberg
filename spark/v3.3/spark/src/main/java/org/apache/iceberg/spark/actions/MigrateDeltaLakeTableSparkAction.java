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

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.actions.BaseMigrateDeltaLakeTableActionResult;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.source.StagedSparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.expressions.LogicalExpressions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

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
        null,
        deltaTableLocation,
        TableIdentifier.of(newIdentifier.name()),
        spark.sessionState().newHadoopConf());
    this.spark = spark;
    this.destCatalog = checkDestinationCatalog(destCatalog);
    this.newIdentifier = newIdentifier;
  }

  @Override
  public Result execute() {
    io.delta.standalone.Snapshot updatedSnapshot = deltaLog().update();
    StructType structType = getStructTypeFromDeltaSnapshot();
    StagedSparkTable stagedTable =
        stageDestTable(
            updatedSnapshot,
            deltaTableLocation(),
            destCatalog,
            newIdentifier,
            structType,
            additionalProperties());
    PartitionSpec partitionSpec = getPartitionSpecFromDeltaSnapshot(structType);
    Table icebergTable = stagedTable.table();
    copyFromDeltaLakeToIceberg(icebergTable, partitionSpec);

    stagedTable.commitStagedChanges();
    Snapshot snapshot = icebergTable.currentSnapshot();
    long totalDataFiles =
        Long.parseLong(snapshot.summary().get(SnapshotSummary.TOTAL_DATA_FILES_PROP));
    LOG.info(
        "Successfully loaded Iceberg metadata for {} files to {}",
        totalDataFiles,
        deltaTableLocation());
    return new BaseMigrateDeltaLakeTableActionResult(totalDataFiles);
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

  private PartitionSpec getPartitionSpecFromDeltaSnapshot(StructType structType) {
    Schema schema = SparkSchemaUtil.convert(structType);
    PartitionSpec spec =
        SparkSchemaUtil.identitySpec(
            schema, deltaLog().snapshot().getMetadata().getPartitionColumns());
    return spec == null ? PartitionSpec.unpartitioned() : spec;
  }

  private StructType getStructTypeFromDeltaSnapshot() {
    io.delta.standalone.types.StructField[] fields =
        Optional.ofNullable(deltaLog().snapshot().getMetadata().getSchema())
            .map(io.delta.standalone.types.StructType::getFields)
            .orElseThrow(() -> new RuntimeException("Cannot determine table schema!"));

    // Convert from Delta StructFields to Spark StructFields
    return new StructType(
        Arrays.stream(fields)
            .map(
                s ->
                    new StructField(
                        s.getName(),
                        DataType.fromJson(s.getDataType().toJson()),
                        s.isNullable(),
                        Metadata.fromJson(s.getMetadata().toString())))
            .toArray(StructField[]::new));
  }

  private static StagedSparkTable stageDestTable(
      io.delta.standalone.Snapshot deltaSnapshot,
      String tableLocation,
      StagingTableCatalog destinationCatalog,
      Identifier destIdentifier,
      StructType structType,
      Map<String, String> additionalProperties) {
    try {
      Map<String, String> props =
          destTableProperties(deltaSnapshot, tableLocation, additionalProperties);
      io.delta.standalone.types.StructType schema = deltaSnapshot.getMetadata().getSchema();
      if (schema == null) {
        throw new IllegalStateException("Could not find schema in existing Delta Lake table.");
      }

      Transform[] partitioning = getPartitioning(deltaSnapshot);

      return (StagedSparkTable)
          destinationCatalog.stageCreate(destIdentifier, structType, partitioning, props);
    } catch (org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException e) {
      throw new NoSuchNamespaceException(
          "Cannot create table %s as the namespace does not exist", destIdentifier);
    } catch (TableAlreadyExistsException e) {
      throw new AlreadyExistsException(
          "Cannot create table %s as it already exists", destIdentifier);
    }
  }

  private static Transform[] getPartitioning(io.delta.standalone.Snapshot deltaSnapshot) {
    return deltaSnapshot.getMetadata().getPartitionColumns().stream()
        .map(
            name ->
                LogicalExpressions.identity(
                    LogicalExpressions.reference(
                        JavaConverters.asScalaBuffer(Collections.singletonList(name)))))
        .toArray(Transform[]::new);
  }
}
