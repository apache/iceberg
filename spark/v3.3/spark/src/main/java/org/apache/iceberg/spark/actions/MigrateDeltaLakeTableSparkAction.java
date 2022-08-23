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

import io.delta.standalone.DeltaLog;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.actions.BaseMigrateDeltaLakeTableActionResult;
import org.apache.iceberg.actions.MigrateDeltaLakeTable;
import org.apache.iceberg.data.TableMigrationUtil;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
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
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

/**
 * Takes a Delta Lake table and attempts to transform it into an Iceberg table in the same location
 * with the same identifier. Once complete the identifier which previously referred to a non-Iceberg
 * table will refer to the newly migrated Iceberg table.
 */
public class MigrateDeltaLakeTableSparkAction implements MigrateDeltaLakeTable {

  private static final Logger LOG = LoggerFactory.getLogger(MigrateDeltaLakeTableSparkAction.class);

  private final Map<String, String> additionalProperties = Maps.newHashMap();
  private final SparkSession spark;
  private final DeltaLog deltaLog;
  private final StagingTableCatalog destCatalog;
  private final String deltaTableLocation;
  private final Identifier newIdentifier;

  MigrateDeltaLakeTableSparkAction(
      SparkSession spark,
      CatalogPlugin destCatalog,
      String deltaTableLocation,
      Identifier newIdentifier) {
    this.spark = spark;
    this.destCatalog = checkDestinationCatalog(destCatalog);
    this.newIdentifier = newIdentifier;
    this.deltaTableLocation = deltaTableLocation;
    this.deltaLog =
        DeltaLog.forTable(spark.sessionState().newHadoopConf(), this.deltaTableLocation);
  }

  @Override
  public Result execute() {
    // Get a DeltaLog instance and retrieve the partitions (if applicable) of the table
    io.delta.standalone.Snapshot updatedSnapshot = deltaLog.update();

    StructType structType = getStructTypeFromDeltaSnapshot();

    StagedSparkTable stagedTable =
        stageDestTable(
            updatedSnapshot,
            deltaTableLocation,
            destCatalog,
            newIdentifier,
            structType,
            additionalProperties);
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
        deltaTableLocation);
    return new BaseMigrateDeltaLakeTableActionResult(totalDataFiles);
  }

  private void copyFromDeltaLakeToIceberg(Table table, PartitionSpec spec) {
    // Get all changes starting from version 0
    Iterator<VersionLog> it = deltaLog.getChanges(0, false);

    while (it.hasNext()) {
      VersionLog versionLog = it.next();
      List<Action> actions = versionLog.getActions();

      // We first need to iterate through to see what kind of transaction this was. There are 3
      // cases:
      // 1. AppendFile - when there are only AddFile instances (an INSERT on the table)
      // 2. DeleteFiles - when there are only RemoveFile instances (a DELETE where all the records
      // of file(s) were removed
      // 3. OverwriteFiles - when there are a mix of AddFile and RemoveFile (a DELETE/UPDATE)

      // Create a map of Delta Lake Action (AddFile, RemoveFile, etc.) --> List<Action>
      Map<String, List<Action>> deltaLakeActionsMap =
          actions.stream()
              .filter(action -> action instanceof AddFile || action instanceof RemoveFile)
              .collect(Collectors.groupingBy(a -> a.getClass().getSimpleName()));
      // Scan the map so that we know what type of transaction this will be in Iceberg
      IcebergTransactionType icebergTransactionType =
          getIcebergTransactionTypeFromDeltaActions(deltaLakeActionsMap);
      if (icebergTransactionType == null) {
        return;
      }

      List<DataFile> filesToAdd = Lists.newArrayList();
      List<DataFile> filesToRemove = Lists.newArrayList();
      for (Action action : Iterables.concat(deltaLakeActionsMap.values())) {
        DataFile dataFile = buildDataFileForAction(action, table, spec);
        if (action instanceof AddFile) {
          filesToAdd.add(dataFile);
        } else {
          // We would have thrown an exception above if it wasn't a RemoveFile
          filesToRemove.add(dataFile);
        }
      }

      switch (icebergTransactionType) {
        case APPEND_FILES:
          AppendFiles appendFiles = table.newAppend();
          filesToAdd.forEach(appendFiles::appendFile);
          appendFiles.commit();
          break;
        case DELETE_FILES:
          DeleteFiles deleteFiles = table.newDelete();
          filesToRemove.forEach(deleteFiles::deleteFile);
          deleteFiles.commit();
          break;
        case OVERWRITE_FILES:
          OverwriteFiles overwriteFiles = table.newOverwrite();
          filesToAdd.forEach(overwriteFiles::addFile);
          filesToRemove.forEach(overwriteFiles::deleteFile);
          overwriteFiles.commit();
          break;
      }
    }
  }

  private DataFile buildDataFileForAction(Action action, Table table, PartitionSpec spec) {
    String path;
    long size;
    Map<String, String> partitionValues;

    if (action instanceof AddFile) {
      AddFile addFile = (AddFile) action;
      path = addFile.getPath();
      size = addFile.getSize();
      partitionValues = addFile.getPartitionValues();
    } else if (action instanceof RemoveFile) {
      RemoveFile removeFile = (RemoveFile) action;
      path = removeFile.getPath();
      size =
          removeFile
              .getSize()
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          String.format("File %s removed with specifying a size", path)));
      partitionValues =
          Optional.ofNullable(removeFile.getPartitionValues())
              .orElseThrow(
                  () ->
                      new RuntimeException(
                          String.format(
                              "File %s removed without specifying partition values", path)));
    } else {
      throw new IllegalStateException(
          String.format(
              "Unexpected action type for Delta Lake: %s", action.getClass().getSimpleName()));
    }

    String fullFilePath = deltaLog.getPath().toString() + File.separator + path;
    Metrics metrics = getMetricsForFile(table, fullFilePath);

    String partition =
        spec.fields().stream()
            .map(PartitionField::name)
            .map(name -> String.format("%s=%s", name, partitionValues.get(name)))
            .collect(Collectors.joining("/"));

    return DataFiles.builder(spec)
        .withPath(fullFilePath)
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(size)
        .withMetrics(metrics)
        .withPartitionPath(partition)
        .withRecordCount(metrics.recordCount())
        .build();
  }

  private Metrics getMetricsForFile(Table table, String fullFilePath) {
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    return TableMigrationUtil.getParquetMetrics(
        new Path(fullFilePath), spark.sessionState().newHadoopConf(), metricsConfig, nameMapping);
  }

  @Nullable
  private IcebergTransactionType getIcebergTransactionTypeFromDeltaActions(
      Map<String, List<Action>> actionsMap) {
    IcebergTransactionType icebergTransactionType;
    if (actionsMap.containsKey(AddFile.class.getSimpleName())
        && !actionsMap.containsKey(RemoveFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.APPEND_FILES;
    } else if (actionsMap.containsKey(RemoveFile.class.getSimpleName())
        && !actionsMap.containsKey(AddFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.DELETE_FILES;
    } else if (actionsMap.containsKey(AddFile.class.getSimpleName())
        && actionsMap.containsKey(RemoveFile.class.getSimpleName())) {
      icebergTransactionType = IcebergTransactionType.OVERWRITE_FILES;
    } else {
      // Some other type of transaction, we can ignore
      return null;
    }
    return icebergTransactionType;
  }

  private PartitionSpec getPartitionSpecFromDeltaSnapshot(StructType structType) {
    Schema schema = SparkSchemaUtil.convert(structType);
    PartitionSpec spec =
        SparkSchemaUtil.identitySpec(
            schema, deltaLog.snapshot().getMetadata().getPartitionColumns());
    return spec == null ? PartitionSpec.unpartitioned() : spec;
  }

  private StructType getStructTypeFromDeltaSnapshot() {
    io.delta.standalone.types.StructField[] fields =
        Optional.ofNullable(deltaLog.snapshot().getMetadata().getSchema())
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

  @Override
  public MigrateDeltaLakeTable tableProperties(Map<String, String> properties) {
    additionalProperties.putAll(properties);
    return this;
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

  private static Map<String, String> destTableProperties(
      io.delta.standalone.Snapshot deltaSnapshot,
      String tableLocation,
      Map<String, String> additionalProperties) {
    Map<String, String> properties = Maps.newHashMap();

    properties.putAll(deltaSnapshot.getMetadata().getConfiguration());
    properties.putAll(
        ImmutableMap.of(
            "provider",
            "iceberg",
            "migrated",
            "true",
            "table_type",
            "iceberg",
            "location",
            tableLocation));
    properties.putAll(additionalProperties);

    return properties;
  }

  private StagingTableCatalog checkDestinationCatalog(CatalogPlugin catalog) {

    return (StagingTableCatalog) catalog;
  }

  private enum IcebergTransactionType {
    APPEND_FILES,
    DELETE_FILES,
    OVERWRITE_FILES
  }
}
