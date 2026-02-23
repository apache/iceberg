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

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a Delta Lake table's location and attempts to create an Iceberg table snapshot in an
 * optional user-specified location (default to the Delta Lake table's location) with a different
 * identifier.
 */
class BaseSnapshotDeltaLakeTableAction implements SnapshotDeltaLakeTable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseSnapshotDeltaLakeTableAction.class);

  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String PARQUET_SUFFIX = ".parquet";
  private static final String DELTA_VERSION_TAG_PREFIX = "delta-version-";
  private static final String DELTA_TIMESTAMP_TAG_PREFIX = "delta-ts-";
  private final ImmutableMap.Builder<String, String> additionalPropertiesBuilder =
      ImmutableMap.builder();
  private Engine deltaEngine;
  private io.delta.kernel.Table deltaTable;
  private Catalog icebergCatalog;
  private final String deltaTableLocation;
  private TableIdentifier newTableIdentifier;
  private String newTableLocation;
  private HadoopFileIO deltaLakeFileIO;
  private long deltaStartVersion;

  /**
   * Snapshot a delta lake table to be an iceberg table. The action will read the delta lake table's
   * log through the table's path, create a new iceberg table using the given icebergCatalog and
   * newTableIdentifier, and commit all changes in one iceberg transaction.
   *
   * <p>The new table will only be created if the snapshot is successful.
   *
   * @param deltaTableLocation the delta lake table's path
   */
  BaseSnapshotDeltaLakeTableAction(String deltaTableLocation) {
    this.deltaTableLocation = deltaTableLocation;
    this.newTableLocation = deltaTableLocation;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperties(Map<String, String> properties) {
    additionalPropertiesBuilder.putAll(properties);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperty(String name, String value) {
    additionalPropertiesBuilder.put(name, value);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableLocation(String location) {
    this.newTableLocation = location;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable as(TableIdentifier identifier) {
    this.newTableIdentifier = identifier;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable icebergCatalog(Catalog catalog) {
    this.icebergCatalog = catalog;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable deltaLakeConfiguration(Configuration conf) {
    this.deltaEngine = DefaultEngine.create(conf);
    this.deltaLakeFileIO = new HadoopFileIO(conf);
    this.deltaTable = io.delta.kernel.Table.forPath(deltaEngine, deltaTableLocation);

    return this;
  }

  @Override
  public SnapshotDeltaLakeTable.Result execute() {
    Preconditions.checkArgument(
        icebergCatalog != null && newTableIdentifier != null,
        "Iceberg catalog and identifier cannot be null. Make sure to configure the action with a valid Iceberg catalog and identifier.");
    Preconditions.checkArgument(
        deltaTable != null && deltaEngine != null && deltaLakeFileIO != null,
        "Make sure to configure the action with a valid deltaLakeConfiguration");
    try {
      // Validate table exists by attempting to load latest snapshot
      this.deltaTable.getLatestSnapshot(deltaEngine);
    } catch (TableNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "Delta Lake table does not exist at the given location: %s", deltaTableLocation),
          e);
    }
    // get the earliest recreatable commit version available in the Delta Lake table
    Path logPath = new Path(deltaTableLocation, "_delta_log");
    deltaStartVersion = DeltaHistoryManager.getEarliestRecreatableCommit(deltaEngine, logPath);
    ImmutableSet.Builder<String> migratedDataFilesBuilder = ImmutableSet.builder();
    Snapshot updatedSnapshot = deltaTable.getLatestSnapshot(deltaEngine);
    Schema schema = convertDeltaLakeSchema(updatedSnapshot.getSchema());
    PartitionSpec partitionSpec = getPartitionSpecFromDeltaSnapshot(schema, updatedSnapshot);
    Transaction icebergTransaction =
        icebergCatalog.newCreateTableTransaction(
            newTableIdentifier,
            schema,
            partitionSpec,
            newTableLocation,
            destTableProperties(updatedSnapshot, deltaTableLocation));
    icebergTransaction
        .table()
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(icebergTransaction.table().schema())))
        .commit();
    long constructableStartVersion =
        commitInitialDeltaSnapshotToIcebergTransaction(
            updatedSnapshot.getVersion(), icebergTransaction, migratedDataFilesBuilder);
    for (long version = constructableStartVersion + 1;
        version <= updatedSnapshot.getVersion();
        version++) {
      commitDeltaVersionLogToIcebergTransaction(
          version, icebergTransaction, migratedDataFilesBuilder);
    }
    icebergTransaction.commitTransaction();

    long totalDataFiles = migratedDataFilesBuilder.build().size();
    LOG.info(
        "Successfully created Iceberg table {} from Delta Lake table at {}, total data file count: {}",
        newTableIdentifier,
        deltaTableLocation,
        totalDataFiles);
    return ImmutableSnapshotDeltaLakeTable.Result.builder()
        .snapshotDataFilesCount(totalDataFiles)
        .build();
  }

  private Schema convertDeltaLakeSchema(io.delta.kernel.types.StructType deltaSchema) {
    Type converted =
        DeltaLakeDataTypeVisitor.visit(deltaSchema, new DeltaLakeTypeToType(deltaSchema));
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private PartitionSpec getPartitionSpecFromDeltaSnapshot(Schema schema, Snapshot deltaSnapshot) {
    List<String> partitionNames = deltaSnapshot.getPartitionColumnNames();
    if (partitionNames.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (String partitionName : partitionNames) {
      builder.identity(partitionName);
    }
    return builder.build();
  }

  /**
   * Commit the initial delta snapshot to iceberg transaction. It tries the snapshot starting from
   * {@code deltaStartVersion} to {@code latestVersion} and commit the first constructable one.
   *
   * <p>There are two cases that the delta snapshot is not constructable:
   *
   * <ul>
   *   <li>the version is earlier than the earliest checkpoint
   *   <li>the corresponding data files are deleted by {@code VACUUM}
   * </ul>
   *
   * <p>For more information, please refer to delta lake's <a
   * href="https://docs.delta.io/latest/delta-batch.html#-data-retention">Data Retention</a>
   *
   * @param latestVersion the latest version of the delta lake table
   * @param transaction the iceberg transaction
   * @return the initial version of the delta lake table that is successfully committed to iceberg
   */
  private long commitInitialDeltaSnapshotToIcebergTransaction(
      long latestVersion,
      Transaction transaction,
      ImmutableSet.Builder<String> migratedDataFilesBuilder) {
    long constructableStartVersion = deltaStartVersion;
    while (constructableStartVersion <= latestVersion) {
      try {
        Snapshot snapshot =
            deltaTable.getSnapshotAsOfVersion(deltaEngine, constructableStartVersion);
        List<AddFile> initDataFiles = getAddFilesFromSnapshot(snapshot);
        List<DataFile> filesToAdd = Lists.newArrayList();
        for (AddFile addFile : initDataFiles) {
          DataFile dataFile = buildDataFileFromAction(addFile, transaction.table());
          filesToAdd.add(dataFile);
          migratedDataFilesBuilder.add(dataFile.location());
        }

        // AppendFiles case
        AppendFiles appendFiles = transaction.newAppend();
        filesToAdd.forEach(appendFiles::appendFile);
        appendFiles.commit();
        tagCurrentSnapshot(constructableStartVersion, transaction);

        return constructableStartVersion;
      } catch (NotFoundException | IllegalArgumentException | KernelException e) {
        constructableStartVersion++;
      }
    }

    throw new ValidationException(
        "Delta Lake table at %s contains no constructable snapshot", deltaTableLocation);
  }

  /**
   * Returns all {@link AddFile} actions visible in the given Delta snapshot.
   *
   * <p>The snapshot is scanned to collect the set of data files that constitute the table state at
   * that version.
   *
   * @param snapshot the Delta snapshot
   * @return list of {@link AddFile} actions in the snapshot
   */
  private List<AddFile> getAddFilesFromSnapshot(Snapshot snapshot) {
    List<AddFile> addFiles = Lists.newArrayList();
    Scan scan = snapshot.getScanBuilder().build();
    try (CloseableIterator<FilteredColumnarBatch> scanFileIter = scan.getScanFiles(deltaEngine)) {
      while (scanFileIter.hasNext()) {
        FilteredColumnarBatch batch = scanFileIter.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row scanFileRow = rows.next();
            if (DeltaActionUtils.isAdd(scanFileRow)) {
              addFiles.add(DeltaActionUtils.getAdd(scanFileRow));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    return addFiles;
  }

  /**
   * Iterate through the Delta Lake change log to determine the update type and commit the update to
   * the given {@code Transaction}.
   *
   * <p>There are 3 cases:
   *
   * <p>1. AppendFiles - when there are only AddFile instances (an INSERT on the table)
   *
   * <p>2. DeleteFiles - when there are only RemoveFile instances (a DELETE where all the records of
   * file(s) were removed)
   *
   * <p>3. OverwriteFiles - when there are a mix of AddFile and RemoveFile (a DELETE/UPDATE)
   *
   * @param version the delta log version to commit to iceberg table transaction
   * @param transaction the iceberg table transaction to commit to
   */
  private void commitDeltaVersionLogToIcebergTransaction(
      long version,
      Transaction transaction,
      ImmutableSet.Builder<String> migratedDataFilesBuilder) {
    // Only need actions related to data change: AddFile and RemoveFile
    List<RowBackedAction> dataFileActions = getDataFileActions(version);

    List<DataFile> filesToAdd = Lists.newArrayList();
    List<DataFile> filesToRemove = Lists.newArrayList();
    for (RowBackedAction action : dataFileActions) {
      DataFile dataFile = buildDataFileFromAction(action, transaction.table());
      if (action instanceof AddFile) {
        filesToAdd.add(dataFile);
      } else if (action instanceof RemoveFile) {
        filesToRemove.add(dataFile);
      } else {
        throw new ValidationException(
            "The action %s's is unsupported", action.getClass().getSimpleName());
      }
      migratedDataFilesBuilder.add(dataFile.location());
    }

    if (!filesToAdd.isEmpty() && !filesToRemove.isEmpty()) {
      // OverwriteFiles case
      OverwriteFiles overwriteFiles = transaction.newOverwrite();
      filesToAdd.forEach(overwriteFiles::addFile);
      filesToRemove.forEach(overwriteFiles::deleteFile);
      overwriteFiles.commit();
    } else if (!filesToAdd.isEmpty()) {
      // AppendFiles case
      AppendFiles appendFiles = transaction.newAppend();
      filesToAdd.forEach(appendFiles::appendFile);
      appendFiles.commit();
    } else if (!filesToRemove.isEmpty()) {
      // DeleteFiles case
      DeleteFiles deleteFiles = transaction.newDelete();
      filesToRemove.forEach(deleteFiles::deleteFile);
      deleteFiles.commit();
    } else {
      // No data change case, dummy append to tag the snapshot
      transaction.newAppend().commit();
    }

    tagCurrentSnapshot(version, transaction);
  }

  /**
   * Returns data file change actions ({@link AddFile} and {@link RemoveFile}) for the specified
   * Delta Lake version.
   *
   * <p>This method reads the Delta change log using the Kernel getChanges API and extracts only
   * file-level actions.
   *
   * @param version the Delta Lake version
   * @return list of file add and remove actions for the version
   */
  private List<RowBackedAction> getDataFileActions(long version) {
    List<RowBackedAction> dataFileActions = Lists.newArrayList();
    TableImpl tableImpl = (TableImpl) deltaTable;
    Set<DeltaAction> actions = Set.of(DeltaAction.ADD, DeltaAction.REMOVE);

    try (CloseableIterator<ColumnarBatch> changes =
        tableImpl.getChanges(deltaEngine, version, version, actions)) {
      while (changes.hasNext()) {
        ColumnarBatch batch = changes.next();
        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row row = rows.next();
            if (DeltaActionUtils.isAdd(row)) {
              dataFileActions.add(DeltaActionUtils.getAdd(row));
            } else if (DeltaActionUtils.isRemove(row)) {
              dataFileActions.add(DeltaActionUtils.getRemove(row));
            }
          }
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return dataFileActions;
  }

  private DataFile buildDataFileFromAction(RowBackedAction action, Table table) {
    PartitionSpec spec = table.spec();
    String path;
    long fileSize;
    Long nullableFileSize;
    Map<String, String> partitionValues;

    if (action instanceof AddFile) {
      AddFile addFile = (AddFile) action;
      path = addFile.getPath();
      nullableFileSize = addFile.getSize();
      partitionValues = extractPartitionValues(addFile);
    } else if (action instanceof RemoveFile) {
      RemoveFile removeFile = (RemoveFile) action;
      path = removeFile.getPath();
      nullableFileSize = removeFile.getSize().orElse(null);
      partitionValues = extractPartitionValues(removeFile);
    } else {
      throw new ValidationException(
          "Unexpected action type for Delta Lake: %s", action.getClass().getSimpleName());
    }

    String fullFilePath = getFullFilePath(path, deltaTableLocation);

    FileFormat format = determineFileFormatFromPath(fullFilePath);
    InputFile file = deltaLakeFileIO.newInputFile(fullFilePath);
    if (!file.exists()) {
      throw new NotFoundException(
          "File %s is referenced in the logs of Delta Lake table at %s, but cannot be found in the storage",
          fullFilePath, deltaTableLocation);
    }

    // If the file size is not specified, the size should be read from the file
    if (nullableFileSize != null) {
      fileSize = nullableFileSize;
    } else {
      fileSize = file.getLength();
    }

    // get metrics from the file
    MetricsConfig metricsConfig = MetricsConfig.forTable(table);
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    Metrics metrics = getMetricsForFile(file, format, metricsConfig, nameMapping);

    List<String> partitionValueList =
        spec.fields().stream()
            .map(PartitionField::name)
            .map(partitionValues::get)
            .collect(Collectors.toList());

    return DataFiles.builder(spec)
        .withPath(fullFilePath)
        .withFormat(format)
        .withFileSizeInBytes(fileSize)
        .withMetrics(metrics)
        .withPartitionValues(partitionValueList)
        .build();
  }

  /**
   * Extracts partition values from an AddFile action.
   *
   * @param addFile the AddFile action
   * @return map of partition column names to values
   */
  private Map<String, String> extractPartitionValues(AddFile addFile) {
    return VectorUtils.toJavaMap(addFile.getPartitionValues());
  }

  /**
   * Extracts partition values from a RemoveFile action.
   *
   * @param removeFile the RemoveFile action
   * @return map of partition column names to values, or empty map if not present
   */
  private Map<String, String> extractPartitionValues(RemoveFile removeFile) {
    return removeFile.getPartitionValues().isPresent()
        ? VectorUtils.toJavaMap(removeFile.getPartitionValues().get())
        : Collections.emptyMap();
  }

  private FileFormat determineFileFormatFromPath(String path) {
    if (path.endsWith(PARQUET_SUFFIX)) {
      return FileFormat.PARQUET;
    } else {
      throw new ValidationException("Do not support file format in path %s", path);
    }
  }

  private Metrics getMetricsForFile(
      InputFile file, FileFormat format, MetricsConfig metricsSpec, NameMapping mapping) {
    if (format == FileFormat.PARQUET) {
      return ParquetUtil.fileMetrics(file, metricsSpec, mapping);
    }
    throw new ValidationException("Cannot get metrics from file format: %s", format);
  }

  private Map<String, String> destTableProperties(Snapshot deltaSnapshot, String originalLocation) {
    additionalPropertiesBuilder.putAll(
        ((io.delta.kernel.internal.SnapshotImpl) deltaSnapshot).getMetadata().getConfiguration());
    additionalPropertiesBuilder.putAll(
        ImmutableMap.of(
            SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE, ORIGINAL_LOCATION_PROP, originalLocation));

    return additionalPropertiesBuilder.build();
  }

  private void tagCurrentSnapshot(long deltaVersion, Transaction transaction) {
    long currentSnapshotId = transaction.table().currentSnapshot().snapshotId();

    ManageSnapshots manageSnapshots = transaction.manageSnapshots();
    manageSnapshots.createTag(DELTA_VERSION_TAG_PREFIX + deltaVersion, currentSnapshotId);

    long deltaVersionTimestamp =
        deltaTable.getSnapshotAsOfVersion(deltaEngine, deltaVersion).getTimestamp(deltaEngine);
    if (deltaVersionTimestamp > 0) {
      manageSnapshots.createTag(
          DELTA_TIMESTAMP_TAG_PREFIX + deltaVersionTimestamp, currentSnapshotId);
    }
    manageSnapshots.commit();
  }

  /**
   * Get the full file path, the input {@code String} path can be either a relative path or an
   * absolute path of a data file in delta table
   *
   * @param path the return value of {@link AddFile#getPath()} or {@link RemoveFile#getPath()}
   *     (either absolute or relative)
   * @param tableRoot the root path of the delta table
   */
  private static String getFullFilePath(String path, String tableRoot) {
    URI dataFileUri = URI.create(path);
    String decodedPath = URLDecoder.decode(path, StandardCharsets.UTF_8);
    if (dataFileUri.isAbsolute()) {
      return decodedPath;
    } else {
      return tableRoot + File.separator + decodedPath;
    }
  }
}
