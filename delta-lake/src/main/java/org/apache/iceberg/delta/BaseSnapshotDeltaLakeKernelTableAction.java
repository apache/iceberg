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
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.delta.InternalDeltaKernelUtils.DeltaAddFile;
import org.apache.iceberg.delta.InternalDeltaKernelUtils.DeltaRemoveFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseSnapshotDeltaLakeKernelTableAction implements SnapshotDeltaLakeTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotDeltaLakeKernelTableAction.class);

  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String CONVERSION_TOOL_PROP = "conversion_tool";
  private static final String CONVERSION_TOOL_VALUE = "iceberg-delta-lake";
  private static final String DELTA_VERSION_TAG_PREFIX = "delta-version-";
  private static final String DELTA_TIMESTAMP_TAG_PREFIX = "delta-ts-";
  private static final Set<String> UNSUPPORTED_DELTA_OPERATIONS =
      Set.of(
          "ADD COLUMNS",
          "CHANGE COLUMN",
          "RENAME COLUMN",
          "DROP COLUMN",
          "ADD CONSTRAINT",
          "SET TBLPROPERTIES"); // The operations will be supported eventually

  private final ImmutableMap.Builder<String, String> icebergPropertiesBuilder =
      ImmutableMap.builder();

  private final String deltaTableLocation;
  private Engine deltaEngine;
  private Table deltaTable;
  private Snapshot deltaLatestSnapshot;

  private Catalog icebergCatalog;
  private TableIdentifier newTableIdentifier;
  private String newTableLocation;
  private HadoopFileIO deltaLakeFileIO;
  private OutputFileFactory icebergDVFileFactory;
  private final Set<Long> deltaTimestampTags = Sets.newHashSet();

  BaseSnapshotDeltaLakeKernelTableAction(String deltaTableLocation) {
    this.deltaTableLocation = deltaTableLocation;
    this.newTableLocation = deltaTableLocation;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperties(Map<String, String> properties) {
    icebergPropertiesBuilder.putAll(properties);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableProperty(String name, String value) {
    icebergPropertiesBuilder.put(name, value);
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable tableLocation(String location) {
    newTableLocation = location;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable as(TableIdentifier identifier) {
    newTableIdentifier = identifier;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable icebergCatalog(Catalog catalog) {
    icebergCatalog = catalog;
    return this;
  }

  @Override
  public SnapshotDeltaLakeTable deltaLakeConfiguration(Configuration conf) {
    deltaEngine = DefaultEngine.create(conf);
    deltaLakeFileIO = new HadoopFileIO(conf);
    deltaTable = Table.forPath(deltaEngine, deltaTableLocation);
    return this;
  }

  @Override
  public Result execute() {
    Preconditions.checkArgument(
        icebergCatalog != null && newTableIdentifier != null,
        "Iceberg catalog and identifier cannot be null. Make sure to configure the action with a valid Iceberg catalog and identifier.");
    Preconditions.checkArgument(
        deltaTable != null && deltaLakeFileIO != null,
        "Make sure to configure the action with a valid deltaLakeConfiguration");

    loadLatestDeltaSnapshot();
    assertDeltaColumnMappingDisabled(
        "Conversion of Delta Lake tables with columnMapping feature is not supported.");

    final long latestDeltaVersion = deltaLatestSnapshot.getVersion();
    final long minimalAvailableVersionInDeltaLogs = getLowerBoundAvailableDeltaVersion();

    LOG.info(
        "Converting Delta Lake table at {} from version {} to version {} into Iceberg table {} ...",
        deltaTableLocation,
        minimalAvailableVersionInDeltaLogs,
        latestDeltaVersion,
        newTableIdentifier);

    Schema icebergSchema = convertToIcebergSchema(deltaLatestSnapshot.getSchema());
    PartitionSpec partitionSpec =
        buildPartitionSpec(icebergSchema, deltaLatestSnapshot.getPartitionColumnNames());

    Transaction transaction =
        icebergCatalog.newCreateTableTransaction(
            newTableIdentifier,
            icebergSchema,
            partitionSpec,
            newTableLocation,
            buildTablePropertiesWithDelta(deltaLatestSnapshot, deltaTableLocation));
    setDefaultNamingMapping(transaction);
    // Create an initial empty snapshot so currentSnapshot() is never null
    transaction.newAppend().commit();

    icebergDVFileFactory =
        OutputFileFactory.builderFor(transaction.table(), 1, 1).format(FileFormat.PUFFIN).build();

    Set<String> processedDataFiles = Sets.newHashSet();
    try {
      long reconstructableStartVersion =
          commitInitialDeltaSnapshotToIcebergTransaction(
              minimalAvailableVersionInDeltaLogs,
              latestDeltaVersion,
              transaction,
              processedDataFiles);
      LOG.info(
          "Converting Delta Lake table from frist re-constructable version {}",
          reconstructableStartVersion);
      convertEachDeltaVersion(
          reconstructableStartVersion + 1, latestDeltaVersion, transaction, processedDataFiles);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    transaction.commitTransaction();

    LOG.info(
        "Successfully created Iceberg table {} from Delta Lake table at {}, processed data file count: {}",
        newTableIdentifier,
        deltaTableLocation,
        processedDataFiles.size());
    return ImmutableSnapshotDeltaLakeTable.Result.builder()
        .snapshotDataFilesCount(processedDataFiles.size())
        .build();
  }

  /**
   * Commit the initial delta snapshot to iceberg transaction. It tries the snapshot starting from
   * {@code startVersion} to {@code latestVersion} and commit the first constructable one.
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
   * @param startVersion the earliest recreatable delta log version
   * @param latestVersion the latest version of the delta lake table
   * @param transaction the iceberg transaction
   * @param processedDataFiles the set to collect processed data file paths
   * @return the initial version of the delta lake table that is successfully committed to iceberg
   */
  private long commitInitialDeltaSnapshotToIcebergTransaction(
      long startVersion,
      long latestVersion,
      Transaction transaction,
      Set<String> processedDataFiles)
      throws IOException {
    long constructableStartVersion = startVersion;
    while (constructableStartVersion <= latestVersion) {
      try {
        Snapshot deltaSnapshot = getDeltaSnapshotAsOfVersion(constructableStartVersion);
        commitDeltaSnapshotToIcebergTransaction(deltaSnapshot, transaction, processedDataFiles);
        return constructableStartVersion;
      } catch (NotFoundException | IllegalArgumentException | KernelException e) {
        constructableStartVersion++;
      }
    }

    throw new ValidationException(
        "Delta Lake table at %s contains no constructable snapshot", deltaTableLocation);
  }

  private void commitDeltaSnapshotToIcebergTransaction(
      Snapshot deltaSnapshot, Transaction transaction, Set<String> processedDataFiles)
      throws IOException {
    Scan scan = deltaSnapshot.getScanBuilder().build();
    try (CloseableIterator<FilteredColumnarBatch> changes = scan.getScanFiles(deltaEngine)) {

      commitDeltaRowsToIcebergTransaction(
          deltaSnapshot.getVersion(),
          Iterators.transform(changes, FilteredColumnarBatch::getRows),
          transaction,
          processedDataFiles);
      tagCurrentSnapshot(
          deltaSnapshot.getVersion(), deltaSnapshot.getTimestamp(deltaEngine), transaction);
    }
  }

  private void convertEachDeltaVersion(
      long initialDeltaVersion,
      long latestDeltaVersion,
      Transaction transaction,
      Set<String> processedDataFiles)
      throws IOException {

    for (long currDeltaVersion = initialDeltaVersion;
        currDeltaVersion <= latestDeltaVersion;
        currDeltaVersion++) {
      try (CloseableIterator<ColumnarBatch> changes =
          InternalDeltaKernelUtils.changes(
              deltaTable, deltaEngine, currDeltaVersion, currDeltaVersion)) {

        Long commitTimestamp =
            commitDeltaRowsToIcebergTransaction(
                currDeltaVersion,
                Iterators.transform(changes, ColumnarBatch::getRows),
                transaction,
                processedDataFiles);
        tagCurrentSnapshot(currDeltaVersion, commitTimestamp, transaction);
      }
    }
  }

  /**
   * Current implementation uses the schema conversion mapping based on the logical names. Delta
   * Lake supports three column mapping modes: none, name, id. So, the renames with columnMapping
   * feature can lead to data corruption.
   */
  private void assertDeltaColumnMappingDisabled(String errorMessage) {
    Map<String, String> configuration =
        InternalDeltaKernelUtils.metadataConfiguration(deltaLatestSnapshot);
    String columnMappingMode = configuration.getOrDefault("delta.columnMapping.mode", "none");
    if (!"none".equals(columnMappingMode)) {
      throw new UnsupportedOperationException(errorMessage);
    }
  }

  private static void setDefaultNamingMapping(Transaction transaction) {
    transaction
        .table()
        .updateProperties()
        .set(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(transaction.table().schema())))
        .commit();
  }

  private Snapshot getDeltaSnapshotAsOfVersion(long deltaVersion) {
    Snapshot snapshot =
        Preconditions.checkNotNull(
            deltaTable.getSnapshotAsOfVersion(deltaEngine, deltaVersion),
            "Delta snapshot for version %s is unreachable.",
            deltaVersion);
    InternalDeltaKernelUtils.assertSnapshotImpl(snapshot);
    return snapshot;
  }

  /**
   * Convert each delta log {@code Row} iterator to Iceberg action and commit to the given {@code
   * Transaction}. The complete <a
   * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Actions">spec</a> of delta
   * actions. <br>
   * Supported:
   * <li>Add
   *
   * @return commit timestamp
   */
  private Long commitDeltaRowsToIcebergTransaction(
      Long deltaVersion,
      Iterator<CloseableIterator<Row>> changes,
      Transaction transaction,
      Set<String> processedDataFiles)
      throws IOException {

    Long originalCommitTimestamp = null;
    List<DataFile> dataFilesToAdd = Lists.newArrayList();
    List<DataFile> dataFilesToRemove = Lists.newArrayList();
    List<DeleteFile> deleteFilesToAdd = Lists.newArrayList();

    while (changes.hasNext()) {
      try (CloseableIterator<Row> rows = changes.next()) {
        while (rows.hasNext()) {
          Row row = rows.next();
          if (DeltaLakeActionsTranslationUtil.isCommitInfo(row)) {
            Row commitInfo = row.getStruct(row.getSchema().indexOf("commitInfo"));
            originalCommitTimestamp =
                commitInfo.getLong(commitInfo.getSchema().indexOf("timestamp"));

            assertSupportedDeltaOperation(deltaVersion, commitInfo);
          } else if (DeltaLakeActionsTranslationUtil.isAdd(row)) {
            DeltaAddFile addFile = InternalDeltaKernelUtils.toAddFile(row);
            DataFile dataFile = buildDataFileFromAddDeltaAction(addFile, transaction);
            dataFilesToAdd.add(dataFile);

            List<DeleteFile> deleteFiles =
                convertDeltaDVsToIcebergDVs(transaction.table().spec(), addFile, dataFile);

            deleteFilesToAdd.addAll(deleteFiles);
          } else if (DeltaLakeActionsTranslationUtil.isRemove(row)) {
            DeltaRemoveFile remove = InternalDeltaKernelUtils.toRemoveFile(row);
            DataFile dataFile = buildDataFileFromRemoveDeltaAction(remove, transaction);
            dataFilesToRemove.add(dataFile);
          }
        }
      }
    }

    // TODO support more actions
    commitIcebergTransaction(transaction, dataFilesToAdd, dataFilesToRemove, deleteFilesToAdd);
    dataFilesToAdd.forEach(dataFile -> processedDataFiles.add(dataFile.location()));
    dataFilesToRemove.forEach(dataFile -> processedDataFiles.add(dataFile.location()));

    return originalCommitTimestamp;
  }

  private List<DeleteFile> convertDeltaDVsToIcebergDVs(
      PartitionSpec partitionSpec, DeltaAddFile addFile, DataFile dataFile) throws IOException {
    if (!addFile.hasDeletionVector()) {
      return List.of();
    }

    DVFileWriter dvWriter = new BaseDVFileWriter(icebergDVFileFactory, path -> null);
    try (DVFileWriter closeableWriter = dvWriter) {
      long[] positions =
          InternalDeltaKernelUtils.readDeltaDVPositions(deltaEngine, deltaTableLocation, addFile);
      for (long deletedRowIndex : positions) {
        closeableWriter.delete(
            dataFile.location(), deletedRowIndex, partitionSpec, dataFile.partition());
      }
    }

    return dvWriter.result().deleteFiles();
  }

  /**
   * CASES:
   *
   * <ol>
   *   <li>Append only
   *   <li>Delete only
   *   <li>Append and Delete =&gt; overwrite
   *   <li>RowDelta with deletes.
   *   <li>No Append, No Delete =&gt; No data changes, append tag or snapshot.
   * </ol>
   */
  private static void commitIcebergTransaction(
      Transaction transaction,
      List<DataFile> dataFilesToAdd,
      List<DataFile> dataFilesToRemove,
      List<DeleteFile> deleteFilesToAdd) {
    if (!deleteFilesToAdd.isEmpty()) {
      // Row Delta
      RowDelta rowDelta = transaction.newRowDelta();
      // Avoid validation for multiple DVs added in transaction
      // org/apache/iceberg/MergingSnapshotProducer.java:854
      // since we do the conversion sequentially in a single Iceberg transaction
      rowDelta.validateFromSnapshot(transaction.table().currentSnapshot().snapshotId());

      dataFilesToAdd.forEach(rowDelta::addRows);
      dataFilesToRemove.forEach(rowDelta::removeRows);
      deleteFilesToAdd.forEach(rowDelta::addDeletes);
      rowDelta.commit();
    } else if (!dataFilesToAdd.isEmpty() && dataFilesToRemove.isEmpty()) {
      // Append only
      AppendFiles appendFiles = transaction.newAppend();
      dataFilesToAdd.forEach(appendFiles::appendFile);
      appendFiles.commit();
    } else if (dataFilesToAdd.isEmpty() && !dataFilesToRemove.isEmpty()) {
      // Delete only
      DeleteFiles deleteFiles = transaction.newDelete();
      dataFilesToRemove.forEach(deleteFiles::deleteFile);
      deleteFiles.commit();
    } else if (!dataFilesToAdd.isEmpty() && !dataFilesToRemove.isEmpty()) {
      // Overwrite
      OverwriteFiles overwriteFiles = transaction.newOverwrite();
      dataFilesToAdd.forEach(overwriteFiles::addFile);
      dataFilesToRemove.forEach(overwriteFiles::deleteFile);
      overwriteFiles.commit();
    } else {
      // Tag case
      transaction.newAppend().commit();
    }
  }

  private DataFile buildDataFileFromAddDeltaAction(DeltaAddFile addFile, Transaction transaction) {
    String path = addFile.path();
    long dataFileSize = addFile.size();
    String fullFilePath = getFullFilePath(path, deltaTable.getPath(deltaEngine));

    InputFile inputDataFile = deltaLakeFileIO.newInputFile(fullFilePath);
    if (!inputDataFile.exists()) {
      throw new NotFoundException(
          "File %s is referenced in the logs of Delta Lake table at %s, but cannot be found in the storage",
          fullFilePath, deltaTableLocation);
    }

    MetricsConfig metricsConfig = MetricsConfig.forTable(transaction.table());
    String nameMappingString =
        transaction.table().properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    // TODO read metrics from Delta log to avoid data flies read
    Metrics metrics = ParquetUtil.fileMetrics(inputDataFile, metricsConfig, nameMapping);

    Map<String, String> partitionValues = addFile.partitionValues();
    PartitionSpec partitionSpec = transaction.table().spec();
    List<String> partitionValueList =
        partitionSpec.fields().stream()
            .map(PartitionField::name)
            .map(partitionValues::get)
            .collect(Collectors.toList());

    return DataFiles.builder(partitionSpec)
        .withPath(fullFilePath)
        .withFormat(FileFormat.PARQUET) // Delta supports only parquet datafiles
        .withFileSizeInBytes(dataFileSize)
        .withMetrics(metrics)
        .withPartitionValues(partitionValueList)
        .build();
  }

  private DataFile buildDataFileFromRemoveDeltaAction(
      DeltaRemoveFile removeFile, Transaction transaction) {
    String path = removeFile.path();
    String fullFilePath = getFullFilePath(path, deltaTable.getPath(deltaEngine));

    InputFile inputDataFile = deltaLakeFileIO.newInputFile(fullFilePath);
    if (!inputDataFile.exists()) {
      throw new NotFoundException(
          "File %s is referenced in the logs of Delta Lake table at %s, but cannot be found in the storage",
          fullFilePath, deltaTableLocation);
    }

    MetricsConfig metricsConfig = MetricsConfig.forTable(transaction.table());
    String nameMappingString =
        transaction.table().properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping nameMapping =
        nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
    Metrics metrics = ParquetUtil.fileMetrics(inputDataFile, metricsConfig, nameMapping);

    Map<String, String> partitionValues = removeFile.partitionValues();
    PartitionSpec partitionSpec = transaction.table().spec();
    List<String> partitionValueList =
        partitionSpec.fields().stream()
            .map(PartitionField::name)
            .map(partitionValues::get)
            .collect(Collectors.toList());

    return DataFiles.builder(partitionSpec)
        .withPath(fullFilePath)
        .withFormat(FileFormat.PARQUET) // Delta supports only parquet datafiles
        .withMetrics(metrics)
        .withFileSizeInBytes(inputDataFile.getLength())
        .withPartitionValues(partitionValueList)
        .build();
  }

  @Nonnull
  private static Schema convertToIcebergSchema(StructType deltaSchema) {
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType();
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  /**
   * Returns the minimal available commit version present in the Delta table log.
   *
   * <p>Note that this method may return a Delta log version that points to data files that no
   * longer exist (e.g. removed by {@code VACUUM}), meaning that this version may not be
   * constructable.
   *
   * @return the earliest available commit version in the Delta log
   */
  private long getLowerBoundAvailableDeltaVersion() {
    try {
      return InternalDeltaKernelUtils.earliestRecreatableCommit(deltaEngine, deltaTableLocation);
    } catch (TableNotFoundException e) {
      throw deltaTableNotFoundException(e);
    }
  }

  private void loadLatestDeltaSnapshot() {
    try {
      this.deltaLatestSnapshot =
          Preconditions.checkNotNull(
              deltaTable.getLatestSnapshot(deltaEngine),
              "The latest Delta table snapshot is unreachable.");

      InternalDeltaKernelUtils.assertSnapshotImpl(deltaLatestSnapshot);
    } catch (TableNotFoundException e) {
      throw deltaTableNotFoundException(e);
    }
  }

  private PartitionSpec buildPartitionSpec(Schema schema, List<String> partitionNames) {
    if (partitionNames.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }

    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    for (String partitionName : partitionNames) {
      builder.identity(partitionName);
    }
    return builder.build();
  }

  private Map<String, String> buildTablePropertiesWithDelta(
      Snapshot deltaSnapshot, String originalLocation) {
    icebergPropertiesBuilder.put(SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE);
    icebergPropertiesBuilder.put(ORIGINAL_LOCATION_PROP, originalLocation);
    icebergPropertiesBuilder.put(CONVERSION_TOOL_PROP, CONVERSION_TOOL_VALUE);
    // Always construct Iceber v3 Table
    icebergPropertiesBuilder.put(TableProperties.FORMAT_VERSION, "3");

    Map<String, String> configuration =
        InternalDeltaKernelUtils.metadataConfiguration(deltaSnapshot);
    icebergPropertiesBuilder.putAll(configuration);

    return icebergPropertiesBuilder.build();
  }

  private void tagCurrentSnapshot(
      long deltaVersion, Long deltaVersionTimestamp, Transaction transaction) {
    long currentSnapshotId = transaction.table().currentSnapshot().snapshotId();

    ManageSnapshots manageSnapshots = transaction.manageSnapshots();
    manageSnapshots.createTag(DELTA_VERSION_TAG_PREFIX + deltaVersion, currentSnapshotId);

    if (deltaVersionTimestamp != null && deltaTimestampTags.add(deltaVersionTimestamp)) {
      // Avoid creating the same timestamp based tag multiple times for small and often writes to
      // source delta table
      manageSnapshots.createTag(
          DELTA_TIMESTAMP_TAG_PREFIX + deltaVersionTimestamp, currentSnapshotId);
    }
    manageSnapshots.commit();
  }

  @Nonnull
  private IllegalArgumentException deltaTableNotFoundException(TableNotFoundException exception) {
    return new IllegalArgumentException(
        String.format(
            "Delta Lake table does not exist at the given location: %s", deltaTableLocation),
        exception);
  }

  private static void assertSupportedDeltaOperation(Long deltaVersion, Row commitInfo) {
    String operation = commitInfo.getString(commitInfo.getSchema().indexOf("operation"));
    if (UNSUPPORTED_DELTA_OPERATIONS.contains(operation)) {
      throw new IllegalStateException(
          String.format(
              java.util.Locale.ROOT,
              "Cannot convert Delta table: schema evolution operation '%s' is not supported (detected at Delta version %d).",
              operation,
              deltaVersion));
    }
  }

  @VisibleForTesting
  static String getFullFilePath(String path, String tableRoot) {
    URI dataFileUri = URI.create(path);
    if (dataFileUri.isAbsolute()) {
      return dataFileUri.getScheme().equalsIgnoreCase("file") ? dataFileUri.getPath() : path;
    } else {
      String decodedPath = dataFileUri.getPath();
      String separator =
          tableRoot.contains(":/")
              ? "/"
              : File.separator; // Cloud Storages path vs File System (windows `\` and other)
      return tableRoot + (tableRoot.endsWith(separator) ? "" : separator) + decodedPath;
    }
  }
}
