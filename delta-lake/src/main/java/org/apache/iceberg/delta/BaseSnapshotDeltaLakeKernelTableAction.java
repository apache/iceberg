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

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.DeltaHistoryManager;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.fs.Path;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class BaseSnapshotDeltaLakeKernelTableAction implements SnapshotDeltaLakeTable {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseSnapshotDeltaLakeKernelTableAction.class);

  private static final String SNAPSHOT_SOURCE_PROP = "snapshot_source";
  private static final String DELTA_SOURCE_VALUE = "delta";
  private static final String ORIGINAL_LOCATION_PROP = "original_location";
  private static final String DELTA_VERSION_TAG_PREFIX = "delta-version-";
  private static final String DELTA_TIMESTAMP_TAG_PREFIX = "delta-ts-";

  private final ImmutableMap.Builder<String, String> icebergPropertiesBuilder =
      ImmutableMap.builder();

  private final String deltaTableLocation;
  private Engine deltaEngine;
  private TableImpl deltaTable;

  private Catalog icebergCatalog;
  private TableIdentifier newTableIdentifier;
  private String newTableLocation;
  private HadoopFileIO deltaLakeFileIO;

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
    deltaTable = (TableImpl) Table.forPath(deltaEngine, deltaTableLocation);
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

    long latestDeltaVersion = getLatestDeltaSnapshot().getVersion();
    long initialDeltaVersion = getEarliestDeltaLog();

    // the initialDeltaVersion used as a lower bound and the actual snapshot can be at bigger
    // version because of possible concurrent operations. It's ok.
    SnapshotImpl initialDeltaSnapshot = getDeltaSnapshotAsOfVersion(initialDeltaVersion);

    LOG.info(
        "Converting Delta Lake table at {} from version {} to version {} into Iceberg table {} ...",
        deltaTableLocation,
        initialDeltaVersion,
        latestDeltaVersion,
        newTableIdentifier);

    Schema icebergSchema = convertToIcebergSchema(initialDeltaSnapshot.getSchema());
    PartitionSpec partitionSpec =
        buildPartitionSpec(icebergSchema, initialDeltaSnapshot.getPartitionColumnNames());

    Transaction transaction =
        icebergCatalog.newCreateTableTransaction(
            newTableIdentifier,
            icebergSchema,
            partitionSpec,
            newTableLocation,
            buildTablePropertiesWithDelta(initialDeltaSnapshot, deltaTableLocation));
    setDefaultNamingMapping(transaction);

    long totalDataFiles;
    try {
      totalDataFiles =
          convertEachDeltaVersion(initialDeltaVersion, latestDeltaVersion, transaction);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    transaction.commitTransaction();

    LOG.info(
        "Successfully created Iceberg table {} from Delta Lake table at {}, total data file count: {}",
        newTableIdentifier,
        deltaTableLocation,
        totalDataFiles);
    return () -> totalDataFiles;
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

  private SnapshotImpl getDeltaSnapshotAsOfVersion(long earliestDeltaFile) {
    Snapshot snapshot = deltaTable.getSnapshotAsOfVersion(deltaEngine, earliestDeltaFile);
    assertSnapshotImpl(snapshot);
    return (SnapshotImpl) snapshot;
  }

  private long convertEachDeltaVersion(
      long initialDeltaVersion, long latestDeltaVersion, Transaction transaction)
      throws IOException {

    long dataFiles = 0;
    for (long currDeltaVersion = initialDeltaVersion;
        currDeltaVersion <= latestDeltaVersion;
        currDeltaVersion++) {
      try (CloseableIterator<ColumnarBatch> changes =
          deltaTable.getChanges(
              deltaEngine,
              currDeltaVersion,
              currDeltaVersion,
              Set.of(DeltaLogActionUtils.DeltaAction.values()))) {
        while (changes.hasNext()) {
          ColumnarBatch columnarBatch = changes.next();

          dataFiles +=
              commitDeltaColumnarBatchToIcebergTransaction(
                  currDeltaVersion, columnarBatch, transaction);
        }
      }
    }
    return dataFiles;
  }

  /**
   * Convert each delta log {@code ColumnarBatch} to Iceberg action and commit to the given {@code
   * Transaction}. The complete <a
   * href="https://github.com/delta-io/delta/blob/master/PROTOCOL.md#Actions">spec</a> of delta
   * actions. <br>
   * Supported:
   * <li>Add
   *
   * @return number of added data files
   */
  private long commitDeltaColumnarBatchToIcebergTransaction(
      long deltaVersion, ColumnarBatch columnarBatch, Transaction transaction) throws IOException {
    // TODO
    // 1. initial delta version with all the data files
    // 1.1 data skipping stats
    // 1.2 DVs support
    // 2. Delta log to Iceberg history 1 by 1
    // 3. Delta versions and Delta tags
    // DeltaLogActionUtils.readCommitFiles(engine, commitFiles, readSchema);
    // DeltaLogFile
    // io.delta.kernel.internal.util.FileNames.deltaVersion(io.delta.kernel.internal.fs.Path)

    Long commitTimestamp = null;
    List<DataFile> dataFilesToAdd = Lists.newArrayList();
    try (CloseableIterator<Row> rows = columnarBatch.getRows()) {
      while (rows.hasNext()) {
        Row row = rows.next();
        if (DeltaLakeActionsTranslationUtil.isAdd(row)) {
          AddFile addFile = DeltaLakeActionsTranslationUtil.toAdd(row);

          DataFile dataFile = buildDataFileFromDeltaAction(addFile, transaction);
          dataFilesToAdd.add(dataFile);
        } else if (DeltaLakeActionsTranslationUtil.isCommitInfo(row)) {
          Row commitInfo = row.getStruct(row.getSchema().indexOf("commitInfo"));
          commitTimestamp = commitInfo.getLong(commitInfo.getSchema().indexOf("timestamp"));
        }
      }
    }

    // TODO Append only now
    if (!dataFilesToAdd.isEmpty()) {
      AppendFiles appendFiles = transaction.newAppend();
      dataFilesToAdd.forEach(appendFiles::appendFile);
      appendFiles.commit();
    }

    tagCurrentSnapshot(deltaVersion, commitTimestamp, transaction);

    return dataFilesToAdd.size();
  }

  // TODO support more actions
  private DataFile buildDataFileFromDeltaAction(AddFile addFile, Transaction transaction) {
    String path = addFile.getPath();
    long dataFileSize = addFile.getSize();
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

    Map<String, String> partitionValues = VectorUtils.toJavaMap(addFile.getPartitionValues());
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

  @Nonnull
  private static Schema convertToIcebergSchema(StructType deltaSchema) {
    Type converted = new DeltaLakeKernelTypeToType(deltaSchema).convertType();
    return new Schema(converted.asNestedType().asStructType().fields());
  }

  private long getEarliestDeltaLog() {
    try {
      // "_delta_log" is unmodifiable logs location
      return DeltaHistoryManager.getEarliestDeltaFile(
          deltaEngine, new Path(deltaTableLocation, "_delta_log"));
    } catch (TableNotFoundException e) {
      throw deltaTableNotFoundException(e);
    }
  }

  private SnapshotImpl getLatestDeltaSnapshot() {
    Snapshot latestSnapshot;
    try {
      latestSnapshot = deltaTable.getLatestSnapshot(deltaEngine);

      assertSnapshotImpl(latestSnapshot);

      return (SnapshotImpl) latestSnapshot;
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
      SnapshotImpl deltaSnapshot, String originalLocation) {
    icebergPropertiesBuilder.putAll(
        org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap.of(
            SNAPSHOT_SOURCE_PROP, DELTA_SOURCE_VALUE, ORIGINAL_LOCATION_PROP, originalLocation));

    Map<String, String> configuration = deltaSnapshot.getMetadata().getConfiguration();
    icebergPropertiesBuilder.putAll(configuration);

    return icebergPropertiesBuilder.build();
  }

  private void tagCurrentSnapshot(
      long deltaVersion, Long deltaVersionTimestamp, Transaction transaction) {
    long currentSnapshotId = transaction.table().currentSnapshot().snapshotId();

    ManageSnapshots manageSnapshots = transaction.manageSnapshots();
    manageSnapshots.createTag(DELTA_VERSION_TAG_PREFIX + deltaVersion, currentSnapshotId);

    if (deltaVersionTimestamp != null) {
      manageSnapshots.createTag(
          DELTA_TIMESTAMP_TAG_PREFIX + deltaVersionTimestamp, currentSnapshotId);
    }
    manageSnapshots.commit();
  }

  private static void assertSnapshotImpl(Snapshot latestSnapshot) {
    if (!(latestSnapshot instanceof SnapshotImpl)) {
      throw new IllegalStateException(
          "Unsupported impl of delta Snapshot: " + latestSnapshot.getClass());
    }
  }

  @Nonnull
  private IllegalArgumentException deltaTableNotFoundException(TableNotFoundException exception) {
    return new IllegalArgumentException(
        String.format(
            "Delta Lake table does not exist at the given location: %s", deltaTableLocation),
        exception);
  }

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
