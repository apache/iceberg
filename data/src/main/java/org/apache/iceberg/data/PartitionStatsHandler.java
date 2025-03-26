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
package org.apache.iceberg.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStats;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.InternalReader;
import org.apache.iceberg.data.parquet.InternalWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.SnapshotUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes, writes and reads the {@link PartitionStatisticsFile}. Uses generic readers and writers
 * to support writing and reading of the stats in table default format.
 */
public class PartitionStatsHandler {

  private PartitionStatsHandler() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsHandler.class);

  public static final int PARTITION_FIELD_ID = 0;
  public static final String PARTITION_FIELD_NAME = "partition";
  public static final NestedField SPEC_ID = NestedField.required(1, "spec_id", IntegerType.get());
  public static final NestedField DATA_RECORD_COUNT =
      NestedField.required(2, "data_record_count", LongType.get());
  public static final NestedField DATA_FILE_COUNT =
      NestedField.required(3, "data_file_count", IntegerType.get());
  public static final NestedField TOTAL_DATA_FILE_SIZE_IN_BYTES =
      NestedField.required(4, "total_data_file_size_in_bytes", LongType.get());
  public static final NestedField POSITION_DELETE_RECORD_COUNT =
      NestedField.optional(5, "position_delete_record_count", LongType.get());
  public static final NestedField POSITION_DELETE_FILE_COUNT =
      NestedField.optional(6, "position_delete_file_count", IntegerType.get());
  public static final NestedField EQUALITY_DELETE_RECORD_COUNT =
      NestedField.optional(7, "equality_delete_record_count", LongType.get());
  public static final NestedField EQUALITY_DELETE_FILE_COUNT =
      NestedField.optional(8, "equality_delete_file_count", IntegerType.get());
  public static final NestedField TOTAL_RECORD_COUNT =
      NestedField.optional(9, "total_record_count", LongType.get());
  public static final NestedField LAST_UPDATED_AT =
      NestedField.optional(10, "last_updated_at", LongType.get());
  public static final NestedField LAST_UPDATED_SNAPSHOT_ID =
      NestedField.optional(11, "last_updated_snapshot_id", LongType.get());

  /**
   * Generates the partition stats file schema based on a combined partition type which considers
   * all specs in a table.
   *
   * @param unifiedPartitionType unified partition schema type. Could be calculated by {@link
   *     Partitioning#partitionType(Table)}.
   * @return a schema that corresponds to the provided unified partition type.
   */
  public static Schema schema(StructType unifiedPartitionType) {
    Preconditions.checkState(!unifiedPartitionType.fields().isEmpty(), "Table must be partitioned");
    return new Schema(
        NestedField.required(PARTITION_FIELD_ID, PARTITION_FIELD_NAME, unifiedPartitionType),
        SPEC_ID,
        DATA_RECORD_COUNT,
        DATA_FILE_COUNT,
        TOTAL_DATA_FILE_SIZE_IN_BYTES,
        POSITION_DELETE_RECORD_COUNT,
        POSITION_DELETE_FILE_COUNT,
        EQUALITY_DELETE_RECORD_COUNT,
        EQUALITY_DELETE_FILE_COUNT,
        TOTAL_RECORD_COUNT,
        LAST_UPDATED_AT,
        LAST_UPDATED_SNAPSHOT_ID);
  }

  /**
   * Computes and writes the {@link PartitionStatisticsFile} for a given table's current snapshot.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @return {@link PartitionStatisticsFile} for the current snapshot, or null if no statistics are
   *     present.
   */
  public static PartitionStatisticsFile computeAndWriteStatsFile(Table table) throws IOException {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    if (table.currentSnapshot() == null) {
      return null;
    }

    return computeAndWrite(table, table.currentSnapshot().snapshotId(), true /* recompute */);
  }

  /**
   * Computes and writes the {@link PartitionStatisticsFile} for a given table and snapshot.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @param snapshotId snapshot for which partition statistics are computed.
   * @return {@link PartitionStatisticsFile} for the given snapshot, or null if no statistics are
   *     present.
   */
  public static PartitionStatisticsFile computeAndWriteStatsFile(Table table, long snapshotId)
      throws IOException {
    return computeAndWrite(table, snapshotId, true /* recompute */);
  }

  /**
   * Incrementally computes the stats after the snapshot that has partition stats file till the
   * given snapshot and writes the combined result into a {@link PartitionStatisticsFile} after
   * merging the stats.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @param snapshotId snapshot for which partition statistics are computed.
   * @return {@link PartitionStatisticsFile} for the given snapshot, or null if no statistics are
   *     present.
   */
  public static PartitionStatisticsFile computeAndWriteStatsFileIncremental(
      Table table, long snapshotId) throws IOException {
    return computeAndWrite(table, snapshotId, false /* recompute */);
  }

  private static PartitionStatisticsFile computeAndWrite(
      Table table, long snapshotId, boolean recompute) throws IOException {
    Preconditions.checkArgument(table != null, "Table cannot be null");
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Snapshot not found: %s", snapshotId);

    StructType partitionType = Partitioning.partitionType(table);
    PartitionMap<PartitionStats> resultStatsMap =
        computeStats(table, snapshot, partitionType, recompute);
    if (resultStatsMap.isEmpty()) {
      return null;
    }

    List<PartitionStats> sortedStats =
        PartitionStatsUtil.sortStats(resultStatsMap.values(), partitionType);
    return writePartitionStatsFile(
        table, snapshot.snapshotId(), schema(partitionType), sortedStats);
  }

  private static PartitionMap<PartitionStats> computeStats(
      Table table, Snapshot snapshot, StructType partitionType, boolean recompute)
      throws IOException {
    if (recompute) {
      return PartitionStatsUtil.computeStats(table, null, snapshot);
    }

    PartitionStatisticsFile statisticsFile = latestStatsFile(table, snapshot.snapshotId());
    if (statisticsFile == null) {
      LOG.info("Previous stats not found. Computing the stats for whole table.");
      return PartitionStatsUtil.computeStats(table, null, snapshot);
    }

    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    // read previous stats, note that partition field will be read as GenericRecord
    try (CloseableIterable<PartitionStats> oldStats =
        readPartitionStatsFile(schema(partitionType), Files.localInput(statisticsFile.path()))) {
      oldStats.forEach(
          partitionStats ->
              statsMap.put(partitionStats.specId(), partitionStats.partition(), partitionStats));
    }

    // incrementally compute the new stats, partition field will be written as PartitionData
    PartitionMap<PartitionStats> incrementalStatsMap =
        PartitionStatsUtil.computeStats(
            table, table.snapshot(statisticsFile.snapshotId()), snapshot);

    // convert PartitionData into GenericRecord and merge stats
    incrementalStatsMap.forEach(
        (key, value) ->
            statsMap.merge(
                Pair.of(key.first(), partitionDataToRecord((PartitionData) key.second())),
                value,
                (existingEntry, newEntry) -> {
                  existingEntry.appendStats(newEntry);
                  return existingEntry;
                }));

    return statsMap;
  }

  private static GenericRecord partitionDataToRecord(PartitionData data) {
    GenericRecord record = GenericRecord.create(data.getPartitionType());
    for (int index = 0; index < record.size(); index++) {
      record.set(index, data.get(index));
    }

    return record;
  }

  private static PartitionStatisticsFile latestStatsFile(Table table, long snapshotId) {
    List<PartitionStatisticsFile> partitionStatisticsFiles = table.partitionStatisticsFiles();
    if (partitionStatisticsFiles.isEmpty()) {
      return null;
    }

    Map<Long, PartitionStatisticsFile> stats =
        partitionStatisticsFiles.stream()
            .collect(Collectors.toMap(PartitionStatisticsFile::snapshotId, file -> file));
    for (Snapshot snapshot : SnapshotUtil.ancestorsOf(snapshotId, table::snapshot)) {
      if (stats.containsKey(snapshot.snapshotId())) {
        return stats.get(snapshot.snapshotId());
      }
    }

    throw new RuntimeException(
        "Unable to find the latest stats for snapshot history of " + snapshotId);
  }

  @VisibleForTesting
  static PartitionStatisticsFile writePartitionStatsFile(
      Table table, long snapshotId, Schema dataSchema, Iterable<PartitionStats> records)
      throws IOException {
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));

    OutputFile outputFile = newPartitionStatsFile(table, fileFormat, snapshotId);

    try (DataWriter<StructLike> writer = dataWriter(dataSchema, outputFile, fileFormat)) {
      records.iterator().forEachRemaining(writer::write);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  /**
   * Reads partition statistics from the specified {@link InputFile} using given schema.
   *
   * @param schema The {@link Schema} of the partition statistics file.
   * @param inputFile An {@link InputFile} pointing to the partition stats file.
   */
  public static CloseableIterable<PartitionStats> readPartitionStatsFile(
      Schema schema, InputFile inputFile) {
    CloseableIterable<StructLike> records = dataReader(schema, inputFile);
    return CloseableIterable.transform(records, PartitionStatsHandler::recordToPartitionStats);
  }

  private static OutputFile newPartitionStatsFile(
      Table table, FileFormat fileFormat, long snapshotId) {
    Preconditions.checkArgument(
        table instanceof HasTableOperations,
        "Table must have operations to retrieve metadata location");

    return table
        .io()
        .newOutputFile(
            ((HasTableOperations) table)
                .operations()
                .metadataFileLocation(
                    fileFormat.addExtension(
                        String.format(
                            Locale.ROOT, "partition-stats-%d-%s", snapshotId, UUID.randomUUID()))));
  }

  private static DataWriter<StructLike> dataWriter(
      Schema dataSchema, OutputFile outputFile, FileFormat fileFormat) throws IOException {
    switch (fileFormat) {
      case PARQUET:
        return Parquet.writeData(outputFile)
            .schema(dataSchema)
            .createWriterFunc(InternalWriter::createWriter)
            .withSpec(PartitionSpec.unpartitioned())
            .build();
      case AVRO:
        return Avro.writeData(outputFile)
            .schema(dataSchema)
            .createWriterFunc(org.apache.iceberg.avro.InternalWriter::create)
            .withSpec(PartitionSpec.unpartitioned())
            .build();
      case ORC:
        // Internal writers are not supported for ORC yet.
      default:
        throw new UnsupportedOperationException("Unsupported file format:" + fileFormat.name());
    }
  }

  private static CloseableIterable<StructLike> dataReader(Schema schema, InputFile inputFile) {
    FileFormat fileFormat = FileFormat.fromFileName(inputFile.location());
    Preconditions.checkArgument(
        fileFormat != null, "Unable to determine format of file: %s", inputFile.location());

    switch (fileFormat) {
      case PARQUET:
        return Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(
                fileSchema ->
                    org.apache.iceberg.data.parquet.InternalReader.create(schema, fileSchema))
            .build();
      case AVRO:
        return Avro.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> InternalReader.create(schema))
            .build();
      case ORC:
        // Internal readers are not supported for ORC yet.
      default:
        throw new UnsupportedOperationException("Unsupported file format:" + fileFormat.name());
    }
  }

  private static PartitionStats recordToPartitionStats(StructLike record) {
    PartitionStats stats =
        new PartitionStats(
            record.get(PARTITION_FIELD_ID, StructLike.class),
            record.get(SPEC_ID.fieldId(), Integer.class));
    stats.set(DATA_RECORD_COUNT.fieldId(), record.get(DATA_RECORD_COUNT.fieldId(), Long.class));
    stats.set(DATA_FILE_COUNT.fieldId(), record.get(DATA_FILE_COUNT.fieldId(), Integer.class));
    stats.set(
        TOTAL_DATA_FILE_SIZE_IN_BYTES.fieldId(),
        record.get(TOTAL_DATA_FILE_SIZE_IN_BYTES.fieldId(), Long.class));
    stats.set(
        POSITION_DELETE_RECORD_COUNT.fieldId(),
        record.get(POSITION_DELETE_RECORD_COUNT.fieldId(), Long.class));
    stats.set(
        POSITION_DELETE_FILE_COUNT.fieldId(),
        record.get(POSITION_DELETE_FILE_COUNT.fieldId(), Integer.class));
    stats.set(
        EQUALITY_DELETE_RECORD_COUNT.fieldId(),
        record.get(EQUALITY_DELETE_RECORD_COUNT.fieldId(), Long.class));
    stats.set(
        EQUALITY_DELETE_FILE_COUNT.fieldId(),
        record.get(EQUALITY_DELETE_FILE_COUNT.fieldId(), Integer.class));
    stats.set(TOTAL_RECORD_COUNT.fieldId(), record.get(TOTAL_RECORD_COUNT.fieldId(), Long.class));
    stats.set(LAST_UPDATED_AT.fieldId(), record.get(LAST_UPDATED_AT.fieldId(), Long.class));
    stats.set(
        LAST_UPDATED_SNAPSHOT_ID.fieldId(),
        record.get(LAST_UPDATED_SNAPSHOT_ID.fieldId(), Long.class));
    return stats;
  }
}
