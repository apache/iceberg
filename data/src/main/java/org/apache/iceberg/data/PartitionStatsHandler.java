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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ImmutableGenericPartitionStatisticsFile;
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

/**
 * Computes, writes and reads the {@link PartitionStatisticsFile}. Uses generic readers and writers
 * to support writing and reading of the stats in table default format.
 */
public class PartitionStatsHandler {

  private PartitionStatsHandler() {}

  public enum Column {
    PARTITION(0),
    SPEC_ID(1),
    DATA_RECORD_COUNT(2),
    DATA_FILE_COUNT(3),
    TOTAL_DATA_FILE_SIZE_IN_BYTES(4),
    POSITION_DELETE_RECORD_COUNT(5),
    POSITION_DELETE_FILE_COUNT(6),
    EQUALITY_DELETE_RECORD_COUNT(7),
    EQUALITY_DELETE_FILE_COUNT(8),
    TOTAL_RECORD_COUNT(9),
    LAST_UPDATED_AT(10),
    LAST_UPDATED_SNAPSHOT_ID(11);

    private final int id;

    Column(int id) {
      this.id = id;
    }

    public int id() {
      return id;
    }
  }

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
        NestedField.required(1, Column.PARTITION.name(), unifiedPartitionType),
        NestedField.required(2, Column.SPEC_ID.name(), IntegerType.get()),
        NestedField.required(3, Column.DATA_RECORD_COUNT.name(), LongType.get()),
        NestedField.required(4, Column.DATA_FILE_COUNT.name(), IntegerType.get()),
        NestedField.required(5, Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.name(), LongType.get()),
        NestedField.optional(6, Column.POSITION_DELETE_RECORD_COUNT.name(), LongType.get()),
        NestedField.optional(7, Column.POSITION_DELETE_FILE_COUNT.name(), IntegerType.get()),
        NestedField.optional(8, Column.EQUALITY_DELETE_RECORD_COUNT.name(), LongType.get()),
        NestedField.optional(9, Column.EQUALITY_DELETE_FILE_COUNT.name(), IntegerType.get()),
        NestedField.optional(10, Column.TOTAL_RECORD_COUNT.name(), LongType.get()),
        NestedField.optional(11, Column.LAST_UPDATED_AT.name(), LongType.get()),
        NestedField.optional(12, Column.LAST_UPDATED_SNAPSHOT_ID.name(), LongType.get()));
  }

  /**
   * Computes and writes the {@link PartitionStatisticsFile} for a given table's current snapshot.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @return {@link PartitionStatisticsFile} for the current snapshot, or null if no statistics are
   *     present.
   */
  public static PartitionStatisticsFile computeAndWriteStatsFile(Table table) throws IOException {
    if (table.currentSnapshot() == null) {
      return null;
    }

    return computeAndWriteStatsFile(table, table.currentSnapshot().snapshotId());
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
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Snapshot not found: %s", snapshotId);

    Collection<PartitionStats> stats = PartitionStatsUtil.computeStats(table, snapshot);
    if (stats.isEmpty()) {
      return null;
    }

    StructType partitionType = Partitioning.partitionType(table);
    List<PartitionStats> sortedStats = PartitionStatsUtil.sortStats(stats, partitionType);
    return writePartitionStatsFile(
        table, snapshot.snapshotId(), schema(partitionType), sortedStats);
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
            record.get(Column.PARTITION.id(), StructLike.class),
            record.get(Column.SPEC_ID.id(), Integer.class));
    stats.set(Column.DATA_RECORD_COUNT.id(), record.get(Column.DATA_RECORD_COUNT.id(), Long.class));
    stats.set(Column.DATA_FILE_COUNT.id(), record.get(Column.DATA_FILE_COUNT.id(), Integer.class));
    stats.set(
        Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.id(),
        record.get(Column.TOTAL_DATA_FILE_SIZE_IN_BYTES.id(), Long.class));
    stats.set(
        Column.POSITION_DELETE_RECORD_COUNT.id(),
        record.get(Column.POSITION_DELETE_RECORD_COUNT.id(), Long.class));
    stats.set(
        Column.POSITION_DELETE_FILE_COUNT.id(),
        record.get(Column.POSITION_DELETE_FILE_COUNT.id(), Integer.class));
    stats.set(
        Column.EQUALITY_DELETE_RECORD_COUNT.id(),
        record.get(Column.EQUALITY_DELETE_RECORD_COUNT.id(), Long.class));
    stats.set(
        Column.EQUALITY_DELETE_FILE_COUNT.id(),
        record.get(Column.EQUALITY_DELETE_FILE_COUNT.id(), Integer.class));
    stats.set(
        Column.TOTAL_RECORD_COUNT.id(), record.get(Column.TOTAL_RECORD_COUNT.id(), Long.class));
    stats.set(Column.LAST_UPDATED_AT.id(), record.get(Column.LAST_UPDATED_AT.id(), Long.class));
    stats.set(
        Column.LAST_UPDATED_SNAPSHOT_ID.id(),
        record.get(Column.LAST_UPDATED_SNAPSHOT_ID.id(), Long.class));
    return stats;
  }
}
