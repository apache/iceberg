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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.PartitionMap;
import org.apache.iceberg.util.PartitionUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Computes, writes and reads the {@link PartitionStatisticsFile}. Uses generic readers and writers
 * to support writing and reading of the stats in table default format.
 */
public class PartitionStatsHandler {

  private PartitionStatsHandler() {}

  private static final Logger LOG = LoggerFactory.getLogger(PartitionStatsHandler.class);

  // schema of the partition stats file as per spec
  public static final int PARTITION_FIELD_ID = 1;
  public static final String PARTITION_FIELD_NAME = "partition";
  public static final NestedField SPEC_ID = NestedField.required(2, "spec_id", IntegerType.get());
  public static final NestedField DATA_RECORD_COUNT =
      NestedField.required(3, "data_record_count", LongType.get());
  public static final NestedField DATA_FILE_COUNT =
      NestedField.required(4, "data_file_count", IntegerType.get());
  public static final NestedField TOTAL_DATA_FILE_SIZE_IN_BYTES =
      NestedField.required(5, "total_data_file_size_in_bytes", LongType.get());
  public static final NestedField POSITION_DELETE_RECORD_COUNT =
      NestedField.optional(6, "position_delete_record_count", LongType.get());
  public static final NestedField POSITION_DELETE_FILE_COUNT =
      NestedField.optional(7, "position_delete_file_count", IntegerType.get());
  public static final NestedField EQUALITY_DELETE_RECORD_COUNT =
      NestedField.optional(8, "equality_delete_record_count", LongType.get());
  public static final NestedField EQUALITY_DELETE_FILE_COUNT =
      NestedField.optional(9, "equality_delete_file_count", IntegerType.get());
  public static final NestedField TOTAL_RECORD_COUNT =
      NestedField.optional(10, "total_record_count", LongType.get());
  public static final NestedField LAST_UPDATED_AT =
      NestedField.optional(11, "last_updated_at", LongType.get());
  public static final NestedField LAST_UPDATED_SNAPSHOT_ID =
      NestedField.optional(12, "last_updated_snapshot_id", LongType.get());
  // Using default value for v3 field to support v3 reader reading file written by v2
  public static final NestedField DV_COUNT =
      NestedField.required("dv_count")
          .withId(13)
          .ofType(Types.IntegerType.get())
          .withInitialDefault(Literal.of(0))
          .withWriteDefault(Literal.of(0))
          .build();

  /**
   * Generates the partition stats file schema based on a combined partition type which considers
   * all specs in a table.
   *
   * <p>Use this only for format version 1 and 2. For version 3 and above use {@link
   * #schema(StructType, int)}
   *
   * @param unifiedPartitionType unified partition schema type. Could be calculated by {@link
   *     Partitioning#partitionType(Table)}.
   * @return a schema that corresponds to the provided unified partition type.
   * @deprecated since 1.10.0, will be removed in 1.11.0. Use {@link #schema(StructType, int)}
   *     instead.
   */
  @Deprecated
  public static Schema schema(StructType unifiedPartitionType) {
    Preconditions.checkState(!unifiedPartitionType.fields().isEmpty(), "Table must be partitioned");
    return v2Schema(unifiedPartitionType);
  }

  /**
   * Generates the partition stats file schema for a given format version based on a combined
   * partition type which considers all specs in a table.
   *
   * @param unifiedPartitionType unified partition schema type. Could be calculated by {@link
   *     Partitioning#partitionType(Table)}.
   * @return a schema that corresponds to the provided unified partition type.
   */
  public static Schema schema(StructType unifiedPartitionType, int formatVersion) {
    Preconditions.checkState(!unifiedPartitionType.fields().isEmpty(), "Table must be partitioned");
    Preconditions.checkState(
        formatVersion > 0 && formatVersion <= TableMetadata.SUPPORTED_TABLE_FORMAT_VERSION,
        "Invalid format version: %d",
        formatVersion);

    if (formatVersion <= 2) {
      return v2Schema(unifiedPartitionType);
    }

    return v3Schema(unifiedPartitionType);
  }

  private static Schema v2Schema(StructType unifiedPartitionType) {
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

  private static Schema v3Schema(StructType unifiedPartitionType) {
    return new Schema(
        NestedField.required(PARTITION_FIELD_ID, PARTITION_FIELD_NAME, unifiedPartitionType),
        SPEC_ID,
        DATA_RECORD_COUNT,
        DATA_FILE_COUNT,
        TOTAL_DATA_FILE_SIZE_IN_BYTES,
        NestedField.required(
            POSITION_DELETE_RECORD_COUNT.fieldId(),
            POSITION_DELETE_RECORD_COUNT.name(),
            LongType.get()),
        NestedField.required(
            POSITION_DELETE_FILE_COUNT.fieldId(),
            POSITION_DELETE_FILE_COUNT.name(),
            IntegerType.get()),
        NestedField.required(
            EQUALITY_DELETE_RECORD_COUNT.fieldId(),
            EQUALITY_DELETE_RECORD_COUNT.name(),
            LongType.get()),
        NestedField.required(
            EQUALITY_DELETE_FILE_COUNT.fieldId(),
            EQUALITY_DELETE_FILE_COUNT.name(),
            IntegerType.get()),
        TOTAL_RECORD_COUNT,
        LAST_UPDATED_AT,
        LAST_UPDATED_SNAPSHOT_ID,
        DV_COUNT);
  }

  /**
   * Computes the stats incrementally after the snapshot that has partition stats file till the
   * current snapshot and writes the combined result into a {@link PartitionStatisticsFile} after
   * merging the stats for a given table's current snapshot.
   *
   * <p>Does a full compute if previous statistics file does not exist.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @return {@link PartitionStatisticsFile} for the current snapshot, or null if no statistics are
   *     present.
   */
  public static PartitionStatisticsFile computeAndWriteStatsFile(Table table) throws IOException {
    Preconditions.checkArgument(table != null, "Invalid table: null");

    if (table.currentSnapshot() == null) {
      return null;
    }

    return computeAndWriteStatsFile(table, table.currentSnapshot().snapshotId());
  }

  /**
   * Computes the stats incrementally after the snapshot that has partition stats file till the
   * given snapshot and writes the combined result into a {@link PartitionStatisticsFile} after
   * merging the stats for a given snapshot.
   *
   * <p>Does a full compute if previous statistics file does not exist.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @param snapshotId snapshot for which partition statistics are computed.
   * @return {@link PartitionStatisticsFile} for the given snapshot, or null if no statistics are
   *     present.
   */
  @SuppressWarnings("CatchBlockLogException")
  public static PartitionStatisticsFile computeAndWriteStatsFile(Table table, long snapshotId)
      throws IOException {
    Preconditions.checkArgument(table != null, "Invalid table: null");
    Preconditions.checkArgument(Partitioning.isPartitioned(table), "Table must be partitioned");
    Snapshot snapshot = table.snapshot(snapshotId);
    Preconditions.checkArgument(snapshot != null, "Snapshot not found: %s", snapshotId);

    StructType partitionType = Partitioning.partitionType(table);

    Collection<PartitionStats> stats;
    PartitionStatisticsFile statisticsFile = latestStatsFile(table, snapshot.snapshotId());
    if (statisticsFile == null) {
      LOG.info(
          "Using full compute as previous statistics file is not present for incremental compute.");
      stats =
          computeStats(table, snapshot.allManifests(table.io()), false /* incremental */).values();
    } else {
      if (statisticsFile.snapshotId() == snapshotId) {
        // no-op
        LOG.info("Returning existing statistics file for snapshot {}", snapshotId);
        return statisticsFile;
      }

      try {
        stats = computeAndMergeStatsIncremental(table, snapshot, partitionType, statisticsFile);
      } catch (InvalidStatsFileException exception) {
        LOG.warn(
            "Using full compute as previous statistics file is corrupted for incremental compute.");
        stats =
            computeStats(table, snapshot.allManifests(table.io()), false /* incremental */)
                .values();
      }
    }

    if (stats.isEmpty()) {
      // empty branch case
      return null;
    }

    List<PartitionStats> sortedStats = sortStatsByPartition(stats, partitionType);
    return writePartitionStatsFile(
        table,
        snapshot.snapshotId(),
        schema(partitionType, TableUtil.formatVersion(table)),
        sortedStats);
  }

  @VisibleForTesting
  static PartitionStatisticsFile writePartitionStatsFile(
      Table table, long snapshotId, Schema dataSchema, Iterable<PartitionStats> records)
      throws IOException {
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));

    OutputFile outputFile = newPartitionStatsFile(table, fileFormat, snapshotId);

    try (FileAppender<StructLike> writer =
        InternalData.write(fileFormat, outputFile).schema(dataSchema).build()) {
      records.iterator().forEachRemaining(writer::add);
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
    Preconditions.checkArgument(schema != null, "Invalid schema: null");
    Preconditions.checkArgument(inputFile != null, "Invalid input file: null");

    FileFormat fileFormat = FileFormat.fromFileName(inputFile.location());
    Preconditions.checkArgument(
        fileFormat != null, "Unable to determine format of file: %s", inputFile.location());

    CloseableIterable<StructLike> records =
        InternalData.read(fileFormat, inputFile).project(schema).build();
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

  private static PartitionStats recordToPartitionStats(StructLike record) {
    int pos = 0;
    PartitionStats stats =
        new PartitionStats(
            record.get(pos++, StructLike.class), // partition
            record.get(pos++, Integer.class)); // spec id
    for (; pos < record.size(); pos++) {
      stats.set(pos, record.get(pos, Object.class));
    }

    return stats;
  }

  private static Collection<PartitionStats> computeAndMergeStatsIncremental(
      Table table,
      Snapshot snapshot,
      StructType partitionType,
      PartitionStatisticsFile previousStatsFile) {
    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    // read previous stats, note that partition field will be read as GenericRecord
    try (CloseableIterable<PartitionStats> oldStats =
        readPartitionStatsFile(
            schema(partitionType, TableUtil.formatVersion(table)),
            table.io().newInputFile(previousStatsFile.path()))) {
      oldStats.forEach(
          partitionStats ->
              statsMap.put(partitionStats.specId(), partitionStats.partition(), partitionStats));
    } catch (Exception exception) {
      throw new InvalidStatsFileException(exception);
    }

    // incrementally compute the new stats, partition field will be written as PartitionData
    PartitionMap<PartitionStats> incrementalStatsMap =
        computeStatsDiff(table, table.snapshot(previousStatsFile.snapshotId()), snapshot);

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

    return statsMap.values();
  }

  private static GenericRecord partitionDataToRecord(PartitionData data) {
    GenericRecord record = GenericRecord.create(data.getPartitionType());
    for (int index = 0; index < record.size(); index++) {
      record.set(index, data.get(index));
    }

    return record;
  }

  @VisibleForTesting
  static PartitionStatisticsFile latestStatsFile(Table table, long snapshotId) {
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

    // A stats file exists but isn't accessible from the current snapshot chain.
    // It may belong to a different snapshot reference (like a branch or tag).
    // Falling back to full computation for current snapshot.
    return null;
  }

  private static PartitionMap<PartitionStats> computeStatsDiff(
      Table table, Snapshot fromSnapshot, Snapshot toSnapshot) {
    Iterable<Snapshot> snapshots =
        SnapshotUtil.ancestorsBetween(
            toSnapshot.snapshotId(), fromSnapshot.snapshotId(), table::snapshot);
    // DELETED manifest entries are not carried over to subsequent snapshots.
    // So, for incremental computation, gather the manifests added by each snapshot
    // instead of relying solely on those from the latest snapshot.
    List<ManifestFile> manifests =
        StreamSupport.stream(snapshots.spliterator(), false)
            .flatMap(
                snapshot ->
                    snapshot.allManifests(table.io()).stream()
                        .filter(file -> file.snapshotId().equals(snapshot.snapshotId())))
            .collect(Collectors.toList());

    return computeStats(table, manifests, true /* incremental */);
  }

  private static PartitionMap<PartitionStats> computeStats(
      Table table, List<ManifestFile> manifests, boolean incremental) {
    StructType partitionType = Partitioning.partitionType(table);
    Queue<PartitionMap<PartitionStats>> statsByManifest = Queues.newConcurrentLinkedQueue();
    Tasks.foreach(manifests)
        .stopOnFailure()
        .throwFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .run(
            manifest ->
                statsByManifest.add(
                    collectStatsForManifest(table, manifest, partitionType, incremental)));

    PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
    for (PartitionMap<PartitionStats> stats : statsByManifest) {
      mergePartitionMap(stats, statsMap);
    }

    return statsMap;
  }

  private static PartitionMap<PartitionStats> collectStatsForManifest(
      Table table, ManifestFile manifest, StructType partitionType, boolean incremental) {
    List<String> projection = BaseScan.scanColumns(manifest.content());
    try (ManifestReader<?> reader = ManifestFiles.open(manifest, table.io()).select(projection)) {
      PartitionMap<PartitionStats> statsMap = PartitionMap.create(table.specs());
      int specId = manifest.partitionSpecId();
      PartitionSpec spec = table.specs().get(specId);
      PartitionData keyTemplate = new PartitionData(partitionType);

      for (ManifestEntry<?> entry : reader.entries()) {
        ContentFile<?> file = entry.file();
        StructLike coercedPartition =
            PartitionUtil.coercePartition(partitionType, spec, file.partition());
        StructLike key = keyTemplate.copyFor(coercedPartition);
        Snapshot snapshot = table.snapshot(entry.snapshotId());
        PartitionStats stats =
            statsMap.computeIfAbsent(
                specId,
                ((PartitionData) file.partition()).copy(),
                () -> new PartitionStats(key, specId));
        if (entry.isLive()) {
          // Live can have both added and existing entries. Consider only added entries for
          // incremental compute as existing entries was already included in previous compute.
          if (!incremental || entry.status() == ManifestEntry.Status.ADDED) {
            stats.liveEntry(file, snapshot);
          }
        } else {
          if (incremental) {
            stats.deletedEntryForIncrementalCompute(file, snapshot);
          } else {
            stats.deletedEntry(snapshot);
          }
        }
      }

      return statsMap;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static void mergePartitionMap(
      PartitionMap<PartitionStats> fromMap, PartitionMap<PartitionStats> toMap) {
    fromMap.forEach(
        (key, value) ->
            toMap.merge(
                key,
                value,
                (existingEntry, newEntry) -> {
                  existingEntry.appendStats(newEntry);
                  return existingEntry;
                }));
  }

  private static List<PartitionStats> sortStatsByPartition(
      Collection<PartitionStats> stats, StructType partitionType) {
    List<PartitionStats> entries = Lists.newArrayList(stats);
    entries.sort(
        Comparator.comparing(PartitionStats::partition, Comparators.forType(partitionType)));
    return entries;
  }

  private static class InvalidStatsFileException extends RuntimeException {

    InvalidStatsFileException(Throwable cause) {
      super(cause);
    }
  }
}
