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
package org.apache.iceberg.tornadovm;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.LongStream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.tornadovm.bench.MorTables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What real tables have that a freshly written one does not: an evolved schema, partitions, and a
 * home on an object store. Each is read, aggregated and compacted on the device and checked against
 * Iceberg's own reader.
 */
public class TestDeviceEvolutionAndStorage {
  @TempDir private Path temp;

  @BeforeAll
  public static void requireDevice() {
    assumeThat(DeviceMergeOnReadScan.isAvailable()).as("TornadoVM with the cuDF shim").isTrue();
  }

  private static long[] everyNth(long rows, int every) {
    return LongStream.range(0, rows).filter(i -> i % every == 0).toArray();
  }

  /** Iceberg's rows, as strings of the given columns, sorted. */
  private static List<String> icebergRows(Table table, List<String> columns) throws IOException {
    List<String> rows = Lists.newArrayList();
    try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
      for (Record record : records) {
        StringBuilder row = new StringBuilder();
        for (String column : columns) {
          row.append(record.getField(column)).append('|');
        }
        rows.add(row.toString());
      }
    }
    rows.sort(String::compareTo);
    return rows;
  }

  /** The device's rows of id (long), k (long), v (double) and extra (long, nullable). */
  private static List<String> deviceRows(DeviceMergeOnReadScan device) {
    List<String> rows = java.util.Collections.synchronizedList(Lists.newArrayList());
    device.materialize(
        ImmutableList.of("id", "k", "v", "extra"),
        batch -> {
          for (int r = 0; r < batch.rows(); r++) {
            rows.add(
                batch.getLong("id", r)
                    + "|"
                    + (batch.isNull("k", r) ? null : batch.getLong("k", r))
                    + "|"
                    + (batch.isNull("v", r) ? null : batch.getDouble("v", r))
                    + "|"
                    + (batch.isNull("extra", r) ? null : batch.getLong("extra", r))
                    + "|");
          }
        });
    List<String> sorted = Lists.newArrayList(rows);
    sorted.sort(String::compareTo);
    return sorted;
  }

  /**
   * Files written before a column was added and k and v were promoted (int to long, float to
   * double) are read alongside files written after: the new column as nulls, the old values
   * widened. Then grouped sums over the promoted columns, and a compaction of it all.
   */
  @Test
  public void testEvolvedSchema() throws IOException {
    Schema before =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "k", Types.IntegerType.get()),
            Types.NestedField.optional(3, "v", Types.FloatType.get()));
    Table table = MorTables.create(temp.resolve("evolved").toString(), before, 2);
    List<DataFile> old = Lists.newArrayList();
    for (long f = 0; f < 2; f++) {
      long index = f;
      old.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                GenericRecord record = GenericRecord.create(before);
                record.setField("id", index * 1_000_000L + i);
                record.setField("k", i % 11 == 0 ? null : (int) (i % 7));
                record.setField("v", (float) i / 4);
                return record;
              }));
    }
    MorTables.append(table, old);

    table
        .updateSchema()
        .addColumn("extra", Types.LongType.get())
        .updateColumn("k", Types.LongType.get())
        .updateColumn("v", Types.DoubleType.get())
        .commit();
    Schema after = table.schema();
    List<DataFile> fresh = Lists.newArrayList();
    for (long f = 2; f < 4; f++) {
      long index = f;
      fresh.add(
          MorTables.writeDataFile(
              table,
              3000,
              i -> {
                GenericRecord record = GenericRecord.create(after);
                record.setField("id", index * 1_000_000L + i);
                record.setField("k", (long) (i % 7) + 5_000_000_000L * (i % 2));
                record.setField("v", i * 0.1);
                record.setField("extra", i % 3 == 0 ? null : index * 10 + i);
                return record;
              }));
    }
    MorTables.append(table, fresh);
    List<DataFile> all = Lists.newArrayList(old);
    all.addAll(fresh);
    MorTables.deletePositions(
        table,
        all,
        ImmutableList.of(everyNth(3000, 5), new long[0], everyNth(3000, 7), new long[] {1, 2}));
    MorTables.deleteEqual(table, "id", ImmutableList.of(3L, 2_000_004L));
    table.refresh();

    List<String> columns = ImmutableList.of("id", "k", "v", "extra");
    List<String> expected = icebergRows(table, columns);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      assertThat(deviceRows(device)).containsExactlyElementsOf(expected);

      // Sums of the promoted double by the promoted long, against Iceberg's rows.
      Map<Long, double[]> want = new TreeMap<>();
      try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
        for (Record record : records) {
          if (record.getField("k") == null) {
            continue;
          }
          double[] sums =
              want.computeIfAbsent((Long) record.getField("k"), ignored -> new double[2]);
          sums[0] += (Double) record.getField("v");
          sums[1] += 1;
        }
      }
      Map<Long, double[]> got = new TreeMap<>();
      device
          .aggregate(ImmutableList.of("k"), ImmutableList.of(DeviceValue.of("v")))
          .groups()
          .forEach(
              (key, totals) -> {
                if (key.get(0) != null) {
                  got.put((Long) key.get(0), totals);
                }
              });
      assertThat(got.keySet()).containsExactlyElementsOf(want.keySet());
      want.forEach(
          (k, sums) -> {
            assertThat(got.get(k)[0])
                .as("sum %s", k)
                .isCloseTo(sums[0], org.assertj.core.api.Assertions.within(1e-6));
            assertThat(got.get(k)[1]).as("rows %s", k).isEqualTo(sums[1]);
          });

      device.rewriteDataFiles();
    }
    table.refresh();
    assertThat(icebergRows(table, columns)).containsExactlyElementsOf(expected);
  }

  /** A partitioned table compacted on the device: every new file holds one partition's rows. */
  @Test
  public void testPartitionedCompaction() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.IntegerType.get()),
            Types.NestedField.optional(3, "amount", Types.DecimalType.of(9, 2)),
            Types.NestedField.optional(4, "note", Types.StringType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").build();
    Table table =
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                spec,
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"),
                temp.resolve("partitioned").toString());
    List<DataFile> files = Lists.newArrayList();
    for (int region = 0; region < 3; region++) {
      for (int f = 0; f < 2; f++) {
        int regionId = region;
        long base = (region * 10L + f) * 1_000_000L;
        GenericRecord sample = GenericRecord.create(schema);
        sample.setField("region", regionId);
        PartitionKey key = new PartitionKey(spec, schema);
        key.partition(sample);
        files.add(
            MorTables.writeDataFile(
                table,
                key,
                2000,
                i -> {
                  GenericRecord record = GenericRecord.create(schema);
                  record.setField("id", base + i);
                  record.setField("region", regionId);
                  record.setField("amount", java.math.BigDecimal.valueOf(i * 37 % 100_000, 2));
                  record.setField("note", i % 5 == 0 ? null : "region " + regionId + " row " + i);
                  return record;
                }));
      }
    }
    MorTables.append(table, files);
    List<long[]> positions = Lists.newArrayList();
    for (int f = 0; f < files.size(); f++) {
      positions.add(everyNth(2000, 3 + f));
    }
    MorTables.deletePositions(table, files, positions);
    table.refresh();

    List<String> columns = ImmutableList.of("id", "region", "amount", "note");
    List<String> expected = icebergRows(table, columns);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.rewriteDataFiles();
    }
    table.refresh();
    assertThat(icebergRows(table, columns)).containsExactlyElementsOf(expected);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      java.util.Set<Integer> partitions = new java.util.TreeSet<>();
      for (FileScanTask task : tasks) {
        assertThat(task.deletes()).isEmpty();
        int partition = task.file().partition().get(0, Integer.class);
        partitions.add(partition);
        // Every row of the file is in the file's partition.
        try (CloseableIterable<Record> rows =
            org.apache.iceberg.parquet.Parquet.read(table.io().newInputFile(task.file().location()))
                .project(schema)
                .createReaderFunc(
                    fileSchema ->
                        org.apache.iceberg.data.parquet.GenericParquetReaders.buildReader(
                            schema, fileSchema))
                .build()) {
          for (Record row : rows) {
            assertThat(row.getField("region")).isEqualTo(partition);
          }
        }
      }
      assertThat(partitions).as("every partition rewritten").containsExactly(0, 1, 2);
    }
  }

  /**
   * Equality deletes in two partitions of a partitioned v3 table become deletion vectors: only the
   * files they apply to get a vector (their earlier one folded in), each vector carries its file's
   * partition, the third partition is untouched, and the table reads the same rows.
   */
  @Test
  public void testPartitionedConversionToDeletionVectors() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.required(2, "region", Types.IntegerType.get()),
            Types.NestedField.optional(3, "amount", Types.DoubleType.get()));
    PartitionSpec spec = PartitionSpec.builderFor(schema).identity("region").build();
    Table table =
        new org.apache.iceberg.hadoop.HadoopTables(new org.apache.hadoop.conf.Configuration())
            .create(
                schema,
                spec,
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "3"),
                temp.resolve("partitioned-dv").toString());
    List<DataFile> files = Lists.newArrayList();
    List<PartitionKey> keys = Lists.newArrayList();
    for (int region = 0; region < 3; region++) {
      int regionId = region;
      GenericRecord sample = GenericRecord.create(schema);
      sample.setField("region", regionId);
      PartitionKey key = new PartitionKey(spec, schema);
      key.partition(sample);
      keys.add(key);
      for (int f = 0; f < 2; f++) {
        long base = (region * 10L + f) * 1_000_000L;
        files.add(
            MorTables.writeDataFile(
                table,
                key,
                3000,
                i -> {
                  GenericRecord record = GenericRecord.create(schema);
                  record.setField("id", base + i);
                  record.setField("region", regionId);
                  record.setField("amount", i % 7 == 0 ? null : i * 0.5);
                  return record;
                }));
      }
    }
    MorTables.append(table, files);
    List<long[]> positions = Lists.newArrayList();
    for (int f = 0; f < files.size(); f++) {
      positions.add(everyNth(3000, 5 + f));
    }
    MorTables.deletePositions(table, files, positions);
    MorTables.deleteEqual(table, "id", ImmutableList.of(7L, 1_000_008L, 9L), keys.get(0));
    MorTables.deleteEqual(table, "id", ImmutableList.of(20_000_100L, 21_000_101L), keys.get(2));
    table.refresh();
    List<String> columns = ImmutableList.of("id", "region", "amount");
    List<String> expected = icebergRows(table, columns);

    DeviceMergeOnReadScan.ConversionResult result;
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      result = device.convertEqualityDeletes();
    }
    table.refresh();

    assertThat(result.dataFiles()).isEqualTo(4);
    assertThat(result.equalityDeletesRemoved()).isEqualTo(2);
    assertThat(result.positionDeletesReplaced()).isEqualTo(4);
    assertThat(result.vectorsAdded()).isEqualTo(4);
    assertThat(icebergRows(table, columns)).containsExactlyElementsOf(expected);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.deletes()).hasSize(1);
        org.apache.iceberg.DeleteFile vector = task.deletes().get(0);
        assertThat(org.apache.iceberg.util.ContentFileUtil.isDV(vector)).isTrue();
        assertThat(vector.partition().get(0, Integer.class))
            .isEqualTo(task.file().partition().get(0, Integer.class));
      }
    }
  }

  /**
   * A v3 table with string columns compacted on the device: the strings are gathered from the read
   * inside the write, and every row keeps its lineage (_row_id, _last_updated_sequence_number).
   */
  @Test
  public void testV3StringsCompactionKeepsLineage() throws IOException {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "comment", Types.StringType.get()),
            Types.NestedField.required(3, "qty", Types.IntegerType.get()),
            Types.NestedField.optional(4, "mode", Types.StringType.get()));
    Table table = MorTables.create(temp.resolve("v3-strings").toString(), schema, 3);
    List<DataFile> files = Lists.newArrayList();
    String[] modes = {"AIR", "REG AIR", "TRUCK", "Ωméga", null};
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              2500,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", index * 1_000_000L + i);
                record.setField(
                    "comment",
                    i % 7 == 0
                        ? null
                        : "a comment longer than eight bytes " + i + "x".repeat((int) (i % 50)));
                record.setField("qty", (int) (i % 50));
                record.setField("mode", modes[(int) ((i + index) % 5)]);
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(2500, 3), new long[] {0, 10}, everyNth(2500, 8)));
    table.refresh();

    Schema withLineage =
        org.apache.iceberg.types.TypeUtil.join(
            table.schema(),
            new Schema(
                org.apache.iceberg.MetadataColumns.ROW_ID,
                org.apache.iceberg.MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER));
    List<String> before = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).project(withLineage).build()) {
      for (Record record : records) {
        before.add(
            record.getField("id")
                + "|"
                + record.getField("comment")
                + "|"
                + record.getField("qty")
                + "|"
                + record.getField("mode")
                + "|"
                + record.getField("_row_id")
                + "|"
                + record.getField("_last_updated_sequence_number"));
      }
    }
    before.sort(String::compareTo);
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      device.rewriteDataFiles();
    }
    table.refresh();
    List<String> after = Lists.newArrayList();
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(table).project(withLineage).build()) {
      for (Record record : records) {
        after.add(
            record.getField("id")
                + "|"
                + record.getField("comment")
                + "|"
                + record.getField("qty")
                + "|"
                + record.getField("mode")
                + "|"
                + record.getField("_row_id")
                + "|"
                + record.getField("_last_updated_sequence_number"));
      }
    }
    after.sort(String::compareTo);
    assertThat(after).containsExactlyElementsOf(before);
  }

  /**
   * A table on an object store -- Iceberg's in-memory catalog and FileIO, whose locations are not
   * local paths: its files are fetched to the local cache to read, and a compaction's files are
   * uploaded back.
   */
  @Test
  public void testObjectStoreTable() throws IOException {
    InMemoryCatalog catalog = new InMemoryCatalog();
    catalog.initialize("mem", ImmutableMap.of("warehouse", "mem://warehouse"));
    catalog.createNamespace(Namespace.of("db"));
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "k", Types.LongType.get()),
            Types.NestedField.optional(3, "v", Types.DoubleType.get()),
            Types.NestedField.optional(4, "extra", Types.LongType.get()),
            Types.NestedField.optional(5, "name", Types.StringType.get()));
    Table table =
        catalog.createTable(
            TableIdentifier.of("db", "remote"),
            schema,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.FORMAT_VERSION, "2"));
    assertThat(table.location()).startsWith("mem://");
    List<DataFile> files = Lists.newArrayList();
    for (long f = 0; f < 3; f++) {
      long index = f;
      files.add(
          MorTables.writeDataFile(
              table,
              2500,
              i -> {
                GenericRecord record = GenericRecord.create(schema);
                record.setField("id", index * 1_000_000L + i);
                record.setField("k", i % 9);
                record.setField("v", i * 0.5);
                record.setField("extra", i % 4 == 0 ? null : i);
                record.setField(
                    "name", i % 6 == 0 ? null : "n-" + i + "-Ωé-" + "x".repeat((int) (i % 30)));
                return record;
              }));
    }
    MorTables.append(table, files);
    MorTables.deletePositions(
        table, files, ImmutableList.of(everyNth(2500, 4), new long[] {0, 1}, new long[0]));
    MorTables.deleteEqual(table, "id", ImmutableList.of(9L, 1_000_010L));
    table.refresh();

    List<String> columns = ImmutableList.of("id", "k", "v", "extra");
    List<String> expected = icebergRows(table, columns);
    List<String> withNames = icebergRows(table, ImmutableList.of("id", "k", "v", "extra", "name"));
    try (DeviceMergeOnReadScan device = DeviceMergeOnReadScan.of(table)) {
      assertThat(deviceRows(device)).containsExactlyElementsOf(expected);
      device.rewriteDataFiles();
    }
    table.refresh();
    assertThat(icebergRows(table, ImmutableList.of("id", "k", "v", "extra", "name")))
        .containsExactlyElementsOf(withNames);
    table.refresh();
    assertThat(icebergRows(table, columns)).containsExactlyElementsOf(expected);
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertThat(task.file().location()).startsWith("mem://");
        assertThat(task.deletes()).isEmpty();
      }
    }
  }
}
