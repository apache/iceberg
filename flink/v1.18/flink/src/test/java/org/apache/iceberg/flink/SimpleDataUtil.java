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
package org.apache.iceberg.flink;

import static org.apache.iceberg.hadoop.HadoopOutputFile.fromPath;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.flink.sink.FlinkAppenderFactory;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.apache.iceberg.util.StructLikeWrapper;
import org.awaitility.Awaitility;
import org.junit.Assert;

public class SimpleDataUtil {

  private SimpleDataUtil() {}

  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "data", Types.StringType.get()));

  public static final TableSchema FLINK_SCHEMA =
      TableSchema.builder().field("id", DataTypes.INT()).field("data", DataTypes.STRING()).build();

  public static final RowType ROW_TYPE = (RowType) FLINK_SCHEMA.toRowDataType().getLogicalType();

  public static final Record RECORD = GenericRecord.create(SCHEMA);

  public static Table createTable(
      String path, Map<String, String> properties, boolean partitioned) {
    PartitionSpec spec;
    if (partitioned) {
      spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    } else {
      spec = PartitionSpec.unpartitioned();
    }
    return new HadoopTables().create(SCHEMA, spec, properties, path);
  }

  public static Record createRecord(Integer id, String data) {
    Record record = RECORD.copy();
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  public static RowData createRowData(Integer id, String data) {
    return GenericRowData.of(id, StringData.fromString(data));
  }

  public static RowData createInsert(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.INSERT, id, StringData.fromString(data));
  }

  public static RowData createDelete(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.DELETE, id, StringData.fromString(data));
  }

  public static RowData createUpdateBefore(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.UPDATE_BEFORE, id, StringData.fromString(data));
  }

  public static RowData createUpdateAfter(Integer id, String data) {
    return GenericRowData.ofKind(RowKind.UPDATE_AFTER, id, StringData.fromString(data));
  }

  public static DataFile writeFile(
      Table table,
      Schema schema,
      PartitionSpec spec,
      Configuration conf,
      String location,
      String filename,
      List<RowData> rows)
      throws IOException {
    return writeFile(table, schema, spec, conf, location, filename, rows, null);
  }

  /** Write the list of {@link RowData} to the given path and with the given partition data */
  public static DataFile writeFile(
      Table table,
      Schema schema,
      PartitionSpec spec,
      Configuration conf,
      String location,
      String filename,
      List<RowData> rows,
      StructLike partition)
      throws IOException {
    Path path = new Path(location, filename);
    FileFormat fileFormat = FileFormat.fromFileName(filename);
    Preconditions.checkNotNull(fileFormat, "Cannot determine format for file: %s", filename);

    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    FileAppenderFactory<RowData> appenderFactory =
        new FlinkAppenderFactory(
            table, schema, flinkSchema, ImmutableMap.of(), spec, null, null, null);

    FileAppender<RowData> appender = appenderFactory.newAppender(fromPath(path, conf), fileFormat);
    try (FileAppender<RowData> closeableAppender = appender) {
      closeableAppender.addAll(rows);
    }

    DataFiles.Builder builder =
        DataFiles.builder(spec)
            .withInputFile(HadoopInputFile.fromPath(path, conf))
            .withMetrics(appender.metrics());

    if (partition != null) {
      builder = builder.withPartition(partition);
    }

    return builder.build();
  }

  public static DeleteFile writeEqDeleteFile(
      Table table,
      FileFormat format,
      String filename,
      FileAppenderFactory<RowData> appenderFactory,
      List<RowData> deletes)
      throws IOException {
    EncryptedOutputFile outputFile =
        table
            .encryption()
            .encrypt(fromPath(new Path(table.location(), filename), new Configuration()));

    EqualityDeleteWriter<RowData> eqWriter =
        appenderFactory.newEqDeleteWriter(outputFile, format, null);
    try (EqualityDeleteWriter<RowData> writer = eqWriter) {
      writer.write(deletes);
    }
    return eqWriter.toDeleteFile();
  }

  public static DeleteFile writePosDeleteFile(
      Table table,
      FileFormat format,
      String filename,
      FileAppenderFactory<RowData> appenderFactory,
      List<Pair<CharSequence, Long>> positions)
      throws IOException {
    EncryptedOutputFile outputFile =
        table
            .encryption()
            .encrypt(fromPath(new Path(table.location(), filename), new Configuration()));

    PositionDeleteWriter<RowData> posWriter =
        appenderFactory.newPosDeleteWriter(outputFile, format, null);
    PositionDelete<RowData> posDelete = PositionDelete.create();
    try (PositionDeleteWriter<RowData> writer = posWriter) {
      for (Pair<CharSequence, Long> p : positions) {
        writer.write(posDelete.set(p.first(), p.second(), null));
      }
    }
    return posWriter.toDeleteFile();
  }

  private static List<Record> convertToRecords(List<RowData> rows) {
    List<Record> records = Lists.newArrayList();
    for (RowData row : rows) {
      Integer id = row.isNullAt(0) ? null : row.getInt(0);
      String data = row.isNullAt(1) ? null : row.getString(1).toString();
      records.add(createRecord(id, data));
    }
    return records;
  }

  public static void assertTableRows(String tablePath, List<RowData> expected, String branch)
      throws IOException {
    assertTableRecords(tablePath, convertToRecords(expected), branch);
  }

  public static void assertTableRows(Table table, List<RowData> expected) throws IOException {
    assertTableRecords(table, convertToRecords(expected), SnapshotRef.MAIN_BRANCH);
  }

  public static void assertTableRows(Table table, List<RowData> expected, String branch)
      throws IOException {
    assertTableRecords(table, convertToRecords(expected), branch);
  }

  /** Get all rows for a table */
  public static List<Record> tableRecords(Table table) throws IOException {
    table.refresh();
    List<Record> records = Lists.newArrayList();
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      for (Record record : iterable) {
        records.add(record);
      }
    }
    return records;
  }

  public static boolean equalsRecords(List<Record> expected, List<Record> actual, Schema schema) {
    if (expected.size() != actual.size()) {
      return false;
    }
    Types.StructType type = schema.asStruct();
    StructLikeSet expectedSet = StructLikeSet.create(type);
    expectedSet.addAll(expected);
    StructLikeSet actualSet = StructLikeSet.create(type);
    actualSet.addAll(actual);
    return expectedSet.equals(actualSet);
  }

  public static void assertRecordsEqual(List<Record> expected, List<Record> actual, Schema schema) {
    Assert.assertEquals(expected.size(), actual.size());
    Types.StructType type = schema.asStruct();
    StructLikeSet expectedSet = StructLikeSet.create(type);
    expectedSet.addAll(expected);
    StructLikeSet actualSet = StructLikeSet.create(type);
    actualSet.addAll(actual);
    Assert.assertEquals(expectedSet, actualSet);
  }

  /**
   * Assert table contains the expected list of records after waiting up to the configured {@code
   * timeout}
   */
  public static void assertTableRecords(Table table, List<Record> expected, Duration timeout) {
    Awaitility.await("expected list of records should be produced")
        .atMost(timeout)
        .untilAsserted(
            () -> {
              equalsRecords(expected, tableRecords(table), table.schema());
              assertRecordsEqual(expected, tableRecords(table), table.schema());
            });
  }

  public static void assertTableRecords(Table table, List<Record> expected) throws IOException {
    assertTableRecords(table, expected, SnapshotRef.MAIN_BRANCH);
  }

  public static void assertTableRecords(Table table, List<Record> expected, String branch)
      throws IOException {
    table.refresh();
    Snapshot snapshot = latestSnapshot(table, branch);

    if (snapshot == null) {
      Assert.assertEquals(expected, ImmutableList.of());
      return;
    }

    Types.StructType type = table.schema().asStruct();
    StructLikeSet expectedSet = StructLikeSet.create(type);
    expectedSet.addAll(expected);

    try (CloseableIterable<Record> iterable =
        IcebergGenerics.read(table).useSnapshot(snapshot.snapshotId()).build()) {
      StructLikeSet actualSet = StructLikeSet.create(type);

      for (Record record : iterable) {
        actualSet.add(record);
      }

      Assert.assertEquals("Should produce the expected record", expectedSet, actualSet);
    }
  }

  // Returns the latest snapshot of the given branch in the table
  public static Snapshot latestSnapshot(Table table, String branch) {
    // For the main branch, currentSnapshot() is used to validate that the API behavior has
    // not changed since that was the API used for validation prior to addition of branches.
    if (branch.equals(SnapshotRef.MAIN_BRANCH)) {
      return table.currentSnapshot();
    }

    return table.snapshot(branch);
  }

  public static void assertTableRecords(String tablePath, List<Record> expected)
      throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    assertTableRecords(new HadoopTables().load(tablePath), expected, SnapshotRef.MAIN_BRANCH);
  }

  public static void assertTableRecords(String tablePath, List<Record> expected, String branch)
      throws IOException {
    Preconditions.checkArgument(expected != null, "expected records shouldn't be null");
    assertTableRecords(new HadoopTables().load(tablePath), expected, branch);
  }

  public static StructLikeSet expectedRowSet(Table table, Record... records) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(table.schema().asStruct());
    for (Record record : records) {
      set.add(wrapper.copyFor(record));
    }
    return set;
  }

  public static StructLikeSet actualRowSet(Table table, String... columns) throws IOException {
    return actualRowSet(table, null, columns);
  }

  public static StructLikeSet actualRowSet(Table table, Long snapshotId, String... columns)
      throws IOException {
    table.refresh();
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    InternalRecordWrapper wrapper = new InternalRecordWrapper(table.schema().asStruct());
    try (CloseableIterable<Record> reader =
        IcebergGenerics.read(table)
            .useSnapshot(snapshotId == null ? table.currentSnapshot().snapshotId() : snapshotId)
            .select(columns)
            .build()) {
      reader.forEach(record -> set.add(wrapper.copyFor(record)));
    }
    return set;
  }

  public static List<DataFile> partitionDataFiles(Table table, Map<String, Object> partitionValues)
      throws IOException {
    table.refresh();
    Types.StructType partitionType = table.spec().partitionType();

    Record partitionRecord = GenericRecord.create(partitionType).copy(partitionValues);
    StructLikeWrapper expectedWrapper =
        StructLikeWrapper.forType(partitionType).set(partitionRecord);

    List<DataFile> dataFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> fileScanTasks = table.newScan().planFiles()) {
      for (FileScanTask scanTask : fileScanTasks) {
        StructLikeWrapper wrapper =
            StructLikeWrapper.forType(partitionType).set(scanTask.file().partition());

        if (expectedWrapper.equals(wrapper)) {
          dataFiles.add(scanTask.file());
        }
      }
    }

    return dataFiles;
  }

  public static Map<Long, List<DataFile>> snapshotToDataFiles(Table table) throws IOException {
    table.refresh();

    Map<Long, List<DataFile>> result = Maps.newHashMap();
    Snapshot current = table.currentSnapshot();
    while (current != null) {
      TableScan tableScan = table.newScan();
      if (current.parentId() != null) {
        // Collect the data files that was added only in current snapshot.
        tableScan = tableScan.appendsBetween(current.parentId(), current.snapshotId());
      } else {
        // Collect the data files that was added in the oldest snapshot.
        tableScan = tableScan.useSnapshot(current.snapshotId());
      }
      try (CloseableIterable<FileScanTask> scanTasks = tableScan.planFiles()) {
        result.put(
            current.snapshotId(),
            ImmutableList.copyOf(Iterables.transform(scanTasks, FileScanTask::file)));
      }

      // Continue to traverse the parent snapshot if exists.
      if (current.parentId() == null) {
        break;
      }
      // Iterate to the parent snapshot.
      current = table.snapshot(current.parentId());
    }
    return result;
  }

  public static List<DataFile> matchingPartitions(
      List<DataFile> dataFiles, PartitionSpec partitionSpec, Map<String, Object> partitionValues) {
    Types.StructType partitionType = partitionSpec.partitionType();
    Record partitionRecord = GenericRecord.create(partitionType).copy(partitionValues);
    StructLikeWrapper expected = StructLikeWrapper.forType(partitionType).set(partitionRecord);
    return dataFiles.stream()
        .filter(
            df -> {
              StructLikeWrapper wrapper =
                  StructLikeWrapper.forType(partitionType).set(df.partition());
              return wrapper.equals(expected);
            })
        .collect(Collectors.toList());
  }
}
