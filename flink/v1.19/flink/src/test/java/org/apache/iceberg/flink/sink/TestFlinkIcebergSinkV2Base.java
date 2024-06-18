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
package org.apache.iceberg.flink.sink;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;

public class TestFlinkIcebergSinkV2Base {

  protected static final int FORMAT_V2 = 2;
  protected static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

  protected static final int ROW_ID_POS = 0;
  protected static final int ROW_DATA_POS = 1;

  protected int parallelism = 1;
  protected TableLoader tableLoader;
  protected Table table;
  protected StreamExecutionEnvironment env;
  protected FileFormat format;
  protected boolean partitioned;
  protected String writeDistributionMode;

  protected static final Map<String, RowKind> ROW_KIND_MAP =
      ImmutableMap.of(
          "+I", RowKind.INSERT,
          "-D", RowKind.DELETE,
          "-U", RowKind.UPDATE_BEFORE,
          "+U", RowKind.UPDATE_AFTER);

  protected Row row(String rowKind, int id, String data) {
    RowKind kind = ROW_KIND_MAP.get(rowKind);
    if (kind == null) {
      throw new IllegalArgumentException("Unknown row kind: " + rowKind);
    }

    return Row.ofKind(kind, id, data);
  }

  protected void testUpsertOnIdDataKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+U", 1, "aaa"), row("+I", 2, "bbb")),
            ImmutableList.of(row("+I", 1, "aaa"), row("-D", 2, "bbb"), row("+I", 2, "ccc")),
            ImmutableList.of(row("+U", 1, "bbb"), row("-U", 1, "ccc"), row("-D", 1, "aaa")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "aaa"), record(2, "bbb")),
            ImmutableList.of(record(1, "aaa"), record(2, "ccc")),
            ImmutableList.of(record(1, "bbb"), record(2, "ccc")));
    testChangeLogs(
        ImmutableList.of("id", "data"),
        row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        true,
        elementsPerCheckpoint,
        expectedRecords,
        branch);
  }

  protected void testChangeLogOnIdDataKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(
                row("+I", 1, "aaa"),
                row("-D", 1, "aaa"),
                row("+I", 2, "bbb"),
                row("+I", 1, "bbb"),
                row("+I", 2, "aaa")),
            ImmutableList.of(row("-U", 2, "aaa"), row("+U", 1, "ccc"), row("+I", 1, "aaa")),
            ImmutableList.of(row("-D", 1, "bbb"), row("+I", 2, "aaa")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "bbb"), record(2, "aaa"), record(2, "bbb")),
            ImmutableList.of(
                record(1, "aaa"), record(1, "bbb"), record(1, "ccc"), record(2, "bbb")),
            ImmutableList.of(
                record(1, "aaa"), record(1, "ccc"), record(2, "aaa"), record(2, "bbb")));

    testChangeLogs(
        ImmutableList.of("data", "id"),
        row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        false,
        elementsPerCheckpoint,
        expectedRecords,
        branch);
  }

  protected void testChangeLogOnSameKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            // Checkpoint #1
            ImmutableList.of(row("+I", 1, "aaa"), row("-D", 1, "aaa"), row("+I", 1, "aaa")),
            // Checkpoint #2
            ImmutableList.of(row("-U", 1, "aaa"), row("+U", 1, "aaa")),
            // Checkpoint #3
            ImmutableList.of(row("-D", 1, "aaa"), row("+I", 1, "aaa")),
            // Checkpoint #4
            ImmutableList.of(row("-U", 1, "aaa"), row("+U", 1, "aaa"), row("+I", 1, "aaa")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "aaa")),
            ImmutableList.of(record(1, "aaa")),
            ImmutableList.of(record(1, "aaa")),
            ImmutableList.of(record(1, "aaa"), record(1, "aaa")));

    testChangeLogs(
        ImmutableList.of("id", "data"),
        row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        false,
        elementsPerCheckpoint,
        expectedRecords,
        branch);
  }

  protected void testChangeLogOnDataKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(
                row("+I", 1, "aaa"),
                row("-D", 1, "aaa"),
                row("+I", 2, "bbb"),
                row("+I", 1, "bbb"),
                row("+I", 2, "aaa")),
            ImmutableList.of(row("-U", 2, "aaa"), row("+U", 1, "ccc"), row("+I", 1, "aaa")),
            ImmutableList.of(row("-D", 1, "bbb"), row("+I", 2, "aaa"), row("+I", 2, "ccc")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "bbb"), record(2, "aaa")),
            ImmutableList.of(record(1, "aaa"), record(1, "bbb"), record(1, "ccc")),
            ImmutableList.of(
                record(1, "aaa"), record(1, "ccc"), record(2, "aaa"), record(2, "ccc")));

    testChangeLogs(
        ImmutableList.of("data"),
        row -> row.getField(ROW_DATA_POS),
        false,
        elementsPerCheckpoint,
        expectedRecords,
        branch);
  }

  protected void testUpsertOnDataKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+I", 2, "aaa"), row("+I", 3, "bbb")),
            ImmutableList.of(row("+U", 4, "aaa"), row("-U", 3, "bbb"), row("+U", 5, "bbb")),
            ImmutableList.of(row("+I", 6, "aaa"), row("+U", 7, "bbb")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(2, "aaa"), record(3, "bbb")),
            ImmutableList.of(record(4, "aaa"), record(5, "bbb")),
            ImmutableList.of(record(6, "aaa"), record(7, "bbb")));

    testChangeLogs(
        ImmutableList.of("data"),
        row -> row.getField(ROW_DATA_POS),
        true,
        elementsPerCheckpoint,
        expectedRecords,
        branch);
  }

  protected void testChangeLogOnIdKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(
                row("+I", 1, "aaa"),
                row("-D", 1, "aaa"),
                row("+I", 1, "bbb"),
                row("+I", 2, "aaa"),
                row("-D", 2, "aaa"),
                row("+I", 2, "bbb")),
            ImmutableList.of(
                row("-U", 2, "bbb"), row("+U", 2, "ccc"), row("-D", 2, "ccc"), row("+I", 2, "ddd")),
            ImmutableList.of(
                row("-D", 1, "bbb"),
                row("+I", 1, "ccc"),
                row("-D", 1, "ccc"),
                row("+I", 1, "ddd")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "bbb"), record(2, "bbb")),
            ImmutableList.of(record(1, "bbb"), record(2, "ddd")),
            ImmutableList.of(record(1, "ddd"), record(2, "ddd")));

    if (partitioned && writeDistributionMode.equals(TableProperties.WRITE_DISTRIBUTION_MODE_HASH)) {
      assertThatThrownBy(
              () ->
                  testChangeLogs(
                      ImmutableList.of("id"),
                      row -> row.getField(ROW_ID_POS),
                      false,
                      elementsPerCheckpoint,
                      expectedRecords,
                      branch))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageStartingWith(
              "In 'hash' distribution mode with equality fields set, partition field")
          .hasMessageContaining("should be included in equality fields:");

    } else {
      testChangeLogs(
          ImmutableList.of("id"),
          row -> row.getField(ROW_ID_POS),
          false,
          elementsPerCheckpoint,
          expectedRecords,
          branch);
    }
  }

  protected void testUpsertOnIdKey(String branch) throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa"), row("+U", 1, "bbb")),
            ImmutableList.of(row("+I", 1, "ccc")),
            ImmutableList.of(row("+U", 1, "ddd"), row("+I", 1, "eee")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(
            ImmutableList.of(record(1, "bbb")),
            ImmutableList.of(record(1, "ccc")),
            ImmutableList.of(record(1, "eee")));

    if (!partitioned) {
      testChangeLogs(
          ImmutableList.of("id"),
          row -> row.getField(ROW_ID_POS),
          true,
          elementsPerCheckpoint,
          expectedRecords,
          branch);
    } else {
      assertThatThrownBy(
              () ->
                  testChangeLogs(
                      ImmutableList.of("id"),
                      row -> row.getField(ROW_ID_POS),
                      true,
                      elementsPerCheckpoint,
                      expectedRecords,
                      branch))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("should be included in equality fields:");
    }
  }

  protected void testChangeLogs(
      List<String> equalityFieldColumns,
      KeySelector<Row, Object> keySelector,
      boolean insertAsUpsert,
      List<List<Row>> elementsPerCheckpoint,
      List<List<Record>> expectedRecordsPerCheckpoint,
      String branch)
      throws Exception {
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .equalityFieldColumns(equalityFieldColumns)
        .upsert(insertAsUpsert)
        .toBranch(branch)
        .append();

    // Execute the program.
    env.execute("Test Iceberg Change-Log DataStream.");

    table.refresh();
    List<Snapshot> snapshots = findValidSnapshots();
    int expectedSnapshotNum = expectedRecordsPerCheckpoint.size();
    Assert.assertEquals(
        "Should have the expected snapshot number", expectedSnapshotNum, snapshots.size());

    for (int i = 0; i < expectedSnapshotNum; i++) {
      long snapshotId = snapshots.get(i).snapshotId();
      List<Record> expectedRecords = expectedRecordsPerCheckpoint.get(i);
      Assert.assertEquals(
          "Should have the expected records for the checkpoint#" + i,
          expectedRowSet(expectedRecords.toArray(new Record[0])),
          actualRowSet(snapshotId, "*"));
    }
  }

  protected Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  private List<Snapshot> findValidSnapshots() {
    List<Snapshot> validSnapshots = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.allManifests(table.io()).stream()
          .anyMatch(m -> snapshot.snapshotId() == m.snapshotId())) {
        validSnapshots.add(snapshot);
      }
    }
    return validSnapshots;
  }

  private StructLikeSet expectedRowSet(Record... records) {
    return SimpleDataUtil.expectedRowSet(table, records);
  }

  private StructLikeSet actualRowSet(long snapshotId, String... columns) throws IOException {
    table.refresh();
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    try (CloseableIterable<Record> reader =
        IcebergGenerics.read(table).useSnapshot(snapshotId).select(columns).build()) {
      reader.forEach(set::add);
    }
    return set;
  }
}
