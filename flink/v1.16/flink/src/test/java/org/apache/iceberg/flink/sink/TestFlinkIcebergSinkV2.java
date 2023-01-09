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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableTestBase;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestTableLoader;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSinkV2 extends TableTestBase {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  private static final int FORMAT_V2 = 2;
  private static final TypeInformation<Row> ROW_TYPE_INFO =
      new RowTypeInfo(SimpleDataUtil.FLINK_SCHEMA.getFieldTypes());

  private static final Map<String, RowKind> ROW_KIND_MAP =
      ImmutableMap.of(
          "+I", RowKind.INSERT,
          "-D", RowKind.DELETE,
          "-U", RowKind.UPDATE_BEFORE,
          "+U", RowKind.UPDATE_AFTER);

  private static final int ROW_ID_POS = 0;
  private static final int ROW_DATA_POS = 1;

  private final FileFormat format;
  private final int parallelism;
  private final boolean partitioned;
  private final String writeDistributionMode;

  private StreamExecutionEnvironment env;
  private TestTableLoader tableLoader;

  @Parameterized.Parameters(
      name = "FileFormat = {0}, Parallelism = {1}, Partitioned={2}, WriteDistributionMode ={3}")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {"avro", 1, true, TableProperties.WRITE_DISTRIBUTION_MODE_NONE},
      new Object[] {"avro", 1, false, TableProperties.WRITE_DISTRIBUTION_MODE_NONE},
      new Object[] {"avro", 4, true, TableProperties.WRITE_DISTRIBUTION_MODE_NONE},
      new Object[] {"avro", 4, false, TableProperties.WRITE_DISTRIBUTION_MODE_NONE},
      new Object[] {"orc", 1, true, TableProperties.WRITE_DISTRIBUTION_MODE_HASH},
      new Object[] {"orc", 1, false, TableProperties.WRITE_DISTRIBUTION_MODE_HASH},
      new Object[] {"orc", 4, true, TableProperties.WRITE_DISTRIBUTION_MODE_HASH},
      new Object[] {"orc", 4, false, TableProperties.WRITE_DISTRIBUTION_MODE_HASH},
      new Object[] {"parquet", 1, true, TableProperties.WRITE_DISTRIBUTION_MODE_RANGE},
      new Object[] {"parquet", 1, false, TableProperties.WRITE_DISTRIBUTION_MODE_RANGE},
      new Object[] {"parquet", 4, true, TableProperties.WRITE_DISTRIBUTION_MODE_RANGE},
      new Object[] {"parquet", 4, false, TableProperties.WRITE_DISTRIBUTION_MODE_RANGE}
    };
  }

  public TestFlinkIcebergSinkV2(
      String format, int parallelism, boolean partitioned, String writeDistributionMode) {
    super(FORMAT_V2);
    this.format = FileFormat.fromString(format);
    this.parallelism = parallelism;
    this.partitioned = partitioned;
    this.writeDistributionMode = writeDistributionMode;
  }

  @Before
  public void setupTable() throws IOException {
    this.tableDir = temp.newFolder();
    this.metadataDir = new File(tableDir, "metadata");
    Assert.assertTrue(tableDir.delete());

    if (!partitioned) {
      table = create(SimpleDataUtil.SCHEMA, PartitionSpec.unpartitioned());
    } else {
      table =
          create(
              SimpleDataUtil.SCHEMA,
              PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build());
    }

    table
        .updateProperties()
        .set(TableProperties.DEFAULT_FILE_FORMAT, format.name())
        .set(TableProperties.WRITE_DISTRIBUTION_MODE, writeDistributionMode)
        .commit();

    env =
        StreamExecutionEnvironment.getExecutionEnvironment(
                MiniClusterResource.DISABLE_CLASSLOADER_CHECK_CONFIG)
            .enableCheckpointing(100L)
            .setParallelism(parallelism)
            .setMaxParallelism(parallelism);

    tableLoader = new TestTableLoader(tableDir.getAbsolutePath());
  }

  private List<Snapshot> findValidSnapshots(Table table) {
    List<Snapshot> validSnapshots = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
      if (snapshot.allManifests(table.io()).stream()
          .anyMatch(m -> snapshot.snapshotId() == m.snapshotId())) {
        validSnapshots.add(snapshot);
      }
    }
    return validSnapshots;
  }

  private void testChangeLogs(
      List<String> equalityFieldColumns,
      KeySelector<Row, Object> keySelector,
      boolean insertAsUpsert,
      List<List<Row>> elementsPerCheckpoint,
      List<List<Record>> expectedRecordsPerCheckpoint)
      throws Exception {
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(elementsPerCheckpoint), ROW_TYPE_INFO);

    FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
        .tableLoader(tableLoader)
        .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
        .writeParallelism(parallelism)
        .equalityFieldColumns(equalityFieldColumns)
        .upsert(insertAsUpsert)
        .append();

    // Execute the program.
    env.execute("Test Iceberg Change-Log DataStream.");

    table.refresh();
    List<Snapshot> snapshots = findValidSnapshots(table);
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

  private Row row(String rowKind, int id, String data) {
    RowKind kind = ROW_KIND_MAP.get(rowKind);
    if (kind == null) {
      throw new IllegalArgumentException("Unknown row kind: " + rowKind);
    }

    return Row.ofKind(kind, id, data);
  }

  private Record record(int id, String data) {
    return SimpleDataUtil.createRecord(id, data);
  }

  @Test
  public void testCheckAndGetEqualityFieldIds() {
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("type", Types.StringType.get())
        .setIdentifierFields("type")
        .commit();

    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(ImmutableList.of()), ROW_TYPE_INFO);
    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA).table(table);

    // Use schema identifier field IDs as equality field id list by default
    Assert.assertEquals(
        table.schema().identifierFieldIds(),
        Sets.newHashSet(builder.checkAndGetEqualityFieldIds()));

    // Use user-provided equality field column as equality field id list
    builder.equalityFieldColumns(Lists.newArrayList("id"));
    Assert.assertEquals(
        Sets.newHashSet(table.schema().findField("id").fieldId()),
        Sets.newHashSet(builder.checkAndGetEqualityFieldIds()));

    builder.equalityFieldColumns(Lists.newArrayList("type"));
    Assert.assertEquals(
        Sets.newHashSet(table.schema().findField("type").fieldId()),
        Sets.newHashSet(builder.checkAndGetEqualityFieldIds()));
  }

  @Test
  public void testChangeLogOnIdKey() throws Exception {
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
      AssertHelpers.assertThrows(
          "Should be error because equality field columns don't include all partition keys",
          IllegalStateException.class,
          "should be included in equality fields",
          () -> {
            testChangeLogs(
                ImmutableList.of("id"),
                row -> row.getField(ROW_ID_POS),
                false,
                elementsPerCheckpoint,
                expectedRecords);
            return null;
          });
    } else {
      testChangeLogs(
          ImmutableList.of("id"),
          row -> row.getField(ROW_ID_POS),
          false,
          elementsPerCheckpoint,
          expectedRecords);
    }
  }

  @Test
  public void testChangeLogOnDataKey() throws Exception {
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
        expectedRecords);
  }

  @Test
  public void testChangeLogOnIdDataKey() throws Exception {
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
        expectedRecords);
  }

  @Test
  public void testChangeLogOnSameKey() throws Exception {
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
        expectedRecords);
  }

  @Test
  public void testUpsertModeCheck() throws Exception {
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(ImmutableList.of()), ROW_TYPE_INFO);
    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .tableLoader(tableLoader)
            .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
            .writeParallelism(parallelism)
            .upsert(true);

    AssertHelpers.assertThrows(
        "Should be error because upsert mode and overwrite mode enable at the same time.",
        IllegalStateException.class,
        "OVERWRITE mode shouldn't be enable",
        () ->
            builder.equalityFieldColumns(ImmutableList.of("id", "data")).overwrite(true).append());

    AssertHelpers.assertThrows(
        "Should be error because equality field columns are empty.",
        IllegalStateException.class,
        "Equality field columns shouldn't be empty",
        () -> builder.equalityFieldColumns(ImmutableList.of()).overwrite(false).append());
  }

  @Test
  public void testUpsertOnIdKey() throws Exception {
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
          expectedRecords);
    } else {
      AssertHelpers.assertThrows(
          "Should be error because equality field columns don't include all partition keys",
          IllegalStateException.class,
          "should be included in equality fields",
          () -> {
            testChangeLogs(
                ImmutableList.of("id"),
                row -> row.getField(ROW_ID_POS),
                true,
                elementsPerCheckpoint,
                expectedRecords);
            return null;
          });
    }
  }

  @Test
  public void testUpsertOnDataKey() throws Exception {
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
        expectedRecords);
  }

  @Test
  public void testUpsertOnIdDataKey() throws Exception {
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
        expectedRecords);
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
