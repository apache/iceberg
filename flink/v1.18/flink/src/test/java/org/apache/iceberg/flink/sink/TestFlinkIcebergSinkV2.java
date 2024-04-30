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

import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopCatalogResource;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestFlinkIcebergSinkV2 extends TestFlinkIcebergSinkV2Base {

  @ClassRule
  public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE =
      MiniClusterResource.createWithClassloaderCheckDisabled();

  @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

  @Rule
  public final HadoopCatalogResource catalogResource =
      new HadoopCatalogResource(TEMPORARY_FOLDER, TestFixtures.DATABASE, TestFixtures.TABLE);

  @Rule public final Timeout globalTimeout = Timeout.seconds(60);

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
    this.format = FileFormat.fromString(format);
    this.parallelism = parallelism;
    this.partitioned = partitioned;
    this.writeDistributionMode = writeDistributionMode;
  }

  @Before
  public void setupTable() {
    table =
        catalogResource
            .catalog()
            .createTable(
                TestFixtures.TABLE_IDENTIFIER,
                SimpleDataUtil.SCHEMA,
                partitioned
                    ? PartitionSpec.builderFor(SimpleDataUtil.SCHEMA).identity("data").build()
                    : PartitionSpec.unpartitioned(),
                ImmutableMap.of(
                    TableProperties.DEFAULT_FILE_FORMAT,
                    format.name(),
                    TableProperties.FORMAT_VERSION,
                    String.valueOf(FORMAT_V2)));

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

    tableLoader = catalogResource.tableLoader();
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
    testChangeLogOnIdKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testUpsertOnlyDeletesOnDataKey() throws Exception {
    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            ImmutableList.of(row("+I", 1, "aaa")),
            ImmutableList.of(row("-D", 1, "aaa"), row("-D", 2, "bbb")));

    List<List<Record>> expectedRecords =
        ImmutableList.of(ImmutableList.of(record(1, "aaa")), ImmutableList.of());

    testChangeLogs(
        ImmutableList.of("data"),
        row -> row.getField(ROW_DATA_POS),
        true,
        elementsPerCheckpoint,
        expectedRecords,
        SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testChangeLogOnDataKey() throws Exception {
    testChangeLogOnDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testChangeLogOnIdDataKey() throws Exception {
    testChangeLogOnIdDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testChangeLogOnSameKey() throws Exception {
    testChangeLogOnSameKey(SnapshotRef.MAIN_BRANCH);
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

    Assertions.assertThatThrownBy(
            () ->
                builder
                    .equalityFieldColumns(ImmutableList.of("id", "data"))
                    .overwrite(true)
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");

    Assertions.assertThatThrownBy(
            () -> builder.equalityFieldColumns(ImmutableList.of()).overwrite(false).append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
  }

  @Test
  public void testUpsertOnIdKey() throws Exception {
    testUpsertOnIdKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testUpsertOnDataKey() throws Exception {
    testUpsertOnDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testUpsertOnIdDataKey() throws Exception {
    testUpsertOnIdDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @Test
  public void testDeleteStats() throws Exception {
    assumeThat(format).isNotEqualTo(FileFormat.AVRO);

    List<List<Row>> elementsPerCheckpoint =
        ImmutableList.of(
            // Checkpoint #1
            ImmutableList.of(row("+I", 1, "aaa"), row("-D", 1, "aaa"), row("+I", 1, "aaa")));

    List<List<Record>> expectedRecords = ImmutableList.of(ImmutableList.of(record(1, "aaa")));

    testChangeLogs(
        ImmutableList.of("id", "data"),
        row -> Row.of(row.getField(ROW_ID_POS), row.getField(ROW_DATA_POS)),
        false,
        elementsPerCheckpoint,
        expectedRecords,
        "main");

    DeleteFile deleteFile = table.currentSnapshot().addedDeleteFiles(table.io()).iterator().next();
    String fromStat =
        new String(
            deleteFile.lowerBounds().get(MetadataColumns.DELETE_FILE_PATH.fieldId()).array());
    DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
    assumeThat(fromStat).isEqualTo(dataFile.path().toString());
  }
}
