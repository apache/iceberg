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

import static org.apache.iceberg.flink.TestFixtures.DATABASE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniClusterResource;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.BoundedTestSource;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 60)
public class TestFlinkIcebergSinkV2 extends TestFlinkIcebergSinkV2Base {
  @RegisterExtension
  public static MiniClusterExtension miniClusterResource =
      MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

  @RegisterExtension
  private static final HadoopCatalogExtension catalogResource =
      new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

  @BeforeEach
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

  @TestTemplate
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
    assertThat(builder.checkAndGetEqualityFieldIds())
        .containsExactlyInAnyOrderElementsOf(table.schema().identifierFieldIds());

    // Use user-provided equality field column as equality field id list
    builder.equalityFieldColumns(Lists.newArrayList("id"));
    assertThat(builder.checkAndGetEqualityFieldIds())
        .containsExactlyInAnyOrder(table.schema().findField("id").fieldId());

    builder.equalityFieldColumns(Lists.newArrayList("type"));
    assertThat(builder.checkAndGetEqualityFieldIds())
        .containsExactlyInAnyOrder(table.schema().findField("type").fieldId());
  }

  @TestTemplate
  public void testChangeLogOnIdKey() throws Exception {
    testChangeLogOnIdKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
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

  @TestTemplate
  public void testChangeLogOnDataKey() throws Exception {
    testChangeLogOnDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
  public void testChangeLogOnIdDataKey() throws Exception {
    testChangeLogOnIdDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
  public void testChangeLogOnSameKey() throws Exception {
    testChangeLogOnSameKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
  public void testUpsertModeCheck() throws Exception {
    DataStream<Row> dataStream =
        env.addSource(new BoundedTestSource<>(ImmutableList.of()), ROW_TYPE_INFO);
    FlinkSink.Builder builder =
        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
            .tableLoader(tableLoader)
            .tableSchema(SimpleDataUtil.FLINK_SCHEMA)
            .writeParallelism(parallelism)
            .upsert(true);

    assertThatThrownBy(
            () ->
                builder
                    .equalityFieldColumns(ImmutableList.of("id", "data"))
                    .overwrite(true)
                    .append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "OVERWRITE mode shouldn't be enable when configuring to use UPSERT data stream.");

    assertThatThrownBy(
            () -> builder.equalityFieldColumns(ImmutableList.of()).overwrite(false).append())
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
  }

  @TestTemplate
  public void testUpsertOnIdKey() throws Exception {
    testUpsertOnIdKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
  public void testUpsertOnDataKey() throws Exception {
    testUpsertOnDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
  public void testUpsertOnIdDataKey() throws Exception {
    testUpsertOnIdDataKey(SnapshotRef.MAIN_BRANCH);
  }

  @TestTemplate
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
