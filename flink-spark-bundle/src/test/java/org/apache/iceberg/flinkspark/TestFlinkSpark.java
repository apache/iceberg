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
package org.apache.iceberg.flinkspark;

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
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.HadoopCatalogExtension;
import org.apache.iceberg.flink.MiniFlinkClusterExtension;
import org.apache.iceberg.flink.SimpleDataUtil;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.sink.FlinkSink;
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
import org.apache.flink.types.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.RowFactory;

@ExtendWith(ParameterizedTestExtension.class)
@Timeout(value = 60)
public class TestFlinkSpark extends TestFlinkIcebergSinkV2Base {
    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_EXTENSION =
            MiniFlinkClusterExtension.createWithClassloaderCheckDisabled();

    @RegisterExtension
    private static final HadoopCatalogExtension CATALOG_EXTENSION =
            new HadoopCatalogExtension(DATABASE, TestFixtures.TABLE);

    @BeforeEach
    public void setupTable() {
        table =
                CATALOG_EXTENSION
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
                                MiniFlinkClusterExtension.DISABLE_CLASSLOADER_CHECK_CONFIG)
                        .enableCheckpointing(100L)
                        .setParallelism(parallelism)
                        .setMaxParallelism(parallelism);

        tableLoader = CATALOG_EXTENSION.tableLoader();
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

        if (writeDistributionMode.equals(DistributionMode.RANGE.modeName()) && !partitioned) {
            // validation error thrown from distributeDataStream
            assertThatThrownBy(
                    () -> builder.equalityFieldColumns(ImmutableList.of()).overwrite(false).append())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "Invalid write distribution mode: range. Need to define sort order or partition spec.");
        } else {
            // validation error thrown from appendWriter
            assertThatThrownBy(
                    () -> builder.equalityFieldColumns(ImmutableList.of()).overwrite(false).append())
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessage(
                            "Equality field columns shouldn't be empty when configuring to use UPSERT data stream.");
        }
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

    @TestTemplate
    public void testCheckAndGetEqualityTable() {
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
    public void testEqualityDeleteWritesOnSpark() throws Exception {
        // Step 1: Write initial data using Flink
        DataStream<Row> dataStream = env.fromCollection(
                ImmutableList.of(
                        row("+I", 1, "value1"),
                        row("+I", 2, "value2"),
                        row("+I", 3, "value3")
                )
        );


        FlinkSink.forRow(dataStream, SimpleDataUtil.FLINK_SCHEMA)
                .tableLoader(tableLoader)
                .writeParallelism(parallelism)
                .append();

        // Execute the Flink job to write initial data
        env.execute("Write Initial Data");

        // Step 2: Apply equality deletes using Flink

        DataStream<Row> deleteStream = env.fromCollection(
                ImmutableList.of(
                        row("-D", 1, "value1"),  // Equality delete row with id=1
                        row("-D", 2, "value2")   // Equality delete row with id=2
                )
        );

        FlinkSink.forRow(deleteStream, SimpleDataUtil.FLINK_SCHEMA)
                .tableLoader(tableLoader)
                .equalityFieldColumns(Lists.newArrayList("id", "data"))
                .writeParallelism(parallelism)
                .upsert(true)  // Enable UPSERT mode for equality deletes
                .append();

        // Execute the Flink job to apply equality deletes
        env.execute("Apply Equality Deletes");

        // Step 3: Use Spark to read the table and verify that equality deletes were applied correctly

        // Initialize SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("IcebergSparkRead")
                .master("local[*]")
                .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
                .config("spark.sql.catalog.myCatalog.warehouse", "file:///path/to/warehouse")
                .getOrCreate();

        // Read the table using Spark
        Dataset<org.apache.spark.sql.Row> result = spark.read()
                .format("iceberg")
                .load("hadoop_catalog." + DATABASE + "." + TestFixtures.TABLE);

        // Collect the result
        List<org.apache.spark.sql.Row> actualData = result.collectAsList();

        // Expected result after applying equality deletes (only id=3 should remain)
        List<org.apache.spark.sql.Row> expectedData = ImmutableList.of(RowFactory.create(3, "value3"));

        // Assert that only row with id=3 remains in the table
        assertThat(actualData).containsExactlyInAnyOrderElementsOf(expectedData);

        // Stop the Spark session
        spark.stop();
    }
}