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
package org.apache.iceberg.spark.source;

import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.apache.iceberg.TestHelpers.waitUntilAfter;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.InetAddress;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSQLProperties;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSnapshotSelection {

  @Parameters(name = "properties = {0}")
  public static Object[] parameters() {
    return new Object[][] {
      {
        ImmutableMap.of(
            TableProperties.DATA_PLANNING_MODE, LOCAL.modeName(),
            TableProperties.DELETE_PLANNING_MODE, LOCAL.modeName())
      },
      {
        ImmutableMap.of(
            TableProperties.DATA_PLANNING_MODE, DISTRIBUTED.modeName(),
            TableProperties.DELETE_PLANNING_MODE, DISTRIBUTED.modeName())
      }
    };
  }

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @TempDir private Path temp;

  private static SparkSession spark = null;

  @Parameter(index = 0)
  private Map<String, String> properties;

  @BeforeAll
  public static void startSpark() {
    TestSnapshotSelection.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestSnapshotSelection.spark;
    TestSnapshotSelection.spark = null;
    currentSpark.stop();
  }

  @AfterEach
  public void cleanupSessionProperties() {
    spark.conf().unset(SparkSQLProperties.AS_OF_TIMESTAMP);
  }

  @TestTemplate
  public void testSnapshotSelectionById() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    assertThat(table.snapshots()).as("Expected 2 snapshots").hasSize(2);

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    assertThat(currentSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // verify records in the previous snapshot
    Snapshot currentSnapshot = table.currentSnapshot();
    Long parentSnapshotId = currentSnapshot.parentId();
    Dataset<Row> previousSnapshotResult =
        spark.read().format("iceberg").option("snapshot-id", parentSnapshotId).load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords =
        previousSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(previousSnapshotRecords)
        .as("Previous snapshot rows should match")
        .isEqualTo(firstBatchRecords);
  }

  @TestTemplate
  public void testSnapshotSelectionByTimestamp() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // remember the time when the first snapshot was valid
    long firstSnapshotTimestamp = System.currentTimeMillis();

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    assertThat(table.snapshots()).as("Expected 2 snapshots").hasSize(2);

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    assertThat(currentSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // verify records in the previous snapshot
    Dataset<Row> previousSnapshotResult =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.AS_OF_TIMESTAMP, firstSnapshotTimestamp)
            .load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords =
        previousSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(previousSnapshotRecords)
        .as("Previous snapshot rows should match")
        .isEqualTo(firstBatchRecords);
  }

  @TestTemplate
  public void testSnapshotSelectionByInvalidSnapshotId() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, properties, tableLocation);

    Dataset<Row> df = spark.read().format("iceberg").option("snapshot-id", -10).load(tableLocation);

    assertThatThrownBy(df::collectAsList)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find snapshot with ID -10");
  }

  @TestTemplate
  public void testSnapshotSelectionByInvalidTimestamp() {
    long timestamp = System.currentTimeMillis();

    String tableLocation = temp.resolve("iceberg-table").toFile().toString();
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, properties, tableLocation);

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                    .load(tableLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find a snapshot older than");
  }

  @TestTemplate
  public void testSnapshotSelectionBySnapshotIdAndTimestamp() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    long timestamp = System.currentTimeMillis();
    long snapshotId = table.currentSnapshot().snapshotId();

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.SNAPSHOT_ID, snapshotId)
                    .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                    .load(tableLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Can specify only one of snapshot-id")
        .hasMessageContaining("as-of-timestamp")
        .hasMessageContaining("branch")
        .hasMessageContaining("tag");
  }

  @TestTemplate
  public void testSnapshotSelectionByTag() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createTag("tag", table.currentSnapshot().snapshotId()).commit();

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // verify records in the current snapshot by tag
    Dataset<Row> currentSnapshotResult =
        spark.read().format("iceberg").option("tag", "tag").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    assertThat(currentSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testSnapshotSelectionByBranch() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // verify records in the current snapshot by branch
    Dataset<Row> currentSnapshotResult =
        spark.read().format("iceberg").option("branch", "branch").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    assertThat(currentSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testSnapshotSelectionByBranchAndTagFails() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();
    table.manageSnapshots().createTag("tag", table.currentSnapshot().snapshotId()).commit();

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.TAG, "tag")
                    .option(SparkReadOptions.BRANCH, "branch")
                    .load(tableLocation)
                    .show())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Can specify only one of snapshot-id");
  }

  @TestTemplate
  public void testSnapshotSelectionByTimestampAndBranchOrTagFails() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    long timestamp = System.currentTimeMillis();
    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();
    table.manageSnapshots().createTag("tag", table.currentSnapshot().snapshotId()).commit();

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                    .option(SparkReadOptions.BRANCH, "branch")
                    .load(tableLocation)
                    .show())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Can specify only one of snapshot-id");

    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                    .option(SparkReadOptions.TAG, "tag")
                    .load(tableLocation)
                    .show())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Can specify only one of snapshot-id");
  }

  @TestTemplate
  public void testSnapshotSelectionByBranchWithSchemaChange() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();

    Dataset<Row> branchSnapshotResult =
        spark.read().format("iceberg").option("branch", "branch").load(tableLocation);
    List<SimpleRecord> branchSnapshotRecords =
        branchSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    assertThat(branchSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // Deleting a column to indicate schema change
    table.updateSchema().deleteColumn("data").commit();

    // The data should not have the deleted column
    assertThat(
            spark
                .read()
                .format("iceberg")
                .option("branch", "branch")
                .load(tableLocation)
                .orderBy("id")
                .collectAsList())
        .containsExactly(RowFactory.create(1), RowFactory.create(2), RowFactory.create(3));

    // re-introducing the column should not let the data re-appear
    table.updateSchema().addColumn("data", Types.StringType.get()).commit();

    assertThat(
            spark
                .read()
                .format("iceberg")
                .option("branch", "branch")
                .load(tableLocation)
                .orderBy("id")
                .as(Encoders.bean(SimpleRecord.class))
                .collectAsList())
        .containsExactly(
            new SimpleRecord(1, null), new SimpleRecord(2, null), new SimpleRecord(3, null));
  }

  @TestTemplate
  public void testWritingToBranchAfterSchemaChange() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createBranch("branch", table.currentSnapshot().snapshotId()).commit();

    Dataset<Row> branchSnapshotResult =
        spark.read().format("iceberg").option("branch", "branch").load(tableLocation);
    List<SimpleRecord> branchSnapshotRecords =
        branchSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    assertThat(branchSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // Deleting and add a new column of the same type to indicate schema change
    table.updateSchema().deleteColumn("data").addColumn("zip", Types.IntegerType.get()).commit();

    assertThat(
            spark
                .read()
                .format("iceberg")
                .option("branch", "branch")
                .load(tableLocation)
                .orderBy("id")
                .collectAsList())
        .containsExactly(
            RowFactory.create(1, null), RowFactory.create(2, null), RowFactory.create(3, null));

    // writing new records into the branch should work with the new column
    List<Row> records =
        Lists.newArrayList(
            RowFactory.create(4, 12345), RowFactory.create(5, 54321), RowFactory.create(6, 67890));

    Dataset<Row> dataFrame =
        spark.createDataFrame(
            records,
            SparkSchemaUtil.convert(
                new Schema(
                    optional(1, "id", Types.IntegerType.get()),
                    optional(2, "zip", Types.IntegerType.get()))));
    dataFrame
        .select("id", "zip")
        .write()
        .format("iceberg")
        .option("branch", "branch")
        .mode("append")
        .save(tableLocation);

    assertThat(
            spark
                .read()
                .format("iceberg")
                .option("branch", "branch")
                .load(tableLocation)
                .collectAsList())
        .hasSize(6)
        .contains(
            RowFactory.create(1, null), RowFactory.create(2, null), RowFactory.create(3, null))
        .containsAll(records);
  }

  @TestTemplate
  public void testSnapshotSelectionByTagWithSchemaChange() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.manageSnapshots().createTag("tag", table.currentSnapshot().snapshotId()).commit();

    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);

    Dataset<Row> tagSnapshotResult =
        spark.read().format("iceberg").option("tag", "tag").load(tableLocation);
    List<SimpleRecord> tagSnapshotRecords =
        tagSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(tagSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // Deleting a column to indicate schema change
    table.updateSchema().deleteColumn("data").commit();

    // The data should have the deleted column as it was captured in an earlier snapshot.
    Dataset<Row> deletedColumnTagSnapshotResult =
        spark.read().format("iceberg").option("tag", "tag").load(tableLocation);
    List<SimpleRecord> deletedColumnTagSnapshotRecords =
        deletedColumnTagSnapshotResult
            .orderBy("id")
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();
    assertThat(deletedColumnTagSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);
  }

  @TestTemplate
  public void testSessionPropertyTimeTravel() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();
    long timestampBeforeAnySnapshots = 1L;

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    // remember the time when the first snapshot was valid
    long firstSnapshotTimestamp = waitUntilAfter(table.currentSnapshot().timestampMillis() + 1000);

    // produce the second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    table.refresh();
    assertThat(table.snapshots()).as("Expected 2 snapshots").hasSize(2);

    // verify without session property, current snapshot is read
    Dataset<Row> currentSnapshotResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    assertThat(currentSnapshotRecords)
        .as("Current snapshot rows should match")
        .isEqualTo(expectedRecords);

    // verify with session property, only first snapshot data is read
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, firstSnapshotTimestamp);
    Dataset<Row> sessionPropertyResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> sessionPropertyRecords =
        sessionPropertyResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    assertThat(sessionPropertyRecords)
        .as("First snapshot should be read when session property set.")
        .isEqualTo(firstBatchRecords);

    // verify session property with timestamp before any snapshots raises exception
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, timestampBeforeAnySnapshots);
    assertThatThrownBy(() -> spark.read().format("iceberg").load(tableLocation).collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find a snapshot older than");

    // verify malformatted session property timestamp raises exception
    String malformattedSessionTimestamp = "2020-01-01 10:00:00";
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, malformattedSessionTimestamp);
    assertThatThrownBy(() -> spark.read().format("iceberg").load(tableLocation).collectAsList())
        .isInstanceOf(NumberFormatException.class)
        .hasMessageContaining(
            String.format("For input string: \"%s\"", malformattedSessionTimestamp));
  }

  @TestTemplate
  public void testSessionPropertyWithTableSelectorThrowsException() {
    String tableLocation = temp.resolve("iceberg-table").toFile().toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, properties, tableLocation);

    // produce the first snapshot
    List<SimpleRecord> firstBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));
    Dataset<Row> firstDf = spark.createDataFrame(firstBatchRecords, SimpleRecord.class);
    firstDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    long snapshotId = table.currentSnapshot().snapshotId();
    long firstSnapshotTimestamp = waitUntilAfter(table.currentSnapshot().timestampMillis() + 1000);
    String tagName = "test_tag";
    String branchName = "test_branch";

    table.manageSnapshots().createTag(tagName, snapshotId).commit();
    table.manageSnapshots().createBranch(branchName, snapshotId).commit();

    // produce a second snapshot
    List<SimpleRecord> secondBatchRecords =
        Lists.newArrayList(
            new SimpleRecord(4, "d"), new SimpleRecord(5, "e"), new SimpleRecord(6, "f"));
    Dataset<Row> secondDf = spark.createDataFrame(secondBatchRecords, SimpleRecord.class);
    secondDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    // set session property so it is active for all assertions below
    spark.conf().set(SparkSQLProperties.AS_OF_TIMESTAMP, firstSnapshotTimestamp);

    // VERSION_AS_OF (snapshot ID) option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.VERSION_AS_OF, snapshotId)
                    .load(tableLocation)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // TIMESTAMP_AS_OF option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(
                        SparkReadOptions.TIMESTAMP_AS_OF,
                        sdf.format(new Date(firstSnapshotTimestamp)))
                    .load(tableLocation)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // VERSION_AS_OF (tag) option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.VERSION_AS_OF, tagName)
                    .load(tableLocation)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Cannot set both snapshot-id and as-of-timestamp to select which table snapshot to scan");

    // BRANCH option combined with session property raises exception
    assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.BRANCH, branchName)
                    .load(tableLocation)
                    .collectAsList())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot override ref, already set snapshot id=%s", snapshotId);
  }
}
