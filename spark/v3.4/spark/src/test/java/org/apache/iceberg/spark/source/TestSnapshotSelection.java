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
import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSnapshotSelection {

  @Parameterized.Parameters(name = "planningMode = {0}")
  public static Object[] parameters() {
    return new Object[] {LOCAL, DISTRIBUTED};
  }

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  private final Map<String, String> properties;

  public TestSnapshotSelection(PlanningMode planningMode) {
    this.properties =
        ImmutableMap.of(
            TableProperties.DATA_PLANNING_MODE, planningMode.modeName(),
            TableProperties.DELETE_PLANNING_MODE, planningMode.modeName());
  }

  @BeforeClass
  public static void startSpark() {
    TestSnapshotSelection.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSnapshotSelection.spark;
    TestSnapshotSelection.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testSnapshotSelectionById() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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

    Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, currentSnapshotRecords);

    // verify records in the previous snapshot
    Snapshot currentSnapshot = table.currentSnapshot();
    Long parentSnapshotId = currentSnapshot.parentId();
    Dataset<Row> previousSnapshotResult =
        spark.read().format("iceberg").option("snapshot-id", parentSnapshotId).load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords =
        previousSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals(
        "Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);
  }

  @Test
  public void testSnapshotSelectionByTimestamp() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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

    Assert.assertEquals("Expected 2 snapshots", 2, Iterables.size(table.snapshots()));

    // verify records in the current snapshot
    Dataset<Row> currentSnapshotResult = spark.read().format("iceberg").load(tableLocation);
    List<SimpleRecord> currentSnapshotRecords =
        currentSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    List<SimpleRecord> expectedRecords = Lists.newArrayList();
    expectedRecords.addAll(firstBatchRecords);
    expectedRecords.addAll(secondBatchRecords);
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, currentSnapshotRecords);

    // verify records in the previous snapshot
    Dataset<Row> previousSnapshotResult =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.AS_OF_TIMESTAMP, firstSnapshotTimestamp)
            .load(tableLocation);
    List<SimpleRecord> previousSnapshotRecords =
        previousSnapshotResult.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals(
        "Previous snapshot rows should match", firstBatchRecords, previousSnapshotRecords);
  }

  @Test
  public void testSnapshotSelectionByInvalidSnapshotId() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, properties, tableLocation);

    Dataset<Row> df = spark.read().format("iceberg").option("snapshot-id", -10).load(tableLocation);

    Assertions.assertThatThrownBy(df::collectAsList)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find snapshot with ID -10");
  }

  @Test
  public void testSnapshotSelectionByInvalidTimestamp() throws IOException {
    long timestamp = System.currentTimeMillis();

    String tableLocation = temp.newFolder("iceberg-table").toString();
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, properties, tableLocation);

    Assertions.assertThatThrownBy(
            () ->
                spark
                    .read()
                    .format("iceberg")
                    .option(SparkReadOptions.AS_OF_TIMESTAMP, timestamp)
                    .load(tableLocation))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find a snapshot older than");
  }

  @Test
  public void testSnapshotSelectionBySnapshotIdAndTimestamp() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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

    Assertions.assertThatThrownBy(
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

  @Test
  public void testSnapshotSelectionByTag() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, currentSnapshotRecords);
  }

  @Test
  public void testSnapshotSelectionByBranch() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, currentSnapshotRecords);
  }

  @Test
  public void testSnapshotSelectionByBranchAndTagFails() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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

    Assertions.assertThatThrownBy(
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

  @Test
  public void testSnapshotSelectionByTimestampAndBranchOrTagFails() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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

    Assertions.assertThatThrownBy(
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

    Assertions.assertThatThrownBy(
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

  @Test
  public void testSnapshotSelectionByBranchWithSchemaChange() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, branchSnapshotRecords);

    // Deleting a column to indicate schema change
    table.updateSchema().deleteColumn("data").commit();

    // The data should not have the deleted column
    Assertions.assertThat(
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

    Assertions.assertThat(
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

  @Test
  public void testWritingToBranchAfterSchemaChange() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, branchSnapshotRecords);

    // Deleting and add a new column of the same type to indicate schema change
    table.updateSchema().deleteColumn("data").addColumn("zip", Types.IntegerType.get()).commit();

    Assertions.assertThat(
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

    Assertions.assertThat(
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

  @Test
  public void testSnapshotSelectionByTagWithSchemaChange() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

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
    Assert.assertEquals("Current snapshot rows should match", expectedRecords, tagSnapshotRecords);

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
    Assert.assertEquals(
        "Current snapshot rows should match", expectedRecords, deletedColumnTagSnapshotRecords);
  }
}
