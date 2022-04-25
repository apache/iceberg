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

package org.apache.iceberg.spark.actions;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.actions.ActionsProvider;
import org.apache.iceberg.actions.GenerateChangeSet;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestGenerateChangeSetSparkAction extends SparkTestBase {
  protected ActionsProvider actions() {
    return SparkActions.get();
  }

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  protected static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  private File tableDir = null;
  protected String tableLocation = null;
  private Table table = null;
  private final String tableName = "tbl";

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
    this.table = createATableWith2Snapshots(tableLocation);

    // setup the catalog
    spark.conf().set("spark.sql.catalog.hive", SparkCatalog.class.getName());
    spark.conf().set("spark.sql.catalog.hive.type", "hive");
    spark.conf().set("spark.sql.catalog.hive.default-namespace", "default");
    spark.conf().set("spark.sql.catalog.hive.cache-enabled", "false");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS hive.default.%s", tableName);
  }

  private Table createATableWith2Snapshots(String location) {
    return createTableWithSnapshots(location, 2);
  }

  private Table createTableWithSnapshots(String location, int snapshotNumber) {
    return createTableWithSnapshots(location, snapshotNumber, Maps.newHashMap());
  }

  protected Table createTableWithSnapshots(String location, int snapshotNumber, Map<String, String> properties) {
    Table newTable = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), properties, location);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    for (int i = 0; i < snapshotNumber; i++) {
      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("append")
          .save(location);
    }

    return newTable;
  }

  @Test
  public void testAppendOnly() {
    // the current snapshot should only have one row
    GenerateChangeSet.Result result = actions().generateChangeSet(table).forCurrentSnapshot().execute();

    // verifyData
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    List<Object[]> actualRecords = rowsToJava(resultDF.collectAsList());
    Snapshot snapshot = table.currentSnapshot();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(1, "AAAAAAAAAA", "AAAA", "I", snapshot.snapshotId(), snapshot.timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  @Test
  public void testDeleteNothing() throws IOException {
    Table sourceTable = createMetastoreTable(Maps.newHashMap());
    // delete nothing, however, it generates a new snapshot with nothing has been changed
    sql("delete from hive.default.%s where c1 = 1", tableName);
    sourceTable.refresh();
    GenerateChangeSet.Result result = actions().generateChangeSet(sourceTable).forCurrentSnapshot().execute();
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    Assert.assertEquals("Incorrect result", null, resultDF);
  }

  @Test
  public void testMetadataDeleteOnly() throws IOException {
    Table tbl = createMetastoreTable(Maps.newHashMap());
    // delete the only row
    sql("delete from hive.default.%s where c1 = 0", tableName);
    tbl.refresh();
    GenerateChangeSet.Result result = actions().generateChangeSet(tbl).forCurrentSnapshot().execute();

    // verify results
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    List<Object[]> actualRecords = rowsToJava(resultDF.collectAsList());
    Snapshot snapshot = tbl.currentSnapshot();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(0, "AAAAAAAAAA", "AAAA", "D", snapshot.snapshotId(), snapshot.timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  @Test
  public void testReadEqualityDeleteRows() throws IOException {
    Table tbl = createMetastoreTable(Maps.newHashMap(), 3);
    TableOperations ops = ((BaseTable) tbl).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // vectorized
    tbl.updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
        .commit();

    Schema deleteSchema = tbl.schema().select("c1");
    Record idDelete = GenericRecord.create(deleteSchema);
    List<Record> idDeletes = Lists.newArrayList(
        idDelete.copy("c1", 0),
        idDelete.copy("c1", 1)
    );

    DeleteFile eqDelete = FileHelpers.writeDeleteFile(
        tbl, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), idDeletes, deleteSchema);

    tbl.newRowDelta()
        .addDeletes(eqDelete)
        .commit();

    GenerateChangeSet.Result result = actions().generateChangeSet(tbl).forCurrentSnapshot().execute();
    // verify the results
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    List<Object[]> actualRecords = rowsToJava(resultDF.sort("c1").collectAsList());
    Snapshot snapshot = tbl.currentSnapshot();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(0, "AAAAAAAAAA", "AAAA", "D", snapshot.snapshotId(), snapshot.timestampMillis(), 0),
        row(1, "AAAAAAAAAA", "AAAA", "D", snapshot.snapshotId(), snapshot.timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  @Test
  public void testNonVectorizedReadEqualityDeleteRows() throws IOException {
    Table tbl = createMetastoreTable(Maps.newHashMap(), 3);
    TableOperations ops = ((BaseTable) tbl).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // non-vectorized
    tbl.updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "false")
        .commit();

    Schema deleteSchema = tbl.schema().select("c1");
    Record idDelete = GenericRecord.create(deleteSchema);
    List<Record> idDeletes = Lists.newArrayList(
        idDelete.copy("c1", 0),
        idDelete.copy("c1", 1)
    );

    DeleteFile eqDelete = FileHelpers.writeDeleteFile(
        tbl, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), idDeletes, deleteSchema);

    tbl.newRowDelta()
        .addDeletes(eqDelete)
        .commit();

    GenerateChangeSet.Result result = actions().generateChangeSet(tbl).forCurrentSnapshot().execute();
    // verify the results
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    List<Object[]> actualRecords = rowsToJava(resultDF.sort("c1").collectAsList());
    Snapshot snapshot = tbl.currentSnapshot();
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(0, "AAAAAAAAAA", "AAAA", "D", snapshot.snapshotId(), snapshot.timestampMillis(), 0),
        row(1, "AAAAAAAAAA", "AAAA", "D", snapshot.snapshotId(), snapshot.timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  @Test
  public void testReadEqualityDeleteRowsWithNoResult() throws IOException {
    Table tbl = createMetastoreTable(Maps.newHashMap());
    TableOperations ops = ((BaseTable) tbl).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // vectorized
    tbl.updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
        .commit();

    Schema deleteSchema = tbl.schema().select("c1");
    Record idDelete = GenericRecord.create(deleteSchema);
    List<Record> idDeletes = Lists.newArrayList(
        idDelete.copy("c1", 8),
        idDelete.copy("c1", 9)
    );

    DeleteFile eqDelete = FileHelpers.writeDeleteFile(
        tbl, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), idDeletes, deleteSchema);

    tbl.newRowDelta()
        .addDeletes(eqDelete)
        .commit();

    GenerateChangeSet.Result result = actions().generateChangeSet(tbl).forCurrentSnapshot().execute();
    Assert.assertTrue("Must be no result since the c1 value in the eq delete file couldn't match any data file",
        result.changeSet() == null);
  }

  @Test
  public void testEqDeleteWithMultipleSnapshots() throws IOException {
    Table tbl = createMetastoreTable(Maps.newHashMap(), 3);
    TableOperations ops = ((BaseTable) tbl).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // vectorized
    tbl.updateProperties()
        .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
        .commit();

    Schema deleteSchema1 = tbl.schema().select("c1");
    Record dataDelete = GenericRecord.create(deleteSchema1);
    List<Record> deleteList1 = Lists.newArrayList(
        dataDelete.copy("c1", 0)
    );

    Schema deleteSchema2 = tbl.schema().select("c1");
    Record idDelete = GenericRecord.create(deleteSchema2);
    List<Record> deleteList2 = Lists.newArrayList(
        idDelete.copy("c1", 1)
    );

    DeleteFile eqDelete1 = FileHelpers.writeDeleteFile(
        tbl, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deleteList1, deleteSchema1);
    tbl.newRowDelta()
        .addDeletes(eqDelete1)
        .commit();
    long snapshotId1 = tbl.currentSnapshot().snapshotId();

    DeleteFile eqDelete2 = FileHelpers.writeDeleteFile(
        tbl, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), deleteList2, deleteSchema2);
    tbl.newRowDelta()
        .addDeletes(eqDelete2)
        .commit();
    Snapshot snapshotId2 = tbl.currentSnapshot();

    GenerateChangeSet.Result result = actions().generateChangeSet(tbl).forCurrentSnapshot().execute();
    // verify the results
    Dataset<Row> resultDF = (Dataset<Row>) result.changeSet();
    List<Object[]> actualRecords = rowsToJava(resultDF.sort("c1").collectAsList());
    ImmutableList<Object[]> expectedRows = ImmutableList.of(
        row(1, "AAAAAAAAAA", "AAAA", "D", snapshotId2.snapshotId(), snapshotId2.timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);

    // select the first eq delete snapshot
    result = actions().generateChangeSet(tbl).forSnapshot(snapshotId1).execute();
    // verify the results
    resultDF = (Dataset<Row>) result.changeSet();
    actualRecords = rowsToJava(resultDF.sort("c1").collectAsList());
    expectedRows = ImmutableList.of(
        row(0, "AAAAAAAAAA", "AAAA", "D", snapshotId1, tbl.snapshot(snapshotId1).timestampMillis(), 0)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);

    // select two snapshots
    result = actions().generateChangeSet(tbl).betweenSnapshots(snapshotId1, snapshotId2.snapshotId()).execute();
    // verify the results
    resultDF = (Dataset<Row>) result.changeSet();
    actualRecords = rowsToJava(resultDF.sort("c1").collectAsList());
    expectedRows = ImmutableList.of(
        row(0, "AAAAAAAAAA", "AAAA", "D", snapshotId1, tbl.snapshot(snapshotId1).timestampMillis(), 0),
        row(1, "AAAAAAAAAA", "AAAA", "D", snapshotId2.snapshotId(), snapshotId2.timestampMillis(), 1)
    );
    assertEquals("Should have expected rows", expectedRows, actualRecords);
  }

  private Table createMetastoreTable(Map<String, String> properties) throws IOException {
    return createMetastoreTable(properties, 1);
  }

  private Table createMetastoreTable(Map<String, String> properties, int rowCount) throws IOException {
    StringBuilder propertiesStr = new StringBuilder();
    properties.forEach((k, v) -> propertiesStr.append("'" + k + "'='" + v + "',"));
    String tblProperties = propertiesStr.substring(0, propertiesStr.length() > 0 ? propertiesStr.length() - 1 : 0);
    String location = newTableLocation();

    if (tblProperties.isEmpty()) {
      sql("CREATE TABLE hive.default.%s (c1 int, c2 string, c3 string) USING iceberg LOCATION '%s'",
          tableName, location);
    } else {
      sql("CREATE TABLE hive.default.%s (c1 int, c2 string, c3 string) USING iceberg LOCATION '%s' TBLPROPERTIES " +
          "(%s)", tableName, location, tblProperties);
    }

    for (int i = 0; i < rowCount; i++) {
      sql("insert into hive.default.%s values (%s, 'AAAAAAAAAA', 'AAAA')", tableName, i);
    }
    return catalog.loadTable(TableIdentifier.of("default", tableName));
  }

  private String newTableLocation() throws IOException {
    return temp.newFolder().toURI().toString();
  }
}
