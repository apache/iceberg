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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergSourceHiveTables {

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  private static SparkSession spark;
  private static TestHiveMetastore metastore;
  private static HiveClientPool clients;
  private static HiveConf hiveConf;
  private static HiveCatalog catalog;

  @BeforeClass
  public static void startMetastoreAndSpark() throws Exception {
    TestIcebergSourceHiveTables.metastore = new TestHiveMetastore();
    metastore.start();
    TestIcebergSourceHiveTables.hiveConf = metastore.hiveConf();
    String dbPath = metastore.getDatabasePath("db");
    Database db = new Database("db", "desc", dbPath, new HashMap<>());
    TestIcebergSourceHiveTables.clients = new HiveClientPool(1, hiveConf);
    clients.run(client -> {
      client.createDatabase(db);
      return null;
    });

    TestIcebergSourceHiveTables.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
        .getOrCreate();

    TestIcebergSourceHiveTables.catalog = new HiveCatalog(hiveConf);
  }

  @AfterClass
  public static void stopMetastoreAndSpark() {
    catalog.close();
    TestIcebergSourceHiveTables.catalog = null;
    clients.close();
    TestIcebergSourceHiveTables.clients = null;
    metastore.stop();
    TestIcebergSourceHiveTables.metastore = null;
    spark.stop();
    TestIcebergSourceHiveTables.spark = null;
  }

  @Test
  public synchronized void testHiveTablesSupport() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "table");
    try {
      catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());

      List<SimpleRecord> expectedRecords = Lists.newArrayList(
          new SimpleRecord(1, "1"),
          new SimpleRecord(2, "2"),
          new SimpleRecord(3, "3"));

      Dataset<Row> inputDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode(SaveMode.Append)
          .save(tableIdentifier.toString());

      Dataset<Row> resultDf = spark.read()
          .format("iceberg")
          .load(tableIdentifier.toString());
      List<SimpleRecord> actualRecords = resultDf.orderBy("id")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals("Records should match", expectedRecords, actualRecords);
    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveEntriesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "entries_test");
    try {
      Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
      Table entriesTable = catalog.loadTable(TableIdentifier.of("db", "entries_test", "entries"));

      List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));

      Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);
      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.entries_test.entries")
          .collectAsList();

      Assert.assertEquals("Should only contain one manifest", 1, table.currentSnapshot().manifests().size());
      InputFile manifest = table.io().newInputFile(table.currentSnapshot().manifests().get(0).path());
      List<GenericData.Record> expected;
      try (CloseableIterable<GenericData.Record> rows = Avro.read(manifest).project(entriesTable.schema()).build()) {
        expected = Lists.newArrayList(rows);
      }

      Assert.assertEquals("Entries table should have one row", 1, expected.size());
      Assert.assertEquals("Actual results should have one row", 1, actual.size());
      TestHelpers.assertEqualsSafe(entriesTable.schema().asStruct(), expected.get(0), actual.get(0));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveFilesTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "files_test");
    try {
      Table table = catalog.createTable(tableIdentifier, SCHEMA,
          PartitionSpec.builderFor(SCHEMA).identity("id").build());
      Table entriesTable = catalog.loadTable(TableIdentifier.of("db", "files_test", "entries"));
      Table filesTable = catalog.loadTable(TableIdentifier.of("db", "files_test", "files"));

      Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
      Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

      df1.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      // add a second file
      df2.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      // delete the first file to test that only live files are listed
      table.newDelete().deleteFromRowFilter(Expressions.equal("id", 1)).commit();

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.files_test.files")
          .collectAsList();

      List<GenericData.Record> expected = Lists.newArrayList();
      for (ManifestFile manifest : table.currentSnapshot().manifests()) {
        InputFile in = table.io().newInputFile(manifest.path());
        try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
          for (GenericData.Record record : rows) {
            if ((Integer) record.get("status") < 2 /* added or existing */) {
              expected.add((GenericData.Record) record.get("data_file"));
            }
          }
        }
      }

      Assert.assertEquals("Files table should have one row", 1, expected.size());
      Assert.assertEquals("Actual results should have one row", 1, actual.size());
      TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(0), actual.get(0));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveFilesUnpartitionedTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "unpartitioned_files_test");
    try {
      Table table = catalog.createTable(tableIdentifier, SCHEMA);
      Table entriesTable = catalog.loadTable(TableIdentifier.of("db", "unpartitioned_files_test", "entries"));
      Table filesTable = catalog.loadTable(TableIdentifier.of("db", "unpartitioned_files_test", "files"));

      Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);
      Dataset<Row> df2 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(2, "b")), SimpleRecord.class);

      df1.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();
      DataFile toDelete = Iterables.getOnlyElement(table.currentSnapshot().addedFiles());

      // add a second file
      df2.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      // delete the first file to test that only live files are listed
      table.newDelete().deleteFile(toDelete).commit();

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.unpartitioned_files_test.files")
          .collectAsList();

      List<GenericData.Record> expected = Lists.newArrayList();
      for (ManifestFile manifest : table.currentSnapshot().manifests()) {
        InputFile in = table.io().newInputFile(manifest.path());
        try (CloseableIterable<GenericData.Record> rows = Avro.read(in).project(entriesTable.schema()).build()) {
          for (GenericData.Record record : rows) {
            if ((Integer) record.get("status") < 2 /* added or existing */) {
              expected.add((GenericData.Record) record.get("data_file"));
            }
          }
        }
      }

      Assert.assertEquals("Files table should have one row", 1, expected.size());
      Assert.assertEquals("Actual results should have one row", 1, actual.size());
      TestHelpers.assertEqualsSafe(filesTable.schema().asStruct(), expected.get(0), actual.get(0));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveHistoryTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "history_test");
    try {
      Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
      Table historyTable = catalog.loadTable(TableIdentifier.of("db", "history_test", "history"));

      List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
      Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);

      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();
      long firstSnapshotTimestamp = table.currentSnapshot().timestampMillis();
      long firstSnapshotId = table.currentSnapshot().snapshotId();

      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();
      long secondSnapshotTimestamp = table.currentSnapshot().timestampMillis();
      long secondSnapshotId = table.currentSnapshot().snapshotId();

      // rollback the table state to the first snapshot
      table.rollback().toSnapshotId(firstSnapshotId).commit();
      long rollbackTimestamp = Iterables.getLast(table.history()).timestampMillis();

      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();
      long thirdSnapshotTimestamp = table.currentSnapshot().timestampMillis();
      long thirdSnapshotId = table.currentSnapshot().snapshotId();

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.history_test.history")
          .collectAsList();

      GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(historyTable.schema(), "history"));
      List<GenericData.Record> expected = Lists.newArrayList(
          builder.set("made_current_at", firstSnapshotTimestamp * 1000)
              .set("snapshot_id", firstSnapshotId)
              .set("parent_id", null)
              .set("is_current_ancestor", true)
              .build(),
          builder.set("made_current_at", secondSnapshotTimestamp * 1000)
              .set("snapshot_id", secondSnapshotId)
              .set("parent_id", firstSnapshotId)
              .set("is_current_ancestor", false) // commit rolled back, not an ancestor of the current table state
              .build(),
          builder.set("made_current_at", rollbackTimestamp * 1000)
              .set("snapshot_id", firstSnapshotId)
              .set("parent_id", null)
              .set("is_current_ancestor", true)
              .build(),
          builder.set("made_current_at", thirdSnapshotTimestamp * 1000)
              .set("snapshot_id", thirdSnapshotId)
              .set("parent_id", firstSnapshotId)
              .set("is_current_ancestor", true)
              .build()
      );

      Assert.assertEquals("History table should have a row for each commit", 4, actual.size());
      TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(0), actual.get(0));
      TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(1), actual.get(1));
      TestHelpers.assertEqualsSafe(historyTable.schema().asStruct(), expected.get(2), actual.get(2));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveSnapshotsTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "snapshots_test");
    try {
      Table table = catalog.createTable(tableIdentifier, SCHEMA, PartitionSpec.unpartitioned());
      Table snapTable = catalog.loadTable(TableIdentifier.of("db", "snapshots_test", "snapshots"));

      List<SimpleRecord> records = Lists.newArrayList(new SimpleRecord(1, "1"));
      Dataset<Row> inputDf = spark.createDataFrame(records, SimpleRecord.class);

      inputDf.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      table.refresh();
      long firstSnapshotTimestamp = table.currentSnapshot().timestampMillis();
      long firstSnapshotId = table.currentSnapshot().snapshotId();
      String firstManifestList = table.currentSnapshot().manifestListLocation();

      table.newDelete().deleteFromRowFilter(Expressions.alwaysTrue()).commit();

      long secondSnapshotTimestamp = table.currentSnapshot().timestampMillis();
      long secondSnapshotId = table.currentSnapshot().snapshotId();
      String secondManifestList = table.currentSnapshot().manifestListLocation();

      // rollback the table state to the first snapshot
      table.rollback().toSnapshotId(firstSnapshotId).commit();

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.snapshots_test.snapshots")
          .collectAsList();

      GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(snapTable.schema(), "snapshots"));
      List<GenericData.Record> expected = Lists.newArrayList(
          builder.set("committed_at", firstSnapshotTimestamp * 1000)
              .set("snapshot_id", firstSnapshotId)
              .set("parent_id", null)
              .set("operation", "append")
              .set("manifest_list", firstManifestList)
              .set("summary", ImmutableMap.of(
                  "added-records", "1",
                  "added-data-files", "1",
                  "changed-partition-count", "1",
                  "total-data-files", "1",
                  "total-records", "1"
              ))
              .build(),
          builder.set("committed_at", secondSnapshotTimestamp * 1000)
              .set("snapshot_id", secondSnapshotId)
              .set("parent_id", firstSnapshotId)
              .set("operation", "delete")
              .set("manifest_list", secondManifestList)
              .set("summary", ImmutableMap.of(
                  "deleted-records", "1",
                  "deleted-data-files", "1",
                  "changed-partition-count", "1",
                  "total-records", "0",
                  "total-data-files", "0"
              ))
              .build()
      );

      Assert.assertEquals("Snapshots table should have a row for each snapshot", 2, actual.size());
      TestHelpers.assertEqualsSafe(snapTable.schema().asStruct(), expected.get(0), actual.get(0));
      TestHelpers.assertEqualsSafe(snapTable.schema().asStruct(), expected.get(1), actual.get(1));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }

  @Test
  public synchronized void testHiveManifestsTable() throws Exception {
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "manifests_test");
    try {
      Table table = catalog.createTable(
          tableIdentifier,
          SCHEMA,
          PartitionSpec.builderFor(SCHEMA).identity("id").build());
      Table manifestTable = catalog.loadTable(TableIdentifier.of("db", "manifests_test", "manifests"));

      Dataset<Row> df1 = spark.createDataFrame(Lists.newArrayList(new SimpleRecord(1, "a")), SimpleRecord.class);

      df1.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableIdentifier.toString());

      List<Row> actual = spark.read()
          .format("iceberg")
          .load("db.manifests_test.manifests")
          .collectAsList();

      table.refresh();

      GenericRecordBuilder builder = new GenericRecordBuilder(AvroSchemaUtil.convert(
          manifestTable.schema(), "manifests"));
      GenericRecordBuilder summaryBuilder = new GenericRecordBuilder(AvroSchemaUtil.convert(
          manifestTable.schema().findType("partition_summaries.element").asStructType(), "partition_summary"));
      List<GenericData.Record> expected = Lists.transform(table.currentSnapshot().manifests(), manifest ->
          builder.set("path", manifest.path())
              .set("length", manifest.length())
              .set("partition_spec_id", manifest.partitionSpecId())
              .set("added_snapshot_id", manifest.snapshotId())
              .set("added_data_files_count", manifest.addedFilesCount())
              .set("existing_data_files_count", manifest.existingFilesCount())
              .set("deleted_data_files_count", manifest.deletedFilesCount())
              .set("partition_summaries", Lists.transform(manifest.partitions(), partition ->
                  summaryBuilder
                      .set("contains_null", false)
                      .set("lower_bound", "1")
                      .set("upper_bound", "1")
                      .build()
                  ))
              .build()
      );

      Assert.assertEquals("Manifests table should have one manifest row", 1, actual.size());
      TestHelpers.assertEqualsSafe(manifestTable.schema().asStruct(), expected.get(0), actual.get(0));

    } finally {
      clients.run(client -> {
        client.dropTable(tableIdentifier.namespace().level(0), tableIdentifier.name());
        return null;
      });
    }
  }
}
