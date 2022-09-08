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
package org.apache.iceberg.examples;

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.io.FileUtils;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class tests the snapshot functionality available with Iceberg. This includes things like
 * time-travel, rollback and retrieving metadata.
 */
public class SnapshotFunctionalityTest {

  private static final Logger log = LoggerFactory.getLogger(SnapshotFunctionalityTest.class);

  private Table table;
  private File tableLocation;
  private SparkSession spark = null;

  @Before
  public void before() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()));

    spark = SparkSession.builder().master("local[2]").getOrCreate();

    tableLocation = Files.createTempDirectory("temp").toFile();

    HadoopTables tables = new HadoopTables(spark.sessionState().newHadoopConf());
    PartitionSpec spec = PartitionSpec.unpartitioned();
    table = tables.create(schema, spec, tableLocation.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "a"), new SimpleRecord(2, "b"), new SimpleRecord(3, "c"));

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    for (int i = 0; i < 5; i++) {
      df.select("id", "data")
          .write()
          .format("iceberg")
          .mode("append")
          .save(tableLocation.toString());
    }
    table.refresh();
  }

  @Test
  public void rollbackToPreviousSnapshotAndReadData() {
    long oldId = table.history().get(0).snapshotId();

    table.manageSnapshots().rollbackTo(oldId).commit();
    table.refresh();

    Dataset<Row> results = spark.read().format("iceberg").load(tableLocation.toString());

    results.createOrReplaceTempView("table");
    spark.sql("select * from table").show();
  }

  @Test
  public void expireOldSnapshotWithSnapshotID() {
    long oldId = table.history().get(0).snapshotId();

    table.expireSnapshots().expireSnapshotId(oldId).commit();
    table.refresh();

    Iterator<Snapshot> iterator = table.snapshots().iterator();
    List<Snapshot> snapshots = IteratorUtils.toList(iterator);
  }

  /** Expires anything older than a given timestamp, NOT including that timestamp. */
  @Test
  public void retireAllSnapshotsOlderThanTimestamp() {
    long secondLatestTimestamp = table.history().get(2).timestampMillis();
    Iterator<Snapshot> beforeIterator = table.snapshots().iterator();
    List<Snapshot> beforeSnapshots = IteratorUtils.toList(beforeIterator);

    // Delete the 2 oldest snapshots
    table.expireSnapshots().expireOlderThan(secondLatestTimestamp).commit();
    table.refresh();

    Iterator<Snapshot> afterIterator = table.snapshots().iterator();
    List<Snapshot> afterSnapshots = IteratorUtils.toList(afterIterator);
  }

  @Test
  public void getInfoAboutFilesAddedFromSnapshot() {
    Snapshot snapshot = table.currentSnapshot();
    Iterable<DataFile> addedFiles = snapshot.addedDataFiles(table.io());

    for (DataFile dataFile : addedFiles) {
      log.info("File path: " + dataFile.path());
      log.info("File format: " + dataFile.format());
      log.info("File size in bytes: " + dataFile.fileSizeInBytes());
      log.info("Record count: " + dataFile.recordCount());
    }
  }

  @After
  public void after() throws IOException {
    FileUtils.deleteDirectory(tableLocation);
    spark.stop();
  }
}
