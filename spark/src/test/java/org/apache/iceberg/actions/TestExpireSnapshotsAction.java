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

package org.apache.iceberg.actions;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.spark.source.ThreeColumnRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestExpireSnapshotsAction extends SparkTestBase {

  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA = new Schema(
      optional(1, "c1", Types.IntegerType.get()),
      optional(2, "c2", Types.StringType.get()),
      optional(3, "c3", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tableDir;
  private String tableLocation = null;

  @Before
  public void setupTableLocation() throws Exception {
    this.tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();
  }

  private void checkExpirationResults(
      ExpireSnapshotActionResult results, Long expectedManifestLists, Long expectedManifests,
      Long expectedDatafiles) {

    Assert.assertEquals("Incorrect number of manifestlists deleted",
        expectedManifestLists, results.getManifestListFilesDeleted());
    Assert.assertEquals("Incorrect number of manifests deleted",
        expectedManifests, results.getManifestFilesDeleted());
    Assert.assertEquals("Incorrect number of datafiles deleted",
        expectedDatafiles, results.getDataFilesDeleted());
  }


  @Test
  public void testFilesCleaned() throws Exception {
    Table table = TABLES
        .create(SCHEMA,
            PartitionSpec.unpartitioned(),
            Maps.newHashMap(),
            tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    List<Path> expiredDataFiles = Files
        .list(tableDir.toPath().resolve("data"))
        .collect(Collectors.toList());

    Assert.assertEquals("There should be a data file to delete but there was none.",
        2, expiredDataFiles.size());

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("overwrite")
        .save(tableLocation);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    long end = System.currentTimeMillis();
    while (end <= table.currentSnapshot().timestampMillis()) {
      end = System.currentTimeMillis();
    }

    ExpireSnapshotActionResult results =
        Actions.forTable(table).expireSnapshots().expireOlderThan(end).execute();

    table.refresh();

    Assert.assertEquals("Table does not have 1 snapshot after expiration", 1,
        Iterables.size(table.snapshots()));

    for (Path p : expiredDataFiles) {
      Assert.assertFalse(String.format("File %s still exists but should have been deleted", p),
          Files.exists(p));
    }

    checkExpirationResults(results, 2L, 2L, 1L);
  }

  @Test
  public void testNoFilesDeletedWhenNoSnapshotsExpired() throws Exception {
    Table table = TABLES
        .create(SCHEMA,
            PartitionSpec.unpartitioned(),
            Maps.newHashMap(),
            tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    ExpireSnapshotActionResult results =
        Actions.forTable(table).expireSnapshots().execute();

    checkExpirationResults(results, 0L, 0L, 0L);
  }

  @Test
  public void testCleanupRepeatedOverwrites() throws Exception {
    Table table = TABLES
        .create(SCHEMA,
            PartitionSpec.unpartitioned(),
            Maps.newHashMap(),
            tableLocation);

    List<ThreeColumnRecord> records = Lists.newArrayList(
        new ThreeColumnRecord(1, "AAAAAAAAAA", "AAAA")
    );

    Dataset<Row> df = spark.createDataFrame(records, ThreeColumnRecord.class).coalesce(1);

    df.select("c1", "c2", "c3")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    for (int i = 0; i < 10; i++) {
      df.select("c1", "c2", "c3")
          .write()
          .format("iceberg")
          .mode("overwrite")
          .save(tableLocation);
    }

    long end = System.currentTimeMillis();
    while (end <= table.currentSnapshot().timestampMillis()) {
      end = System.currentTimeMillis();
    }

    ExpireSnapshotActionResult results =
        Actions.forTable(table).expireSnapshots().expireOlderThan(end).execute();

    table.refresh();
    checkExpirationResults(results, 10L, 19L, 9L);
  }

}

