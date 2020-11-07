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

import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.iceberg.Files.localOutput;

public abstract class TestDuplicateFileIncrementalDataTableScan extends TestDuplicateFileDataTableScan {
  private static final Configuration CONF = new Configuration();

  @Override
  protected String testName() {
    return "incremental-duplicate-file-scan";
  }

  protected void addIgnoredSnapshot(Table table, File dataFolder) throws IOException {
    List<Record> ignoredData = RandomData.generateList(table.schema(), 100, 1L);
    File ignoredFile = createFileWithRecords(table.schema(), dataFolder, ignoredData);
    DataFile file = toDataFile(ignoredFile);
    table.newAppend().appendFile(file).commit();
  }

  /**
   * Create 4 snapshots:
   * S0: ignored and provides the start snapshot id
   * S1: holds 2 of the same file
   * S2: holds 1 of the files from S1. is the end snapshot
   * S3: ignored but added in case an optimization switches to fromSnapshot when currentSnapshot is the end-snapshot-id
   */
  @Override
  protected Optional<Pair<Long, Long>> write(Table table, File dataFolder, List<Record> expected) throws IOException {
    File avroFile = createFileWithRecords(table.schema(), dataFolder, expected);
    DataFile file = toDataFile(avroFile);

    addIgnoredSnapshot(table, dataFolder);
    table.refresh();
    long fromSnapshotId = table.currentSnapshot().snapshotId();

    // append the file in a snapshot twice
    table.newAppend().appendFile(file).appendFile(file).commit();
    // append the file in a single snapshot
    table.newAppend().appendFile(file).commit();

    table.refresh();
    long toSnapshotId = table.currentSnapshot().snapshotId();

    addIgnoredSnapshot(table, dataFolder);

    return Optional.of(Pair.of(fromSnapshotId, toSnapshotId));
  }

  @Override
  protected Dataset<Row> read(String location, Optional<Pair<Long, Long>> startEndSnapshot) {
    return spark.read()
        .format("iceberg")
        .option("start-snapshot-id", startEndSnapshot.get().first())
        .option("end-snapshot-id", startEndSnapshot.get().second())
        .load(location);
  }
}
