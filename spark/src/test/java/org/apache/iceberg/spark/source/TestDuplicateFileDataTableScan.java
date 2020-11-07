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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
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


import static org.apache.iceberg.Files.localOutput;

public abstract class TestDuplicateFileDataTableScan extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestDuplicateFileDataTableScan.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestDuplicateFileDataTableScan.spark;
    TestDuplicateFileDataTableScan.spark = null;
    currentSpark.stop();
  }

  protected String testName() {
    return "duplicate-file-scan";
  }

  protected File createFileWithRecords(Schema tableSchema, File dataFolder, List<Record> records) throws IOException {
    File avroFile = new File(dataFolder, FileFormat.AVRO.addExtension(UUID.randomUUID().toString()));
    try (FileAppender<Record> writer = Avro.write(localOutput(avroFile))
        .schema(tableSchema)
        .build()) {
      writer.addAll(records);
    }
    return avroFile;
  }

  protected DataFile toDataFile(File avroFile) {
    return DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(100)
        .withFileSizeInBytes(avroFile.length())
        .withPath(avroFile.toString())
        .build();
  }

  /**
   * Create 2 snapshots:
   * S0: holds 2 of the same file
   * S1: holds 1 of the files from S1. is the end snapshot
   */
  protected Optional<Pair<Long, Long>> write(Table table, File dataFolder, List<Record> expected) throws IOException {
    File avroFile = createFileWithRecords(table.schema(), dataFolder, expected);
    DataFile file = toDataFile(avroFile);
    // append the file in a snapshot twice
    table.newAppend().appendFile(file).appendFile(file).commit();
    // append the file in a single snapshot
    table.newAppend().appendFile(file).commit();
    return Optional.empty();
  }

  protected Dataset<Row> read(String location, Optional<Pair<Long, Long>> startEndSnapshot) {
    return spark.read()
        .format("iceberg")
        .load(location);
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    File parent = temp.newFolder(testName());
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    dataFolder.mkdirs();

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location.toString());
    // Important: use the table's schema for the rest of the test
    // When tables are created, the column ids are reassigned.
    Schema tableSchema = table.schema();

    List<Record> expected = RandomData.generateList(tableSchema, 100, 1L);

    Optional<Pair<Long, Long>> startEndSnapshot = write(table, dataFolder, expected);

    // first verify that the files are normally read multiple times
    List<Row> rows = read(table.location(), startEndSnapshot).collectAsList();
    Assert.assertEquals("Should contain 300 rows", 300, rows.size());

    // then verify that the files are deduplicated with the property is set
    table
        .updateProperties()
        .set(TableProperties.DEDUPE_DUPLICATE_FILES_IN_SCAN, "true")
        .commit();
    List<Row> dedupeFileRows = read(table.location(), startEndSnapshot).collectAsList();
    Assert.assertEquals("Should contain 100 rows", 100, dedupeFileRows.size());

    for (int i = 0; i < expected.size(); i += 1) {
      TestHelpers.assertEqualsSafe(tableSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
