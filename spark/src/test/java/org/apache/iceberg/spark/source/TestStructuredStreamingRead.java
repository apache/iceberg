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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import static org.apache.iceberg.types.Types.NestedField.optional;

public abstract class TestStructuredStreamingRead {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );
  protected static final int INIT_SCANNED_FILE_INDEX = -1;
  protected static final int END_SCANNED_FILE_INDEX = 3;
  protected static SparkSession spark = null;
  protected static Path parent = null;
  protected static File tableLocation = null;
  protected static Table table = null;
  protected static List<SimpleRecord> expected = null;
  protected static final FileIO FILE_IO = new TestTables.LocalFileIO();

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();
  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() throws IOException {
    TestStructuredStreamingRead.spark = SparkSession.builder()
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", 4)
        .getOrCreate();

    parent = Files.createTempDirectory("test");
    tableLocation = new File(parent.toFile(), "table");
    tableLocation.mkdir();
    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    table = tables.create(SCHEMA, spec, tableLocation.toString());

    expected = Lists.newArrayList(new SimpleRecord(1, "1"),
        new SimpleRecord(2, "2"),
        new SimpleRecord(3, "3"),
        new SimpleRecord(4, "4"));

    // Write records one by one to generate 3 snapshots.
    for (int i = 0; i < 3; i++) {
      Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(tableLocation.toString());
    }

    table.refresh();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead.spark;
    TestStructuredStreamingRead.spark = null;
    currentSpark.stop();
  }
}
