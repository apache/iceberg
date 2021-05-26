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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.JavaConversions;

import static org.apache.iceberg.types.Types.NestedField.optional;

public final class TestStructuredStreamingRead3 {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get())
  );
  private static SparkSession spark = null;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @BeforeClass
  public static void startSpark() {
    TestStructuredStreamingRead3.spark = SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreamingRead3.spark;
    TestStructuredStreamingRead3.spark = null;
    currentSpark.stop();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAllAppendsFromStartAcrossMultipleSnapshots() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");
    File checkpoint = new File(parent, "checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(
          new SimpleRecord(1, "one"),
          new SimpleRecord(2, "two"),
          new SimpleRecord(3, "three")),
        Lists.newArrayList(
          new SimpleRecord(4, "four"),
          new SimpleRecord(5, "five")),
        Lists.newArrayList(
          new SimpleRecord(6, "six"),
          new SimpleRecord(7, "seven"))
    );

    // generate multiple snapshots
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> df = spark.createDataFrame(l, SimpleRecord.class);
      df.select("id", "data").write()
              .format("iceberg")
              .mode("append")
              .save(location.toString());
    }

    table.refresh();
    List<SimpleRecord> actual;

    try {
      Dataset<Row> df = spark.readStream()
              .format("iceberg")
              .option("checkpointLocation", checkpoint.toString())
              .load(location.toString());
      StreamingQuery streamingQuery = df.writeStream()
              .format("memory")
              .queryName("test12")
              .outputMode(OutputMode.Append())
              .start();
      streamingQuery.processAllAvailable();
      actual = spark.sql("select * from test12")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals(
          expected.stream().flatMap(List::stream).collect(Collectors.toList()),
          actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
          query.stop();
      }
    }
  }
}
