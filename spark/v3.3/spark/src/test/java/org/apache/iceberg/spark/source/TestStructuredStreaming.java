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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.streaming.MemoryStream;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import scala.Option;
import scala.collection.JavaConverters;

public class TestStructuredStreaming {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static SparkSession spark = null;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    TestStructuredStreaming.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.shuffle.partitions", 4)
            .getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestStructuredStreaming.spark;
    TestStructuredStreaming.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testStreamingWriteAppendMode() throws Exception {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test-table");
    File checkpoint = new File(parent, "checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, "1"),
            new SimpleRecord(2, "2"),
            new SimpleRecord(3, "3"),
            new SimpleRecord(4, "4"));

    MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
    DataStreamWriter<Row> streamWriter =
        inputStream
            .toDF()
            .selectExpr("value AS id", "CAST (value AS STRING) AS data")
            .writeStream()
            .outputMode("append")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString());

    try {
      // start the original query with checkpointing
      StreamingQuery query = streamWriter.start();
      List<Integer> batch1 = Lists.newArrayList(1, 2);
      send(batch1, inputStream);
      query.processAllAvailable();
      List<Integer> batch2 = Lists.newArrayList(3, 4);
      send(batch2, inputStream);
      query.processAllAvailable();
      query.stop();

      // remove the last commit to force Spark to reprocess batch #1
      File lastCommitFile = new File(checkpoint + "/commits/1");
      Assert.assertTrue("The commit file must be deleted", lastCommitFile.delete());

      // restart the query from the checkpoint
      StreamingQuery restartedQuery = streamWriter.start();
      restartedQuery.processAllAvailable();

      // ensure the write was idempotent
      Dataset<Row> result = spark.read().format("iceberg").load(location.toString());
      List<SimpleRecord> actual =
          result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
      Assert.assertEquals("Result rows should match", expected, actual);
      Assert.assertEquals("Number of snapshots should match", 2, Iterables.size(table.snapshots()));
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @Test
  public void testStreamingWriteCompleteMode() throws Exception {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test-table");
    File checkpoint = new File(parent, "checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(2, "1"), new SimpleRecord(3, "2"), new SimpleRecord(1, "3"));

    MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
    DataStreamWriter<Row> streamWriter =
        inputStream
            .toDF()
            .groupBy("value")
            .count()
            .selectExpr("CAST(count AS INT) AS id", "CAST (value AS STRING) AS data")
            .writeStream()
            .outputMode("complete")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString());

    try {
      // start the original query with checkpointing
      StreamingQuery query = streamWriter.start();
      List<Integer> batch1 = Lists.newArrayList(1, 2);
      send(batch1, inputStream);
      query.processAllAvailable();
      List<Integer> batch2 = Lists.newArrayList(1, 2, 2, 3);
      send(batch2, inputStream);
      query.processAllAvailable();
      query.stop();

      // remove the last commit to force Spark to reprocess batch #1
      File lastCommitFile = new File(checkpoint + "/commits/1");
      Assert.assertTrue("The commit file must be deleted", lastCommitFile.delete());

      // restart the query from the checkpoint
      StreamingQuery restartedQuery = streamWriter.start();
      restartedQuery.processAllAvailable();

      // ensure the write was idempotent
      Dataset<Row> result = spark.read().format("iceberg").load(location.toString());
      List<SimpleRecord> actual =
          result.orderBy("data").as(Encoders.bean(SimpleRecord.class)).collectAsList();
      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
      Assert.assertEquals("Result rows should match", expected, actual);
      Assert.assertEquals("Number of snapshots should match", 2, Iterables.size(table.snapshots()));
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @Test
  public void testStreamingWriteCompleteModeWithProjection() throws Exception {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test-table");
    File checkpoint = new File(parent, "checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected =
        Lists.newArrayList(
            new SimpleRecord(1, null), new SimpleRecord(2, null), new SimpleRecord(3, null));

    MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
    DataStreamWriter<Row> streamWriter =
        inputStream
            .toDF()
            .groupBy("value")
            .count()
            .selectExpr("CAST(count AS INT) AS id") // select only id column
            .writeStream()
            .outputMode("complete")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString());

    try {
      // start the original query with checkpointing
      StreamingQuery query = streamWriter.start();
      List<Integer> batch1 = Lists.newArrayList(1, 2);
      send(batch1, inputStream);
      query.processAllAvailable();
      List<Integer> batch2 = Lists.newArrayList(1, 2, 2, 3);
      send(batch2, inputStream);
      query.processAllAvailable();
      query.stop();

      // remove the last commit to force Spark to reprocess batch #1
      File lastCommitFile = new File(checkpoint + "/commits/1");
      Assert.assertTrue("The commit file must be deleted", lastCommitFile.delete());

      // restart the query from the checkpoint
      StreamingQuery restartedQuery = streamWriter.start();
      restartedQuery.processAllAvailable();

      // ensure the write was idempotent
      Dataset<Row> result = spark.read().format("iceberg").load(location.toString());
      List<SimpleRecord> actual =
          result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
      Assert.assertEquals("Result rows should match", expected, actual);
      Assert.assertEquals("Number of snapshots should match", 2, Iterables.size(table.snapshots()));
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @Test
  public void testStreamingWriteUpdateMode() throws Exception {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test-table");
    File checkpoint = new File(parent, "checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    tables.create(SCHEMA, spec, location.toString());

    MemoryStream<Integer> inputStream = newMemoryStream(1, spark.sqlContext(), Encoders.INT());
    DataStreamWriter<Row> streamWriter =
        inputStream
            .toDF()
            .selectExpr("value AS id", "CAST (value AS STRING) AS data")
            .writeStream()
            .outputMode("update")
            .format("iceberg")
            .option("checkpointLocation", checkpoint.toString())
            .option("path", location.toString());

    try {
      StreamingQuery query = streamWriter.start();
      List<Integer> batch1 = Lists.newArrayList(1, 2);
      send(batch1, inputStream);

      assertThatThrownBy(query::processAllAvailable)
          .isInstanceOf(StreamingQueryException.class)
          .hasMessageContaining("does not support Update mode");
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  private <T> MemoryStream<T> newMemoryStream(int id, SQLContext sqlContext, Encoder<T> encoder) {
    return new MemoryStream<>(id, sqlContext, Option.empty(), encoder);
  }

  private <T> void send(List<T> records, MemoryStream<T> stream) {
    stream.addData(JavaConverters.asScalaBuffer(records));
  }
}
