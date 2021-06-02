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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

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
  public void testGetAllInsertsAcrossIcebergSnapshots() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

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

  @SuppressWarnings("unchecked")
  @Test
  public void testGetAllInsertsAcrossSparkCheckpoints() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");
    File writerCheckpoint = new File(parent, "writer-checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    Table table = tables.create(SCHEMA, spec, location.toString());
    final String tempView = "microBatchView";

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
            new SimpleRecord(7, "seven")),
        Lists.newArrayList(
            new SimpleRecord(8, "eight"),
            new SimpleRecord(9, "nine"))
    );

    // generate multiple snapshots
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> ds = spark.createDataFrame(l, SimpleRecord.class);
      ds.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());
    }

    table.refresh();

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      DataStreamWriter<Row> singleBatchWriter = df.writeStream()
          .trigger(Trigger.Once())
          .option("checkpointLocation", writerCheckpoint.toString())
          .foreachBatch((batchDF, batchId) -> {
            batchDF.createOrReplaceGlobalTempView(tempView);
          });

      String globalTempView = "global_temp." + tempView;
      Assert.assertEquals(expected.get(0), processMicroBatch(singleBatchWriter, globalTempView));
      Assert.assertEquals(expected.get(1), processMicroBatch(singleBatchWriter, globalTempView));
      Assert.assertEquals(expected.get(2), processMicroBatch(singleBatchWriter, globalTempView));
      Assert.assertEquals(expected.get(3), processMicroBatch(singleBatchWriter, globalTempView));
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void validateWhenBatchSizeEquals1ThenOneFileAtATimeIsStreamed() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");
    File writerCheckpoint = new File(parent, "writer-checkpoint");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());
    final String tempView = "microBatchSingleRecordView";

    // produce unique file per record - to test BatchSize=1
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
            new SimpleRecord(7, "seven")),
        Lists.newArrayList(
            new SimpleRecord(8, "eight"),
            new SimpleRecord(9, "nine"))
    );

    // generate multiple snapshots - each snapshot with multiple files
    for (List<SimpleRecord> l : expected) {
      Dataset<Row> ds = spark.createDataFrame(l, SimpleRecord.class);
      ds.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());
    }

    table.refresh();

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .option(SparkReadOptions.VECTORIZATION_BATCH_SIZE, 1)
          .load(location.toString());
      DataStreamWriter<Row> singleBatchWriter = df.writeStream()
          .trigger(Trigger.Once())
          .option("checkpointLocation", writerCheckpoint.toString())
          .foreachBatch((batchDF, batchId) -> {
            batchDF.createOrReplaceGlobalTempView(tempView);
          });

      String globalTempView = "global_temp." + tempView;
      for (SimpleRecord simpleRecord :
          expected.stream().flatMap(List::stream).collect(Collectors.toList())) {
        Assert.assertEquals(
            Collections.singletonList(simpleRecord),
            processMicroBatch(singleBatchWriter, globalTempView));
      }
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testParquetOrcAvroDataInOneTable() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> parquetFileRecords = Lists.newArrayList(
        new SimpleRecord(1, "one"),
        new SimpleRecord(2, "two"),
        new SimpleRecord(3, "three"));

    List<SimpleRecord> orcFileRecords = Lists.newArrayList(
        new SimpleRecord(4, "four"),
        new SimpleRecord(5, "five"));

    List<SimpleRecord> avroFileRecords = Lists.newArrayList(
        new SimpleRecord(6, "six"),
        new SimpleRecord(7, "seven"));

    // generate multiple snapshots
    Dataset<Row> df = spark.createDataFrame(parquetFileRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", "parquet")
        .mode("append")
        .save(location.toString());

    df = spark.createDataFrame(orcFileRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", "orc")
        .mode("append")
        .save(location.toString());

    df = spark.createDataFrame(avroFileRecords, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", "avro")
        .mode("append")
        .save(location.toString());

    table.refresh();
    List<SimpleRecord> actual;

    try {
      Dataset<Row> ds = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      StreamingQuery streamingQuery = ds.writeStream()
          .format("memory")
          .queryName("test12")
          .outputMode(OutputMode.Append())
          .start();
      streamingQuery.processAllAvailable();
      actual = spark.sql("select * from test12")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals(Stream.concat(Stream.concat(parquetFileRecords.stream(), orcFileRecords.stream()),
          avroFileRecords.stream())
          .collect(Collectors.toList()),
          actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamFromEmptyTable() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    table.refresh();
    List<SimpleRecord> actual;

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      StreamingQuery streamingQuery = df.writeStream()
          .format("memory")
          .queryName("testemptytable")
          .outputMode(OutputMode.Append())
          .start();
      streamingQuery.processAllAvailable();
      actual = spark.sql("select * from testemptytable")
          .as(Encoders.bean(SimpleRecord.class))
          .collectAsList();

      Assert.assertEquals(Collections.emptyList(), actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeOverwriteErrorsOut() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    Table table = tables.create(SCHEMA, spec, location.toString());
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three")),
        Lists.newArrayList(
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"))
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

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes = Lists.newArrayList(
        dataDelete.copy("data", "one") // id = 1
    );

    DeleteFile eqDeletes = FileHelpers.writeDeleteFile(
        table, Files.localOutput(temp.newFile()), TestHelpers.Row.of(0), dataDeletes, deleteRowSchema);

    table.newRowDelta()
        .addDeletes(eqDeletes)
        .commit();

    // check pre-condition
    Assert.assertEquals(DataOperations.OVERWRITE, table.currentSnapshot().operation());

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      StreamingQuery streamingQuery = df.writeStream()
          .format("memory")
          .queryName("testtablewithoverwrites")
          .outputMode(OutputMode.Append())
          .start();

      try {
        streamingQuery.processAllAvailable();
        Assert.assertTrue(false); // should be unreachable
      } catch (Exception exception) {
        Assert.assertTrue(exception instanceof StreamingQueryException);
        Assert.assertTrue(((StreamingQueryException) exception).cause() instanceof IllegalStateException);
      }

    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeReplaceErrorsOut() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three")),
        Lists.newArrayList(
            new SimpleRecord(4, "four"),
            new SimpleRecord(5, "five"))
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

    // this should create a snapshot with type Replace.
    table.rewriteManifests()
        .clusterBy(f -> 1)
        .commit();

    // check pre-condition
    Assert.assertEquals(DataOperations.REPLACE, table.currentSnapshot().operation());

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      StreamingQuery streamingQuery = df.writeStream()
          .format("memory")
          .queryName("testtablewithreplace")
          .outputMode(OutputMode.Append())
          .start();

      try {
        streamingQuery.processAllAvailable();
        Assert.assertTrue(false); // should be unreachable
      } catch (Exception exception) {
        Assert.assertTrue(exception instanceof StreamingQueryException);
        Assert.assertTrue(((StreamingQueryException) exception).cause() instanceof IllegalStateException);
      }

    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeDeleteErrorsOut() throws IOException, TimeoutException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<List<SimpleRecord>> expected = Lists.newArrayList(
        Lists.newArrayList(
            new SimpleRecord(1, "one"),
            new SimpleRecord(2, "two"),
            new SimpleRecord(3, "three")),
        Lists.newArrayList(
            new SimpleRecord(4, "four"))
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

    // this should create a snapshot with type delete.
    table.newDelete()
        .deleteFromRowFilter(Expressions.equal("id", 4))
        .commit();

    // check pre-condition
    Assert.assertEquals(DataOperations.DELETE, table.currentSnapshot().operation());

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(location.toString());
      StreamingQuery streamingQuery = df.writeStream()
          .format("memory")
          .queryName("testtablewithdelete")
          .outputMode(OutputMode.Append())
          .start();

      try {
        streamingQuery.processAllAvailable();
        Assert.assertTrue(false); // should be unreachable
      } catch (Exception exception) {
        Assert.assertTrue(exception instanceof StreamingQueryException);
        Assert.assertTrue(((StreamingQueryException) exception).cause() instanceof IllegalStateException);
      }

    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  private static List<SimpleRecord> processMicroBatch(DataStreamWriter<Row> singleBatchWriter, String viewName)
      throws TimeoutException {
    StreamingQuery streamingQuery = singleBatchWriter.start();
    streamingQuery.processAllAvailable();

    return spark.sql(String.format("select * from %s", viewName))
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }
}
