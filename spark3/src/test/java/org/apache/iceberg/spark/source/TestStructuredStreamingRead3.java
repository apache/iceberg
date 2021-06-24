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
import java.util.Map;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
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
import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public final class TestStructuredStreamingRead3 extends SparkCatalogTestBase {

  public TestStructuredStreamingRead3(String catalogName, String implementation, Map<String, String> config, boolean fileBased)
  {
    super(catalogName, implementation, config);
    this.fileBased = fileBased;
  }

  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}, fileBased = {3}")
  public static Object[][] parameters() {
    return new Object[][] {
            {
              "testhive",
               SparkCatalog.class.getName(),
               ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default"
               ),
               false
            },
            {
              "testhadoop",
              SparkCatalog.class.getName(),
              ImmutableMap.of(
                "type", "hadoop",
                "default-namespace", "default"
              ),
              false
            },
            {
              "spark_catalog", SparkSessionCatalog.class.getName(),
              ImmutableMap.of(
                "type", "hive",
                "default-namespace", "default",
                "parquet-enabled", "true",
                "cache-enabled", "false" // Spark will delete tables using v1, leaving the cache out of sync
              ),
              false
            },
            {
              "dummy_catalog",
              SparkCatalog.class.getName(),
              ImmutableMap.of(
                "type", "hadoop"
              ),
              true
            }
    };
  }

  private final boolean fileBased;
  private Table table;
  private String tableIdentifier;

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  /**
   * test data to be used by multiple writes
   * each write creates a snapshot and writes a list of records
   */
  private static final List<List<SimpleRecord>> TEST_DATA_MULTIPLE_SNAPSHOTS = Lists.newArrayList(
      Lists.newArrayList(
          new SimpleRecord(1, "one"),
          new SimpleRecord(2, "two"),
          new SimpleRecord(3, "three")),
      Lists.newArrayList(
          new SimpleRecord(4, "four"),
          new SimpleRecord(5, "five")),
      Lists.newArrayList(
          new SimpleRecord(6, "six"),
          new SimpleRecord(7, "seven")));

  /**
   * test data - to be used for multiple write batches
   * each batch inturn will have multiple snapshots
   */
  private static final List<List<List<SimpleRecord>>> TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS = Lists.newArrayList(
      Lists.newArrayList(
          Lists.newArrayList(
              new SimpleRecord(1, "one"),
              new SimpleRecord(2, "two"),
              new SimpleRecord(3, "three")),
          Lists.newArrayList(
              new SimpleRecord(4, "four"),
              new SimpleRecord(5, "five"))),
      Lists.newArrayList(
          Lists.newArrayList(
              new SimpleRecord(6, "six"),
              new SimpleRecord(7, "seven")),
          Lists.newArrayList(
              new SimpleRecord(8, "eight"),
              new SimpleRecord(9, "nine"))),
      Lists.newArrayList(
          Lists.newArrayList(
              new SimpleRecord(10, "ten"),
              new SimpleRecord(11, "eleven"),
              new SimpleRecord(12, "twelve")),
          Lists.newArrayList(
              new SimpleRecord(13, "thirteen"),
              new SimpleRecord(14, "fourteen")),
          Lists.newArrayList(
              new SimpleRecord(15, "fifteen"),
              new SimpleRecord(16, "sixteen"))));

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

  @Before
  public void setupTable() throws IOException {
    if (this.fileBased) {
      this.setupFileBasedTable();
    }
    else {
      this.setupCatalogBasedTable();
    }
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamOnIcebergTableWithMultipleSnapshots() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected, this.tableIdentifier);

    table.refresh();

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(this.tableIdentifier);
      List<SimpleRecord> actual = processStreamTillEnd(df);

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
  public void testResumingStreamReadFromCheckpoint() throws Exception {
    File writerCheckpointFolder = temp.newFolder("writer-checkpoint-folder");
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    final String tempView = "microBatchView";

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(tableIdentifier);
      DataStreamWriter<Row> singleBatchWriter = df.writeStream()
          .trigger(Trigger.Once())
          .option("checkpointLocation", writerCheckpoint.toString())
          .foreachBatch((batchDF, batchId) -> {
            batchDF.createOrReplaceGlobalTempView(tempView);
          });

      String globalTempView = "global_temp." + tempView;

      List<SimpleRecord> processStreamOnEmptyIcebergTable = processMicroBatch(singleBatchWriter, globalTempView);
      Assert.assertEquals(
          Collections.emptyList(),
          processStreamOnEmptyIcebergTable);

      for (List<List<SimpleRecord>> expectedCheckpoint : TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS) {
        appendDataAsMultipleSnapshots(expectedCheckpoint, tableIdentifier);
        table.refresh();

        List<SimpleRecord> actualDataInCurrentMicroBatch = processMicroBatch(singleBatchWriter, globalTempView);
        Assert.assertEquals(
            expectedCheckpoint.stream().flatMap(List::stream).collect(Collectors.toList()),
            actualDataInCurrentMicroBatch);
      }
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testParquetOrcAvroDataInOneTable() throws Exception {
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

    appendData(parquetFileRecords, tableIdentifier, "parquet");
    appendData(orcFileRecords, tableIdentifier, "orc");
    appendData(avroFileRecords, tableIdentifier, "avro");

    table.refresh();


    try {
      Dataset<Row> ds = spark.readStream()
          .format("iceberg")
          .load(tableIdentifier);
      List<SimpleRecord> actual = processStreamTillEnd(ds);
      List<SimpleRecord> expected = Stream.concat(Stream.concat(
          parquetFileRecords.stream(),
          orcFileRecords.stream()),
          avroFileRecords.stream())
            .collect(Collectors.toList());
      Assert.assertEquals(expected, actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamFromEmptyTable() throws Exception {
    table.refresh();

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(tableIdentifier);

      List<SimpleRecord> actual = processStreamTillEnd(df);
      Assert.assertEquals(Collections.emptyList(), actual);
    } finally {
      for (StreamingQuery query : spark.streams().active()) {
        query.stop();
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeOverwriteErrorsOut() throws Exception {
    // upgrade table to verison 2 - to facilitate creation of Snapshot of type OVERWRITE.
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots, tableIdentifier);

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

    // check pre-condition - that the above Delete file write - actually resulted in snapshot of type OVERWRITE
    Assert.assertEquals(DataOperations.OVERWRITE, table.currentSnapshot().operation());

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(tableIdentifier);
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
  public void testReadStreamWithSnapshotTypeReplaceErrorsOut() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots, tableIdentifier);

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
          .load(tableIdentifier);
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
  public void testReadStreamWithSnapshotTypeDeleteErrorsOut() throws Exception {
    table.updateSpec()
            .removeField("id_bucket")
            .addField(ref("id"))
            .commit();

    table.refresh();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots, tableIdentifier);

    table.refresh();

    // this should create a snapshot with type delete.
    table.newDelete()
        .deleteFromRowFilter(Expressions.equal("id", 4))
        .commit();

    // check pre-condition - that the above newDelete operation on table resulted in Snapshot of Type DELETE.
    Assert.assertEquals(DataOperations.DELETE, table.currentSnapshot().operation());

    try {
      Dataset<Row> df = spark.readStream()
          .format("iceberg")
          .load(tableIdentifier);
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
      throws TimeoutException, StreamingQueryException {
    StreamingQuery streamingQuery = singleBatchWriter.start();
    streamingQuery.awaitTermination();

    return spark.sql(String.format("select * from %s", viewName))
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }

  /**
   * appends each list as a Snapshot on the iceberg table at the given location.
   * accepts a list of lists - each list representing data per snapshot.
   */
  private static void appendDataAsMultipleSnapshots(List<List<SimpleRecord>> data, String tableIdentifier) {
    for (List<SimpleRecord> l : data) {
      appendData(l, tableIdentifier, "parquet");
    }
  }

  private static void appendData(List<SimpleRecord> data, String tableIdentifier, String fileFormat) {
    Dataset<Row> df = spark.createDataFrame(data, SimpleRecord.class);
    df.select("id", "data").write()
        .format("iceberg")
        .option("write-format", fileFormat)
        .mode("append")
        .save(tableIdentifier);
  }

  private static List<SimpleRecord> processStreamTillEnd(Dataset<Row> df) throws TimeoutException {
    StreamingQuery streamingQuery = df.writeStream()
        .format("memory")
        .queryName("test12")
        .outputMode(OutputMode.Append())
        .start();
    streamingQuery.processAllAvailable();
    return spark.sql("select * from test12")
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }

  private void setupFileBasedTable() throws IOException {
    File parent = temp.newFolder("parent");
    File location = new File(parent, "test-table");

    HadoopTables tables = new HadoopTables(CONF);

    // use partition buckets - so that single list<SimpleRecord> can generate multiple files per write
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).bucket("id", 3).build();
    this.table = tables.create(SCHEMA, spec, location.toString());
    this.tableIdentifier = location.toString();
  }

  private void setupCatalogBasedTable() {
    sql("CREATE TABLE %s " +
            "(id INT, data STRING) " +
            "USING iceberg " +
            "PARTITIONED BY (bucket(3, id))", tableName);
    this.table = validationCatalog.loadTable(tableIdent);
    this.tableIdentifier = tableName;
  }
}
