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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.expressions.Expressions.ref;
import static org.apache.iceberg.types.Types.NestedField.optional;

@RunWith(Parameterized.class)
public final class TestStructuredStreamingRead3 extends SparkCatalogTestBase {
  public TestStructuredStreamingRead3(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

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

  @Before
  public void setupTable() {
    sql("CREATE TABLE %s " +
        "(id INT, data STRING) " +
        "USING iceberg " +
        "PARTITIONED BY (bucket(3, id))", tableName);
    this.table = validationCatalog.loadTable(tableIdent);
    this.tableIdentifier = tableName;
  }

  @After
  public void stopStreams() throws TimeoutException {
    for (StreamingQuery query : spark.streams().active()) {
      query.stop();
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
    appendDataAsMultipleSnapshots(expected, tableIdentifier);

    table.refresh();

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);
    List<SimpleRecord> actual = processAvailable(df);

    Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testResumingStreamReadFromCheckpoint() throws Exception {
    File writerCheckpointFolder = temp.newFolder("writer-checkpoint-folder");
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    final String tempView = "microBatchView";

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);

    // Trigger.Once with the combination of StreamingQuery.awaitTermination, which succeeds after this code
    // will result in stopping the stream.
    // This is how Stream STOP and RESUME is simulated in this Test Case.
    DataStreamWriter<Row> singleBatchWriter = df.writeStream()
        .trigger(Trigger.Once())
        .option("checkpointLocation", writerCheckpoint.toString())
        .foreachBatch((batchDF, batchId) -> {
          batchDF.createOrReplaceGlobalTempView(tempView);
        });

    String globalTempView = "global_temp." + tempView;

    List<SimpleRecord> processStreamOnEmptyIcebergTable = processMicroBatch(singleBatchWriter, globalTempView);
    Assert.assertEquals(Collections.emptyList(), processStreamOnEmptyIcebergTable);

    for (List<List<SimpleRecord>> expectedCheckpoint : TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS) {
      appendDataAsMultipleSnapshots(expectedCheckpoint, tableIdentifier);
      table.refresh();

      List<SimpleRecord> actualDataInCurrentMicroBatch = processMicroBatch(singleBatchWriter, globalTempView);
      Assertions.assertThat(actualDataInCurrentMicroBatch)
          .containsExactlyInAnyOrderElementsOf(Iterables.concat(expectedCheckpoint));
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

    Dataset<Row> ds = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);
    Assertions.assertThat(processAvailable(ds))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(parquetFileRecords, orcFileRecords, avroFileRecords));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamFromEmptyTable() throws Exception {
    table.refresh();

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);

    List<SimpleRecord> actual = processAvailable(df);
    Assert.assertEquals(Collections.emptyList(), actual);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeOverwriteErrorsOut() throws Exception {
    // upgrade table to version 2 - to facilitate creation of Snapshot of type OVERWRITE.
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // fill table with some initial data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots, tableIdentifier);

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

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);
    StreamingQuery streamingQuery = df.writeStream()
        .format("memory")
        .queryName("testtablewithoverwrites")
        .outputMode(OutputMode.Append())
        .start();

    AssertHelpers.assertThrowsCause(
        "Streaming should fail with IllegalStateException, as the snapshot is not of type APPEND",
        IllegalStateException.class,
        "Cannot process overwrite snapshot",
        () -> streamingQuery.processAllAvailable()
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeReplaceIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected, tableIdentifier);

    table.refresh();

    // this should create a snapshot with type Replace.
    table.rewriteManifests()
        .clusterBy(f -> 1)
        .commit();

    // check pre-condition
    Assert.assertEquals(DataOperations.REPLACE, table.currentSnapshot().operation());

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);

    List<SimpleRecord> actual = processAvailable(df);
    Assertions.assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
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

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type DELETE.
    table.refresh();
    Assert.assertEquals(DataOperations.DELETE, table.currentSnapshot().operation());

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .load(tableIdentifier);
    StreamingQuery streamingQuery = df.writeStream()
        .format("memory")
        .queryName("testtablewithdelete")
        .outputMode(OutputMode.Append())
        .start();

    AssertHelpers.assertThrowsCause(
        "Streaming should fail with IllegalStateException, as the snapshot is not of type APPEND",
        IllegalStateException.class,
        "Cannot process delete snapshot",
        () -> streamingQuery.processAllAvailable()
    );
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testReadStreamWithSnapshotTypeDeleteAndSkipDeleteOption() throws Exception {
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

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type DELETE.
    table.refresh();
    Assert.assertEquals(DataOperations.DELETE, table.currentSnapshot().operation());

    Dataset<Row> df = spark.readStream()
        .format("iceberg")
        .option(SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS, "true")
        .load(tableIdentifier);
    Assertions.assertThat(processAvailable(df))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(dataAcrossSnapshots));
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

  private static List<SimpleRecord> processAvailable(Dataset<Row> df) throws TimeoutException {
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
}
