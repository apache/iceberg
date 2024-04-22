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

import static org.apache.iceberg.expressions.Expressions.ref;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.DataStreamWriter;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public final class TestStructuredStreamingRead3 extends CatalogTestBase {

  private Table table;

  private final AtomicInteger microBatches = new AtomicInteger();

  /**
   * test data to be used by multiple writes each write creates a snapshot and writes a list of
   * records
   */
  private static final List<List<SimpleRecord>> TEST_DATA_MULTIPLE_SNAPSHOTS =
      Lists.newArrayList(
          Lists.newArrayList(
              new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three")),
          Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five")),
          Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven")));

  /**
   * test data - to be used for multiple write batches each batch inturn will have multiple
   * snapshots
   */
  private static final List<List<List<SimpleRecord>>> TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS =
      Lists.newArrayList(
          Lists.newArrayList(
              Lists.newArrayList(
                  new SimpleRecord(1, "one"),
                  new SimpleRecord(2, "two"),
                  new SimpleRecord(3, "three")),
              Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five"))),
          Lists.newArrayList(
              Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven")),
              Lists.newArrayList(new SimpleRecord(8, "eight"), new SimpleRecord(9, "nine"))),
          Lists.newArrayList(
              Lists.newArrayList(
                  new SimpleRecord(10, "ten"),
                  new SimpleRecord(11, "eleven"),
                  new SimpleRecord(12, "twelve")),
              Lists.newArrayList(
                  new SimpleRecord(13, "thirteen"), new SimpleRecord(14, "fourteen")),
              Lists.newArrayList(
                  new SimpleRecord(15, "fifteen"), new SimpleRecord(16, "sixteen"))));

  @BeforeAll
  public static void setupSpark() {
    // disable AQE as tests assume that writes generate a particular number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTable() {
    sql(
        "CREATE TABLE %s "
            + "(id INT, data STRING) "
            + "USING iceberg "
            + "PARTITIONED BY (bucket(3, id)) "
            + "TBLPROPERTIES ('commit.manifest.min-count-to-merge'='3', 'commit.manifest-merge.enabled'='true')",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);
    microBatches.set(0);
  }

  @AfterEach
  public void stopStreams() throws TimeoutException {
    for (StreamingQuery query : spark.streams().active()) {
      query.stop();
    }
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    StreamingQuery query = startStream();

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots_WithNumberOfFiles_1()
      throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1")))
        .isEqualTo(6);
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots_WithNumberOfFiles_2()
      throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "2")))
        .isEqualTo(3);
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots_WithNumberOfRows_1()
      throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    // only 1 micro-batch will be formed and we will read data partially
    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "1")))
        .isEqualTo(1);

    StreamingQuery query = startStream(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "1");

    // check answer correctness only 1 record read the micro-batch will be stuck
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(
            Lists.newArrayList(TEST_DATA_MULTIPLE_SNAPSHOTS.get(0).get(0)));
  }

  @TestTemplate
  public void testReadStreamOnIcebergTableWithMultipleSnapshots_WithNumberOfRows_4()
      throws Exception {
    appendDataAsMultipleSnapshots(TEST_DATA_MULTIPLE_SNAPSHOTS);

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "4")))
        .isEqualTo(2);
  }

  @TestTemplate
  public void testReadStreamOnIcebergThenAddData() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;

    StreamingQuery query = startStream();

    appendDataAsMultipleSnapshots(expected);

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromTimestamp() throws Exception {
    List<SimpleRecord> dataBeforeTimestamp =
        Lists.newArrayList(
            new SimpleRecord(-2, "minustwo"),
            new SimpleRecord(-1, "minusone"),
            new SimpleRecord(0, "zero"));

    appendData(dataBeforeTimestamp);

    table.refresh();
    long streamStartTimestamp = table.currentSnapshot().timestampMillis() + 1;

    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(streamStartTimestamp));

    List<SimpleRecord> empty = rowsAvailable(query);
    assertThat(empty).isEmpty();

    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    List<SimpleRecord> actual = rowsAvailable(query);

    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromFutureTimetsamp() throws Exception {
    long futureTimestamp = System.currentTimeMillis() + 10000;

    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(futureTimestamp));

    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).isEmpty();

    List<SimpleRecord> data =
        Lists.newArrayList(
            new SimpleRecord(-2, "minustwo"),
            new SimpleRecord(-1, "minusone"),
            new SimpleRecord(0, "zero"));

    // Perform several inserts that should not show up because the fromTimestamp has not elapsed
    IntStream.range(0, 3)
        .forEach(
            x -> {
              appendData(data);
              assertThat(rowsAvailable(query)).isEmpty();
            });

    waitUntilAfter(futureTimestamp);

    // Data appended after the timestamp should appear
    appendData(data);
    actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(data);
  }

  @TestTemplate
  public void testReadingStreamFromTimestampFutureWithExistingSnapshots() throws Exception {
    List<SimpleRecord> dataBeforeTimestamp =
        Lists.newArrayList(
            new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three"));
    appendData(dataBeforeTimestamp);

    long streamStartTimestamp = System.currentTimeMillis() + 2000;

    // Start the stream with a future timestamp after the current snapshot
    StreamingQuery query =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(streamStartTimestamp));
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).isEmpty();

    // Stream should contain data added after the timestamp elapses
    waitUntilAfter(streamStartTimestamp);
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamFromTimestampOfExistingSnapshot() throws Exception {
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;

    // Create an existing snapshot with some data
    appendData(expected.get(0));
    table.refresh();
    long firstSnapshotTime = table.currentSnapshot().timestampMillis();

    // Start stream giving the first Snapshot's time as the start point
    StreamingQuery stream =
        startStream(SparkReadOptions.STREAM_FROM_TIMESTAMP, Long.toString(firstSnapshotTime));

    // Append rest of expected data
    for (int i = 1; i < expected.size(); i++) {
      appendData(expected.get(i));
    }

    List<SimpleRecord> actual = rowsAvailable(stream);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadingStreamWithExpiredSnapshotFromTimestamp() throws TimeoutException {
    List<SimpleRecord> firstSnapshotRecordList = Lists.newArrayList(new SimpleRecord(1, "one"));

    List<SimpleRecord> secondSnapshotRecordList = Lists.newArrayList(new SimpleRecord(2, "two"));

    List<SimpleRecord> thirdSnapshotRecordList = Lists.newArrayList(new SimpleRecord(3, "three"));

    List<SimpleRecord> expectedRecordList = Lists.newArrayList();
    expectedRecordList.addAll(secondSnapshotRecordList);
    expectedRecordList.addAll(thirdSnapshotRecordList);

    appendData(firstSnapshotRecordList);
    table.refresh();
    long firstSnapshotid = table.currentSnapshot().snapshotId();
    long firstSnapshotCommitTime = table.currentSnapshot().timestampMillis();

    appendData(secondSnapshotRecordList);
    appendData(thirdSnapshotRecordList);

    table.expireSnapshots().expireSnapshotId(firstSnapshotid).commit();

    StreamingQuery query =
        startStream(
            SparkReadOptions.STREAM_FROM_TIMESTAMP, String.valueOf(firstSnapshotCommitTime));
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expectedRecordList);
  }

  @TestTemplate
  public void testResumingStreamReadFromCheckpoint() throws Exception {
    File writerCheckpointFolder = temp.resolve("writer-checkpoint-folder").toFile();
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    File output = temp.resolve("junit").toFile();

    DataStreamWriter querySource =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .option("checkpointLocation", writerCheckpoint.toString())
            .format("parquet")
            .queryName("checkpoint_test")
            .option("path", output.getPath());

    StreamingQuery startQuery = querySource.start();
    startQuery.processAllAvailable();
    startQuery.stop();

    List<SimpleRecord> expected = Lists.newArrayList();
    for (List<List<SimpleRecord>> expectedCheckpoint :
        TEST_DATA_MULTIPLE_WRITES_MULTIPLE_SNAPSHOTS) {
      // New data was added while the stream was down
      appendDataAsMultipleSnapshots(expectedCheckpoint);
      expected.addAll(Lists.newArrayList(Iterables.concat(Iterables.concat(expectedCheckpoint))));

      // Stream starts up again from checkpoint read the newly added data and shut down
      StreamingQuery restartedQuery = querySource.start();
      restartedQuery.processAllAvailable();
      restartedQuery.stop();

      // Read data added by the stream
      List<SimpleRecord> actual =
          spark.read().load(output.getPath()).as(Encoders.bean(SimpleRecord.class)).collectAsList();
      assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
    }
  }

  @TestTemplate
  public void testFailReadingCheckpointInvalidSnapshot() throws IOException, TimeoutException {
    File writerCheckpointFolder = temp.resolve("writer-checkpoint-folder").toFile();
    File writerCheckpoint = new File(writerCheckpointFolder, "writer-checkpoint");
    File output = temp.resolve("junit").toFile();

    DataStreamWriter querySource =
        spark
            .readStream()
            .format("iceberg")
            .load(tableName)
            .writeStream()
            .option("checkpointLocation", writerCheckpoint.toString())
            .format("parquet")
            .queryName("checkpoint_test")
            .option("path", output.getPath());

    List<SimpleRecord> firstSnapshotRecordList = Lists.newArrayList(new SimpleRecord(1, "one"));
    List<SimpleRecord> secondSnapshotRecordList = Lists.newArrayList(new SimpleRecord(2, "two"));
    StreamingQuery startQuery = querySource.start();

    appendData(firstSnapshotRecordList);
    table.refresh();
    long firstSnapshotid = table.currentSnapshot().snapshotId();
    startQuery.processAllAvailable();
    startQuery.stop();

    appendData(secondSnapshotRecordList);

    table.expireSnapshots().expireSnapshotId(firstSnapshotid).commit();

    StreamingQuery restartedQuery = querySource.start();
    assertThatThrownBy(restartedQuery::processAllAvailable)
        .hasCauseInstanceOf(IllegalStateException.class)
        .hasMessageContaining(
            String.format(
                "Cannot load current offset at snapshot %d, the snapshot was expired or removed",
                firstSnapshotid));
  }

  @TestTemplate
  public void testParquetOrcAvroDataInOneTable() throws Exception {
    List<SimpleRecord> parquetFileRecords =
        Lists.newArrayList(
            new SimpleRecord(1, "one"), new SimpleRecord(2, "two"), new SimpleRecord(3, "three"));

    List<SimpleRecord> orcFileRecords =
        Lists.newArrayList(new SimpleRecord(4, "four"), new SimpleRecord(5, "five"));

    List<SimpleRecord> avroFileRecords =
        Lists.newArrayList(new SimpleRecord(6, "six"), new SimpleRecord(7, "seven"));

    appendData(parquetFileRecords);
    appendData(orcFileRecords, "orc");
    appendData(avroFileRecords, "avro");

    StreamingQuery query = startStream();
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(
            Iterables.concat(parquetFileRecords, orcFileRecords, avroFileRecords));
  }

  @TestTemplate
  public void testReadStreamFromEmptyTable() throws Exception {
    StreamingQuery stream = startStream();
    List<SimpleRecord> actual = rowsAvailable(stream);
    assertThat(actual).isEmpty();
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeOverwriteErrorsOut() throws Exception {
    // upgrade table to version 2 - to facilitate creation of Snapshot of type OVERWRITE.
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    // fill table with some initial data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    Schema deleteRowSchema = table.schema().select("data");
    Record dataDelete = GenericRecord.create(deleteRowSchema);
    List<Record> dataDeletes =
        Lists.newArrayList(
            dataDelete.copy("data", "one") // id = 1
            );

    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            TestHelpers.Row.of(0),
            dataDeletes,
            deleteRowSchema);

    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath(File.createTempFile("junit", null, temp.toFile()).getPath())
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .withFormat(FileFormat.PARQUET)
            .build();

    table.newRowDelta().addRows(dataFile).addDeletes(eqDeletes).commit();

    // check pre-condition - that the above Delete file write - actually resulted in snapshot of
    // type OVERWRITE
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);

    StreamingQuery query = startStream();

    assertThatThrownBy(query::processAllAvailable)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Cannot process overwrite snapshot");
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1")))
        .isEqualTo(6);
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceMaxRows()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH, "4")))
        .isEqualTo(2);
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceMaxFilesAndRows()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();

    assertThat(
            microBatchCount(
                ImmutableMap.of(
                    SparkReadOptions.STREAMING_MAX_ROWS_PER_MICRO_BATCH,
                    "4",
                    SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH,
                    "1")))
        .isEqualTo(6);
  }

  @TestTemplate
  public void testReadStreamWithSnapshotType2RewriteDataFilesIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();
    makeRewriteDataFiles();

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1")))
        .isEqualTo(6);
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeRewriteDataFilesIgnoresReplaceFollowedByAppend()
      throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    makeRewriteDataFiles();

    appendDataAsMultipleSnapshots(expected);

    assertThat(
            microBatchCount(
                ImmutableMap.of(SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1")))
        .isEqualTo(12);
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeReplaceIgnoresReplace() throws Exception {
    // fill table with some data
    List<List<SimpleRecord>> expected = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(expected);

    // this should create a snapshot with type Replace.
    table.rewriteManifests().clusterBy(f -> 1).commit();

    // check pre-condition
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.REPLACE);

    StreamingQuery query = startStream();
    List<SimpleRecord> actual = rowsAvailable(query);
    assertThat(actual).containsExactlyInAnyOrderElementsOf(Iterables.concat(expected));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteErrorsOut() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    // this should create a snapshot with type delete.
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 4)).commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // DELETE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    StreamingQuery query = startStream();

    assertThatThrownBy(query::processAllAvailable)
        .cause()
        .isInstanceOf(IllegalStateException.class)
        .hasMessageStartingWith("Cannot process delete snapshot");
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteAndSkipDeleteOption() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    // this should create a snapshot with type delete.
    table.newDelete().deleteFromRowFilter(Expressions.equal("id", 4)).commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // DELETE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.DELETE);

    StreamingQuery query = startStream(SparkReadOptions.STREAMING_SKIP_DELETE_SNAPSHOTS, "true");
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(dataAcrossSnapshots));
  }

  @TestTemplate
  public void testReadStreamWithSnapshotTypeDeleteAndSkipOverwriteOption() throws Exception {
    table.updateSpec().removeField("id_bucket").addField(ref("id")).commit();

    // fill table with some data
    List<List<SimpleRecord>> dataAcrossSnapshots = TEST_DATA_MULTIPLE_SNAPSHOTS;
    appendDataAsMultipleSnapshots(dataAcrossSnapshots);

    // this should create a snapshot with type overwrite.
    table.newOverwrite().overwriteByRowFilter(Expressions.greaterThan("id", 4)).commit();

    // check pre-condition - that the above delete operation on table resulted in Snapshot of Type
    // OVERWRITE.
    assertThat(table.currentSnapshot().operation()).isEqualTo(DataOperations.OVERWRITE);

    StreamingQuery query = startStream(SparkReadOptions.STREAMING_SKIP_OVERWRITE_SNAPSHOTS, "true");
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(dataAcrossSnapshots));
  }

  /**
   * We are testing that all the files in a rewrite snapshot are skipped Create a rewrite data files
   * snapshot using existing files.
   */
  public void makeRewriteDataFiles() {
    table.refresh();

    // we are testing that all the files in a rewrite snapshot are skipped
    // create a rewrite data files snapshot using existing files
    RewriteFiles rewrite = table.newRewrite();
    Iterable<Snapshot> it = table.snapshots();
    for (Snapshot snapshot : it) {
      if (snapshot.operation().equals(DataOperations.APPEND)) {
        Iterable<DataFile> datafiles = snapshot.addedDataFiles(table.io());
        for (DataFile datafile : datafiles) {
          rewrite.addFile(datafile);
          rewrite.deleteFile(datafile);
        }
      }
    }
    rewrite.commit();
  }

  /**
   * appends each list as a Snapshot on the iceberg table at the given location. accepts a list of
   * lists - each list representing data per snapshot.
   */
  private void appendDataAsMultipleSnapshots(List<List<SimpleRecord>> data) {
    for (List<SimpleRecord> l : data) {
      appendData(l);
    }
  }

  private void appendData(List<SimpleRecord> data) {
    appendData(data, "parquet");
  }

  private void appendData(List<SimpleRecord> data, String format) {
    Dataset<Row> df = spark.createDataFrame(data, SimpleRecord.class);
    df.select("id", "data")
        .write()
        .format("iceberg")
        .option("write-format", format)
        .mode("append")
        .save(tableName);
  }

  private static final String MEMORY_TABLE = "_stream_view_mem";

  private StreamingQuery startStream(Map<String, String> options) throws TimeoutException {
    return spark
        .readStream()
        .options(options)
        .format("iceberg")
        .load(tableName)
        .writeStream()
        .options(options)
        .format("memory")
        .queryName(MEMORY_TABLE)
        .outputMode(OutputMode.Append())
        .start();
  }

  private StreamingQuery startStream() throws TimeoutException {
    return startStream(Collections.emptyMap());
  }

  private StreamingQuery startStream(String key, String value) throws TimeoutException {
    return startStream(
        ImmutableMap.of(key, value, SparkReadOptions.STREAMING_MAX_FILES_PER_MICRO_BATCH, "1"));
  }

  private int microBatchCount(Map<String, String> options) throws TimeoutException {
    Dataset<Row> ds = spark.readStream().options(options).format("iceberg").load(tableName);

    ds.writeStream()
        .options(options)
        .foreachBatch(
            (VoidFunction2<Dataset<Row>, Long>)
                (dataset, batchId) -> {
                  microBatches.getAndIncrement();
                })
        .start()
        .processAllAvailable();

    stopStreams();
    return microBatches.get();
  }

  private List<SimpleRecord> rowsAvailable(StreamingQuery query) {
    query.processAllAvailable();
    return spark
        .sql("select * from " + MEMORY_TABLE)
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }
}
