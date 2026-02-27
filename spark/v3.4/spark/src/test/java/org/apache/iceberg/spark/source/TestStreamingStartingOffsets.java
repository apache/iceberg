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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestStreamingStartingOffsets extends CatalogTestBase {

  private static final String MEMORY_TABLE = "_stream_view_starting_offsets";

  private Table table;

  @BeforeAll
  public static void setupSpark() {
    // disable AQE so each write produces a predictable number of files
    spark.conf().set(SQLConf.ADAPTIVE_EXECUTION_ENABLED().key(), "false");
  }

  @BeforeEach
  public void setupTable() {
    sql(
        "CREATE TABLE %s (id INT, data STRING) USING iceberg "
            + "TBLPROPERTIES ('commit.manifest.min-count-to-merge'='3', "
            + "'commit.manifest-merge.enabled'='true')",
        tableName);
    this.table = validationCatalog.loadTable(tableIdent);
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

  // -------------------------------------------------------------------------
  // earliest (default)
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testEarliestIsDefault() throws Exception {
    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    List<SimpleRecord> batch2 = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(batch1);
    appendData(batch2);

    StreamingQuery query = startStream();

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  @TestTemplate
  public void testEarliestExplicitMatchesDefault() throws Exception {
    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    List<SimpleRecord> batch2 = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(batch1);
    appendData(batch2);

    StreamingQuery query = startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "earliest");

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  // -------------------------------------------------------------------------
  // latest
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testLatestSkipsExistingDataAndPicksUpNewData() throws Exception {
    List<SimpleRecord> existing =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    appendData(existing);

    StreamingQuery query = startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest");

    // pre-existing data must be invisible
    assertThat(rowsAvailable(query)).isEmpty();

    // data appended after the stream starts is processed
    List<SimpleRecord> newData = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(newData);
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(newData);
  }

  @TestTemplate
  public void testLatestOnEmptyTablePicksUpNewData() throws Exception {
    StreamingQuery query = startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest");
    assertThat(rowsAvailable(query)).isEmpty();

    List<SimpleRecord> data = Lists.newArrayList(new SimpleRecord(1, "one"));
    appendData(data);
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(data);
  }

  // -------------------------------------------------------------------------
  // latest-with-snapshot
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testLatestWithSnapshotReadsAllFilesFromCurrentSnapshot() throws Exception {
    // batch1 files end up in snapshot1; batch2 files end up in snapshot2.
    // The current snapshot's manifest contains both sets of files, so
    // latest-with-snapshot (scanAllFiles=true) must return rows from both batches.
    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    List<SimpleRecord> batch2 =
        Lists.newArrayList(new SimpleRecord(3, "three"), new SimpleRecord(4, "four"));
    appendData(batch1);
    appendData(batch2);

    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest-with-snapshot");

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  @TestTemplate
  public void testLatestWithSnapshotThenContinuesIncremental() throws Exception {
    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    appendData(batch1);

    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest-with-snapshot");
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(batch1);

    // after the bootstrap batch, only newly added files are returned
    List<SimpleRecord> batch2 = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(batch2);
    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  @TestTemplate
  public void testLatestWithSnapshotOnEmptyTable() throws Exception {
    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest-with-snapshot");
    assertThat(rowsAvailable(query)).isEmpty();

    List<SimpleRecord> data = Lists.newArrayList(new SimpleRecord(1, "one"));
    appendData(data);
    assertThat(rowsAvailable(query)).containsExactlyInAnyOrderElementsOf(data);
  }

  // -------------------------------------------------------------------------
  // earliest-with-snapshot
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testEarliestWithSnapshotReadsAllData() throws Exception {
    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    List<SimpleRecord> batch2 = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(batch1);
    appendData(batch2);

    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "earliest-with-snapshot");

    assertThat(rowsAvailable(query))
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  @TestTemplate
  public void testEarliestWithSnapshotOnEmptyTable() throws Exception {
    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "earliest-with-snapshot");
    assertThat(rowsAvailable(query)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // validation
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testInvalidOptionThrows() throws TimeoutException {
    StreamingQuery query =
        startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "invalid-value");
    assertThatThrownBy(query::processAllAvailable)
        .satisfies(
            e -> {
              // the IAE is wrapped by StreamingQueryException; walk the cause chain
              Throwable root = e;
              while (root.getCause() != null) {
                root = root.getCause();
              }
              assertThat(root)
                  .isInstanceOf(IllegalArgumentException.class)
                  .hasMessageContaining("Invalid streaming-starting-offsets value: invalid-value");
            });
  }

  @TestTemplate
  public void testOptionIsCaseInsensitive() throws Exception {
    List<SimpleRecord> existing = Lists.newArrayList(new SimpleRecord(1, "one"));
    appendData(existing);

    // "LATEST" must be accepted and behave identically to "latest"
    StreamingQuery query = startStream(SparkReadOptions.STREAMING_STARTING_OFFSETS, "LATEST");
    assertThat(rowsAvailable(query)).isEmpty();
  }

  // -------------------------------------------------------------------------
  // checkpoint takes precedence
  // -------------------------------------------------------------------------

  @TestTemplate
  public void testOptionIgnoredWhenCheckpointExists() throws Exception {
    File checkpointDir = new File(temp.resolve("checkpoint").toFile(), "test-checkpoint");
    File outputDir = temp.resolve("output").toFile();

    List<SimpleRecord> batch1 =
        Lists.newArrayList(new SimpleRecord(1, "one"), new SimpleRecord(2, "two"));
    appendData(batch1);

    // First run: latest-with-snapshot bootstraps from the full current snapshot
    assertThat(
            spark
                .readStream()
                .format("iceberg")
                .option(SparkReadOptions.STREAMING_STARTING_OFFSETS, "latest-with-snapshot")
                .load(tableName)
                .writeStream()
                .format("parquet")
                .option("checkpointLocation", checkpointDir.toString())
                .option("path", outputDir.toString())
                .trigger(Trigger.AvailableNow())
                .start()
                .awaitTermination(60_000))
        .isTrue();

    // Add new data after the first run
    List<SimpleRecord> batch2 = Lists.newArrayList(new SimpleRecord(3, "three"));
    appendData(batch2);

    // Second run: 'earliest' would re-read everything if the checkpoint were ignored;
    // the checkpoint must override the option so only batch2 is new
    assertThat(
            spark
                .readStream()
                .format("iceberg")
                .option(SparkReadOptions.STREAMING_STARTING_OFFSETS, "earliest")
                .load(tableName)
                .writeStream()
                .format("parquet")
                .option("checkpointLocation", checkpointDir.toString())
                .option("path", outputDir.toString())
                .trigger(Trigger.AvailableNow())
                .start()
                .awaitTermination(60_000))
        .isTrue();

    // Total output across both runs should be batch1 + batch2 with no duplicates
    List<SimpleRecord> actual =
        spark
            .read()
            .load(outputDir.toString())
            .as(Encoders.bean(SimpleRecord.class))
            .collectAsList();
    assertThat(actual)
        .containsExactlyInAnyOrderElementsOf(Iterables.concat(batch1, batch2));
  }

  // -------------------------------------------------------------------------
  // helpers
  // -------------------------------------------------------------------------

  private void appendData(List<SimpleRecord> data) {
    Dataset<Row> df = spark.createDataFrame(data, SimpleRecord.class);
    df.select("id", "data").write().format("iceberg").mode("append").save(tableName);
  }

  private StreamingQuery startStream() throws TimeoutException {
    return startStream(Collections.emptyMap());
  }

  private StreamingQuery startStream(String key, String value) throws TimeoutException {
    return startStream(ImmutableMap.of(key, value));
  }

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

  private List<SimpleRecord> rowsAvailable(StreamingQuery query) {
    query.processAllAvailable();
    return spark
        .sql("SELECT * FROM " + MEMORY_TABLE)
        .as(Encoders.bean(SimpleRecord.class))
        .collectAsList();
  }
}
