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
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.TestBase;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestStreamingWriteReplay {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static final String QUERY_ID = "6a1c2d3e-0000-0000-0000-000000000000";
  private static final long EPOCH_ID = 7L;
  private static final int PARTITION_ID = 0;
  // a restarted driver hands out task attempt IDs from 0 again
  private static final long TASK_ID = 0L;

  private static SparkSession spark = null;

  @TempDir private Path temp;

  @BeforeAll
  static void startSpark() {
    TestStreamingWriteReplay.spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            .config(TestBase.DISABLE_UI)
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    SparkSession currentSpark = TestStreamingWriteReplay.spark;
    TestStreamingWriteReplay.spark = null;
    currentSpark.stop();
  }

  @Test
  void replayOfCommittedEpochKeepsCommittedFilesIntact() throws IOException {
    String location = temp.resolve("table").toString();
    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(), location);

    StreamingWrite firstRun = newStreamingWrite(table);
    firstRun.commit(EPOCH_ID, new WriterCommitMessage[] {runTask(firstRun, 1, 2, 3)});

    DataFile committed = onlyDataFile(tables.load(location));

    // the driver died before Spark recorded the epoch as committed, so Spark runs it again and
    // different rows land in the same partition
    StreamingWrite replay = newStreamingWrite(tables.load(location));
    replay.commit(EPOCH_ID, new WriterCommitMessage[] {runTask(replay, 10, 20)});

    assertThat(spark.read().parquet(committed.location()).count())
        .as("Records in the committed data file must match the record count in table metadata")
        .isEqualTo(committed.recordCount());
  }

  private static StreamingWrite newStreamingWrite(Table table) {
    LogicalWriteInfo info =
        new LogicalWriteInfo() {
          @Override
          public String queryId() {
            return QUERY_ID;
          }

          @Override
          public StructType schema() {
            return SparkSchemaUtil.convert(SCHEMA);
          }

          @Override
          public CaseInsensitiveStringMap options() {
            return CaseInsensitiveStringMap.empty();
          }

          @Override
          public Optional<StructType> rowIdSchema() {
            return Optional.empty();
          }

          @Override
          public Optional<StructType> metadataSchema() {
            return Optional.empty();
          }
        };

    return new SparkWriteBuilder(spark, table, null, info).build().toStreaming();
  }

  private static WriterCommitMessage runTask(StreamingWrite write, int... ids) throws IOException {
    PhysicalWriteInfo physicalInfo = () -> 1;
    StreamingDataWriterFactory factory = write.createStreamingWriterFactory(physicalInfo);
    DataWriter<InternalRow> writer = factory.createWriter(PARTITION_ID, TASK_ID, EPOCH_ID);
    try {
      for (int id : ids) {
        writer.write(new GenericInternalRow(new Object[] {id, UTF8String.fromString("v" + id)}));
      }

      return writer.commit();
    } finally {
      writer.close();
    }
  }

  private static DataFile onlyDataFile(Table table) throws IOException {
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      return Iterables.getOnlyElement(tasks).file().copyWithoutStats();
    }
  }
}
