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

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MicroBatches;
import org.apache.iceberg.MicroBatches.MicroBatch;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.TableScanUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReader;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.execution.streaming.HDFSMetadataLog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassTag;

public class SparkMicroBatchStream implements MicroBatchStream {
  private static final Logger LOG = LoggerFactory.getLogger(SparkMicroBatchStream.class);

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final boolean caseSensitive;
  private final Schema expectedSchema;
  private final List<Expression> filterExpressions;
  private final int batchSize;
  private final CaseInsensitiveStringMap options;
  private final String checkpointLocation;
  private final Long splitSize;
  private final Integer splitLookback;
  private final Long splitOpenFileCost;
  private final boolean localityPreferred;

  // lazy variables
  private StructType readSchema = null;

  // state
  private StreamingOffset committedOffset = null;

  SparkMicroBatchStream(JavaSparkContext sparkContext, Table table, boolean caseSensitive, Schema expectedSchema,
                        List<Expression> filterExpressions, CaseInsensitiveStringMap options, String checkpointLocation) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.caseSensitive = caseSensitive;
    this.expectedSchema = expectedSchema;
    this.filterExpressions = filterExpressions;
    this.batchSize = Spark3Util.batchSize(table.properties(), options);
    this.options = options;
    this.checkpointLocation = checkpointLocation;
    this.localityPreferred = Spark3Util.isLocalityEnabled(table.io(), table.location(), options);
    this.splitSize = Spark3Util.propertyAsLong(options, SparkReadOptions.SPLIT_SIZE, null);
    this.splitLookback = Spark3Util.propertyAsInt(options, SparkReadOptions.LOOKBACK, null);
    this.splitOpenFileCost = Spark3Util.propertyAsLong(options, SparkReadOptions.FILE_OPEN_COST, null);
  }

  @Override
  public Offset latestOffset() {
    if (committedOffset == null) {
      // Spark MicroBatchStream stateMachine invokes latestOffset as its first step.
      // this makes sense for sources like SocketStream - when there is a running stream of data.
      // in case of iceberg - particularly in case of full table reads from start
      // the amount of data to read could be very large.
      // so - do not participate in the statemachine unless spark specifies StartingOffset
      // - via planInputPartitions - in its 2nd step
      return StreamingOffset.START_OFFSET;
    }

    MicroBatch microBatch = MicroBatches.from(table.snapshot(committedOffset.snapshotId()), table.io())
        .caseSensitive(caseSensitive)
        .specsById(table.specs())
        .generate(committedOffset.position(), batchSize, committedOffset.shouldScanAllFiles());

    return new PlannedEndOffset(
        microBatch.snapshotId(), microBatch.endFileIndex(),
        committedOffset.shouldScanAllFiles(), committedOffset, microBatch);
  }

  @Override
  public InputPartition[] planInputPartitions(Offset start, Offset end) {
    if (end.equals(StreamingOffset.START_OFFSET)) {
      // TODO: validate that this is exercised - when a stream is being resumed from a checkpoint
      this.committedOffset = (StreamingOffset) start;
      return new InputPartition[0];
    }

    // broadcast the table metadata as input partitions will be sent to executors
    Broadcast<Table> tableBroadcast = sparkContext.broadcast(SerializableTable.copyOf(table));
    String expectedSchemaString = SchemaParser.toJson(expectedSchema);

    PlannedEndOffset endOffset = (PlannedEndOffset) end;
    // Preconditions.checkState(endOffset.getStartOffset().equals(start), "The cached MicroBatch doesn't match requested planInputPartitions");

    List<FileScanTask> fileScanTasks = endOffset.getMicroBatch().tasks();

    CloseableIterable<FileScanTask> splitTasks = TableScanUtil.splitFiles(CloseableIterable.withNoopClose(fileScanTasks),
        splitSize);
    List<CombinedScanTask> combinedScanTasks = Lists.newArrayList(
        TableScanUtil.planTasks(splitTasks, splitSize, splitLookback, splitOpenFileCost));
    InputPartition[] readTasks = new InputPartition[combinedScanTasks.size()];

    for (int i = 0; i < combinedScanTasks.size(); i++) {
      readTasks[i] = new ReadTask(
          combinedScanTasks.get(i), tableBroadcast, expectedSchemaString,
          caseSensitive, localityPreferred);
    }

    return readTasks;
  }

  @Override
  public PartitionReaderFactory createReaderFactory() {
    // TODO: what about batchSize?
    return new ReaderFactory(batchSize);
  }

  @Override
  public Offset initialOffset() {
    // TODO: should this read initial offset from checkpoint location?
    if (committedOffset != null) {
      return committedOffset;
    } else {
      List<Long> snapshotIds = SnapshotUtil.currentAncestors(table);
      if (snapshotIds.isEmpty()) {
        return StreamingOffset.START_OFFSET;
      } else {
        return new StreamingOffset(Iterables.getLast(snapshotIds), 0, true);
      }
    }
  }

  @Override
  public Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public void commit(Offset end) {
    committedOffset = (StreamingOffset) end;
  }

  @Override
  public void stop() {
    LOG.info("---------- stop");
  }

  // TODO: is this needed?
  // https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/sources/RateStreamMicroBatchStream.scala
  private static final class IcebergMetadataLog extends HDFSMetadataLog<StreamingOffset> {

    public IcebergMetadataLog(SparkSession sparkSession, String path, ClassTag<StreamingOffset> evidence$1) {
      super(sparkSession, path, evidence$1);
    }
  }

  private static class ReaderFactory implements PartitionReaderFactory {
    private final int batchSize;

    private ReaderFactory(int batchSize) {
      this.batchSize = batchSize;
    }

    @Override
    public PartitionReader<InternalRow> createReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new RowReader((ReadTask) partition);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public PartitionReader<ColumnarBatch> createColumnarReader(InputPartition partition) {
      if (partition instanceof ReadTask) {
        return new BatchReader((ReadTask) partition, batchSize);
      } else {
        throw new UnsupportedOperationException("Incorrect input partition type: " + partition);
      }
    }

    @Override
    public boolean supportColumnarReads(InputPartition partition) {
      return batchSize > 1;
    }
  }

  private static class RowReader extends RowDataReader implements PartitionReader<InternalRow> {
    RowReader(ReadTask task) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive());
    }
  }

  private static class BatchReader extends BatchDataReader implements PartitionReader<ColumnarBatch> {
    BatchReader(ReadTask task, int batchSize) {
      super(task.task, task.table(), task.expectedSchema(), task.isCaseSensitive(), batchSize);
    }
  }

  private static class ReadTask implements InputPartition, Serializable {
    private final CombinedScanTask task;
    private final Broadcast<Table> tableBroadcast;
    private final String expectedSchemaString;
    private final boolean caseSensitive;

    private transient Schema expectedSchema = null;
    private transient String[] preferredLocations = null;

    ReadTask(CombinedScanTask task, Broadcast<Table> tableBroadcast, String expectedSchemaString,
             boolean caseSensitive, boolean localityPreferred) {
      this.task = task;
      this.tableBroadcast = tableBroadcast;
      this.expectedSchemaString = expectedSchemaString;
      this.caseSensitive = caseSensitive;
      if (localityPreferred) {
        Table table = tableBroadcast.value();
        this.preferredLocations = Util.blockLocations(table.io(), task);
      } else {
        this.preferredLocations = HadoopInputFile.NO_LOCATION_PREFERENCE;
      }
    }

    @Override
    public String[] preferredLocations() {
      return preferredLocations;
    }

    public Collection<FileScanTask> files() {
      return task.files();
    }

    public Table table() {
      return tableBroadcast.value();
    }

    public boolean isCaseSensitive() {
      return caseSensitive;
    }

    private Schema expectedSchema() {
      if (expectedSchema == null) {
        this.expectedSchema = SchemaParser.fromJson(expectedSchemaString);
      }
      return expectedSchema;
    }
  }

  private static class PlannedEndOffset extends StreamingOffset {

    private final StreamingOffset startOffset;
    private final MicroBatch microBatch;

    PlannedEndOffset(long snapshotId, long position, boolean scanAllFiles, StreamingOffset startOffset, MicroBatch microBatch) {
      super(snapshotId, position, scanAllFiles);

      this.startOffset = startOffset;
      this.microBatch = microBatch;
    }

    public StreamingOffset getStartOffset() {
      return startOffset;
    }

    public MicroBatch getMicroBatch() {
      return microBatch;
    }
  }
}
