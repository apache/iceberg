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

import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.SparkReadConf;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.PartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.MicroBatchStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.SupportsTriggerAvailableNow;

abstract class SparkMicroBatchStreamBase implements MicroBatchStream, SupportsTriggerAvailableNow {

  private static final Types.StructType EMPTY_GROUPING_KEY_TYPE = Types.StructType.of();

  private final JavaSparkContext sparkContext;
  private final Table table;
  private final Supplier<FileIO> fileIO;
  private final SparkReadConf readConf;
  private final String projection;
  private Broadcast<Table> tableBroadcast = null;
  private final Broadcast<FileIO> fileIOBroadcast;
  private final StreamingOffset initialOffset;
  private StreamingOffset lastOffsetForTriggerAvailableNow = null;

  SparkMicroBatchStreamBase(
      JavaSparkContext sparkContext,
      Table table,
      Supplier<FileIO> fileIO,
      SparkReadConf readConf,
      Schema projection,
      String checkpointLocation,
      Supplier<StreamingOffset> initialOffsetSupplier) {
    this.sparkContext = sparkContext;
    this.table = table;
    this.fileIO = fileIO;
    this.readConf = readConf;
    this.projection = SchemaParser.toJson(projection);
    this.fileIOBroadcast = sparkContext.broadcast(SerializableFileIOWithSize.wrap(fileIO.get()));
    this.initialOffset =
        new StreamingInitialOffsetStore(
                checkpointLocation, sparkContext.hadoopConfiguration(), initialOffsetSupplier)
            .initialOffset();
  }

  protected final Table table() {
    return table;
  }

  protected final JavaSparkContext sparkContext() {
    return sparkContext;
  }

  protected final FileIO fileIO() {
    return fileIO.get();
  }

  protected final SparkReadConf readConf() {
    return readConf;
  }

  protected final StreamingOffset initialStreamingOffset() {
    return initialOffset;
  }

  protected final StreamingOffset lastOffsetForTriggerAvailableNow() {
    return lastOffsetForTriggerAvailableNow;
  }

  @Override
  public final Offset latestOffset() {
    return lastOffsetForTriggerAvailableNow != null
        ? lastOffsetForTriggerAvailableNow
        : latestStreamingOffset();
  }

  protected abstract StreamingOffset latestStreamingOffset();

  @Override
  public final InputPartition[] planInputPartitions(Offset start, Offset end) {
    Preconditions.checkArgument(
        start instanceof StreamingOffset, "Invalid start offset: %s", start);
    Preconditions.checkArgument(end instanceof StreamingOffset, "Invalid end offset: %s", end);

    List<? extends ScanTaskGroup<?>> taskGroups =
        planTaskGroups((StreamingOffset) start, (StreamingOffset) end);
    if (taskGroups.isEmpty()) {
      return new InputPartition[0];
    }

    String[][] locations =
        localityPreferred() ? SparkPlanningUtil.fetchBlockLocations(fileIO(), taskGroups) : null;
    Broadcast<Table> currentTableBroadcast = tableBroadcast();
    InputPartition[] partitions = new InputPartition[taskGroups.size()];
    for (int index = 0; index < taskGroups.size(); index++) {
      partitions[index] =
          new SparkInputPartition(
              EMPTY_GROUPING_KEY_TYPE,
              taskGroups.get(index),
              currentTableBroadcast,
              fileIOBroadcast,
              projection,
              readConf.caseSensitive(),
              locations != null ? locations[index] : SparkPlanningUtil.NO_LOCATION_PREFERENCE,
              readConf.cacheDeleteFilesOnExecutors());
    }

    return partitions;
  }

  protected abstract List<? extends ScanTaskGroup<?>> planTaskGroups(
      StreamingOffset startOffset, StreamingOffset endOffset);

  protected boolean localityPreferred() {
    return false;
  }

  protected Broadcast<Table> tableBroadcast() {
    if (tableBroadcast == null) {
      this.tableBroadcast = sparkContext.broadcast(SerializableTableWithSize.copyOf(table));
    }

    return tableBroadcast;
  }

  @Override
  public final PartitionReaderFactory createReaderFactory() {
    return new SparkRowReaderFactory();
  }

  @Override
  public final Offset initialOffset() {
    return initialOffset;
  }

  @Override
  public final Offset deserializeOffset(String json) {
    return StreamingOffset.fromJson(json);
  }

  @Override
  public final void commit(Offset end) {}

  @Override
  public final void stop() {
    stopStream();
  }

  protected void stopStream() {}

  @Override
  public final void prepareForTriggerAvailableNow() {
    this.lastOffsetForTriggerAvailableNow = availableNowEndOffset();
    availableNowPrepared();
  }

  protected StreamingOffset availableNowEndOffset() {
    return latestStreamingOffset();
  }

  protected void availableNowPrepared() {}
}
