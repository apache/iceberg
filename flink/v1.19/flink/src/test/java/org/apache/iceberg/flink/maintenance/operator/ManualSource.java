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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Queues;
import org.jetbrains.annotations.Nullable;

/** Testing source implementation for Flink sources which can be triggered manually. */
class ManualSource<T>
    implements Source<T, ManualSource.DummySplit, ManualSource.DummyCheckpoint>,
        ResultTypeQueryable<T> {

  private static final long serialVersionUID = 1L;
  private static final List<ArrayDeque<Tuple2<?, Long>>> QUEUES =
      Collections.synchronizedList(Lists.newArrayList());
  private static final List<CompletableFuture<Void>> AVAILABILITIES =
      Collections.synchronizedList(Lists.newArrayList());
  private static int numSources = 0;
  private final TypeInformation<T> type;
  private final int index;
  private transient DataStream<T> stream;
  private final transient StreamExecutionEnvironment env;

  /**
   * Creates a new source for testing.
   *
   * @param env to register the source
   * @param type of the events returned by the source
   */
  ManualSource(StreamExecutionEnvironment env, TypeInformation<T> type) {
    this.type = type;
    this.env = env;
    this.index = numSources++;
    QUEUES.add(Queues.newArrayDeque());
    AVAILABILITIES.add(new CompletableFuture<>());
  }

  /**
   * Emit a new record from the source.
   *
   * @param event to emit
   */
  void sendRecord(T event) {
    this.sendInternal(Tuple2.of(event, null));
  }

  /**
   * Emit a new record with the given event time from the source.
   *
   * @param event to emit
   * @param eventTime of the event
   */
  void sendRecord(T event, long eventTime) {
    this.sendInternal(Tuple2.of(event, eventTime));
  }

  /**
   * Emit a watermark from the source.
   *
   * @param timeStamp of the watermark
   */
  void sendWatermark(long timeStamp) {
    this.sendInternal(Tuple2.of(null, timeStamp));
  }

  /** Mark the source as finished. */
  void markFinished() {
    this.sendWatermark(Long.MAX_VALUE);
    this.sendInternal(Tuple2.of(null, null));
  }

  /**
   * Get the {@link DataStream} for this source.
   *
   * @return the stream emitted by this source
   */
  DataStream<T> dataStream() {
    if (this.stream == null) {
      this.stream =
          this.env
              .fromSource(this, WatermarkStrategy.noWatermarks(), "ManualSource-" + index, type)
              .forceNonParallel();
    }

    return this.stream;
  }

  private void sendInternal(Tuple2<?, Long> tuple) {
    QUEUES.get(index).offer(tuple);
    AVAILABILITIES.get(index).complete(null);
  }

  @Override
  public Boundedness getBoundedness() {
    return Boundedness.CONTINUOUS_UNBOUNDED;
  }

  @Override
  public SplitEnumerator<DummySplit, DummyCheckpoint> createEnumerator(
      SplitEnumeratorContext<DummySplit> enumContext) {
    return new DummyCheckpointEnumerator();
  }

  @Override
  public SplitEnumerator<DummySplit, DummyCheckpoint> restoreEnumerator(
      SplitEnumeratorContext<DummySplit> enumContext, DummyCheckpoint checkpoint) {
    return new DummyCheckpointEnumerator();
  }

  @Override
  public SimpleVersionedSerializer<DummySplit> getSplitSerializer() {
    return new NoOpDummySplitSerializer();
  }

  @Override
  public SimpleVersionedSerializer<DummyCheckpoint> getEnumeratorCheckpointSerializer() {
    return new NoOpDummyCheckpointSerializer();
  }

  @Override
  public SourceReader<T, DummySplit> createReader(SourceReaderContext sourceReaderContext) {
    return new SourceReader<>() {
      @Override
      public void start() {
        // Do nothing
      }

      @SuppressWarnings("unchecked")
      @Override
      public InputStatus pollNext(ReaderOutput<T> output) {
        Tuple2<T, Long> next = (Tuple2<T, Long>) QUEUES.get(index).poll();

        if (next != null) {
          if (next.f0 == null) {
            if (next.f1 == null) {
              // No more input
              return InputStatus.END_OF_INPUT;
            } else {
              output.emitWatermark(new Watermark(next.f1));
            }
          } else if (next.f1 == null) {
            // No event time set
            output.collect(next.f0);
          } else {
            // With event time
            output.collect(next.f0, next.f1);
          }
        }

        AVAILABILITIES.set(index, new CompletableFuture<>());
        return QUEUES.get(index).isEmpty()
            ? InputStatus.NOTHING_AVAILABLE
            : InputStatus.MORE_AVAILABLE;
      }

      @Override
      public List<DummySplit> snapshotState(long checkpointId) {
        return Lists.newArrayList(new DummySplit());
      }

      @Override
      public CompletableFuture<Void> isAvailable() {
        return AVAILABILITIES.get(index);
      }

      @Override
      public void addSplits(List<DummySplit> splits) {
        // do nothing
      }

      @Override
      public void notifyNoMoreSplits() {
        // do nothing
      }

      @Override
      public void close() {
        // do nothing
      }
    };
  }

  @Override
  public TypeInformation<T> getProducedType() {
    return this.type;
  }

  /**
   * Placeholder because the ManualSource itself implicitly represents the only split and does not
   * require an actual split object.
   */
  public static class DummySplit implements SourceSplit {
    @Override
    public String splitId() {
      return "dummy";
    }
  }

  /**
   * Placeholder because the ManualSource does not support fault-tolerance and thus does not require
   * actual checkpointing.
   */
  public static class DummyCheckpoint {}

  /** Placeholder because the ManualSource does not need enumeration, but checkpointing needs it. */
  private static class DummyCheckpointEnumerator
      implements SplitEnumerator<DummySplit, DummyCheckpoint> {

    @Override
    public void start() {
      // do nothing
    }

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
      // do nothing
    }

    @Override
    public void addSplitsBack(List<DummySplit> splits, int subtaskId) {
      // do nothing
    }

    @Override
    public void addReader(int subtaskId) {
      // do nothing
    }

    @Override
    public DummyCheckpoint snapshotState(long checkpointId) {
      return new DummyCheckpoint();
    }

    @Override
    public void close() {
      // do nothing
    }
  }

  /**
   * Not used - only required to avoid NullPointerException. The split is not transferred from the
   * enumerator, it is implicitly represented by the ManualSource.
   */
  private static class NoOpDummySplitSerializer implements SimpleVersionedSerializer<DummySplit> {
    @Override
    public int getVersion() {
      return 0;
    }

    @Override
    public byte[] serialize(DummySplit split) {
      return new byte[0];
    }

    @Override
    public DummySplit deserialize(int version, byte[] serialized) {
      return new DummySplit();
    }
  }

  /**
   * Not used - only required to avoid NullPointerException. The split is not transferred from the
   * enumerator, it is implicitly represented by the ManualSource.
   */
  private static class NoOpDummyCheckpointSerializer
      implements SimpleVersionedSerializer<DummyCheckpoint> {
    @Override
    public int getVersion() {
      return 0;
    }

    @Override
    public byte[] serialize(DummyCheckpoint split) {
      return new byte[0];
    }

    @Override
    public DummyCheckpoint deserialize(int version, byte[] serialized) {
      return new DummyCheckpoint();
    }
  }
}
