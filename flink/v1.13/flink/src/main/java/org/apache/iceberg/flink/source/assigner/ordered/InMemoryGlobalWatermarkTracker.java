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

package org.apache.iceberg.flink.source.assigner.ordered;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map.Entry;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects.ToStringHelper;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-Memory version of GlobalWatermarkTracker that can be run on the JobMaster.
 *
 * <p>This particular implementation computes the watermark value on every read - so technically
 * reads can be expensive. However, the reads are still bound by the number of partitions (which we
 * expect to be super low). If this assumption becomes false in the future, we can revisit the
 * implementation to cache the watermark on every write.
 *
 * @param <Partition> type of the partition
 */
class InMemoryGlobalWatermarkTracker<PartitionT> implements GlobalWatermarkTracker<PartitionT> {
  private static final Logger log = LoggerFactory.getLogger(InMemoryGlobalWatermarkTracker.class);

  private final ConcurrentMap<PartitionT, PartitionWatermarkState> acc = Maps.newConcurrentMap();
  private final ConcurrentMap<PartitionT, WeakHashMap<WatermarkTracker.Listener, Void>> listeners =
      Maps.newConcurrentMap();
  private final AtomicReference<Long> globalWatermark = new AtomicReference<>();
  // private final Registry registry;

  // public InMemoryGlobalWatermarkTracker(Registry registry) {
  //   this.registry = new FlinkRegistry(registry, "GlobalWatermarkTracker", ImmutableList.of());
  // }

  // private Gauge getWatermarkGaugeFor(String partition) {
  //   return registry.gauge("watermark", "partition", partition);
  // }

  private Long updateAndGet() {
    return globalWatermark.updateAndGet(
        oldValue -> {
          return acc.values()
              .stream()
              .filter(partitionWatermarkState -> !partitionWatermarkState.isComplete())
              .map(PartitionWatermarkState::getWatermark)
              .min(Comparator.naturalOrder())
              .orElse(null);
        });
  }

  private void updateWatermarkAndInformListeners(PartitionT updatedPartitionT) {
    Long v1 = globalWatermark.get();
    Long v2 = updateAndGet();

    if (v1 == null || !v1.equals(v2)) {
      listeners
          .entrySet()
          .stream()
          .forEach(
              entry -> {
                entry.getValue().keySet().forEach(listener -> listener.onWatermarkChange(v2));
              });
    }

    updateGauges();
  }

  @Nullable
  @Override
  public Long getGlobalWatermark() throws Exception {
    return globalWatermark.get();
  }

  private void updateGauges() {
    try {
      ToStringHelper helper = MoreObjects.toStringHelper(this);
      for (Entry<PartitionT, PartitionWatermarkState> entry : acc.entrySet()) {
        PartitionT partition = entry.getKey();
        PartitionWatermarkState partitionWatermarkState = entry.getValue();
        helper = helper.add(partition.toString(), partitionWatermarkState.toString());
        // getWatermarkGaugeFor(p.toString()).set(v.getWatermark());
      }

      helper.add("global", getGlobalWatermark());
      log.info("watermarkState={}", helper.toString());
      // if (globalWatermark != null) {
      //   getWatermarkGaugeFor("global").set(globalWatermark);
      // }
    } catch (Exception e) {
      log.error("Failed to update the gauges", e);
    }
  }

  @Override
  public Long updateWatermarkForPartition(PartitionT partitionT, long watermark) throws Exception {
    log.info("Updating watermark tracker to {} for partition {}", watermark, partitionT);
    acc.compute(
        partitionT,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(oldState, new PartitionWatermarkState(watermark, false)));
    updateWatermarkAndInformListeners(partitionT);
    return getGlobalWatermark();
  }

  @Override
  public void onPartitionCompletion(PartitionT partitionT) throws Exception {
    log.info("Marking partition {} as complete", partitionT);
    acc.compute(
        partitionT,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(
                oldState, new PartitionWatermarkState(Long.MIN_VALUE, true)));
    updateWatermarkAndInformListeners(partitionT);
  }

  @Override
  public void onPartitionInitialization(PartitionT partitionT) {
    acc.remove(partitionT);
    listeners.remove(partitionT);
    updateWatermarkAndInformListeners(partitionT);
  }

  @Override
  public void addListener(PartitionT partitionT, WatermarkTracker.Listener listener) {
    listeners.compute(
        partitionT,
        (dontCare, oldValue) -> {
          if (oldValue != null) {
            oldValue.put(listener, null);
            return oldValue;
          } else {
            WeakHashMap<WatermarkTracker.Listener, Void> temp = new WeakHashMap<>();
            temp.put(listener, null);
            return temp;
          }
        });
  }

  @Override
  public void removeListener(PartitionT partitionT, WatermarkTracker.Listener listener) {
    listeners.compute(
        partitionT,
        (dontCare, oldValue) -> {
          Preconditions.checkArgument(
              oldValue != null && oldValue.containsKey(listener), "listener not found");
          oldValue.remove(listener);

          if (oldValue.isEmpty()) {
            return null;
          } else {
            return oldValue;
          }
        });
  }

  static class PartitionWatermarkState implements Serializable {

    private final Long watermark;

    private final boolean isComplete;

    PartitionWatermarkState(Long watermark, boolean isComplete) {
      this.watermark = watermark;
      this.isComplete = isComplete;
    }

    public Long getWatermark() {
      return watermark;
    }

    public boolean isComplete() {
      return isComplete;
    }

    static PartitionWatermarkState max(PartitionWatermarkState first, PartitionWatermarkState second) {
      if (first == null) {
        return second;
      }
      if (second == null) {
        return first;
      }

      return new PartitionWatermarkState(
          Math.max(first.watermark, second.watermark), first.isComplete || second.isComplete);
    }
  }
}

