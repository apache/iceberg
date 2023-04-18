/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *   http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing,
 *  * software distributed under the License is distributed on an
 *  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  * KIND, either express or implied.  See the License for the
 *  * specific language governing permissions and limitations
 *  * under the License.
 *
 */

package org.apache.iceberg.flink.source.assigner;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.flink.shaded.guava30.com.google.common.base.MoreObjects;
import org.apache.flink.shaded.guava30.com.google.common.base.Preconditions;
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
 * @param <T> type of the partition
 */
class InMemoryGlobalWatermarkTracker<T> implements GlobalWatermarkTracker<T> {

  private static final Logger log = LoggerFactory.getLogger(InMemoryGlobalWatermarkTracker.class);

  private final ConcurrentMap<T, PartitionWatermarkState> acc = new ConcurrentHashMap<>();
  private final ConcurrentMap<T, List<WatermarkTracker.Listener>> listeners = new ConcurrentHashMap<>();
  private final AtomicReference<Long> globalWatermark = new AtomicReference<>();

  public InMemoryGlobalWatermarkTracker() {
    // this.registry = new FlinkRegistry(registry, "GlobalWatermarkTracker.", ImmutableList.of());
  }

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
              .orElseGet(
                  () ->
                      acc.values()
                          .stream()
                          .map(PartitionWatermarkState::getWatermark)
                          .max(Comparator.naturalOrder())
                          .orElse(null));
        });
  }

  private void updateWatermarkAndInformListeners(T updatedPartition) {
    Long v1 = globalWatermark.get();
    Long v2 = updateAndGet();

    if (v1 == null || !v1.equals(v2)) {
      listeners
          .entrySet()
          .stream()
          .filter(entry -> !entry.getKey().equals(updatedPartition))
          .forEach(
              entry -> {
                entry.getValue().forEach(listener -> listener.onWatermarkChange(v2));
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
      MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this);
      for (Entry<T, PartitionWatermarkState> entry : acc.entrySet()) {
        T p = entry.getKey();
        PartitionWatermarkState v = entry.getValue();
        helper = helper.add(p.toString(), v.toString());
        // getWatermarkGaugeFor(p.toString()).set(v.getWatermark());
      }

      Long globalWatermark = getGlobalWatermark();
      helper.add("global", globalWatermark);
      log.info("watermarkState={}", helper.toString());
      // if (globalWatermark != null) {
      //   getWatermarkGaugeFor("global").set(globalWatermark);
      // }
    } catch (Exception e) {
      log.error("Failed to update the gauges", e);
    }
  }

  @Override
  public Long updateWatermarkForPartition(T partition, long watermark) throws Exception {
    log.info("Updating watermark tracker to {} for partition {}", watermark, partition);
    acc.compute(
        partition,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(oldState, new PartitionWatermarkState(watermark, false)));
    updateWatermarkAndInformListeners(partition);
    return getGlobalWatermark();
  }

  @Override
  public void onPartitionCompletion(T partition) throws Exception {
    log.info("Marking partition {} as complete", partition);
    acc.compute(
        partition,
        (dontCare, oldState) ->
            PartitionWatermarkState.max(
                oldState, new PartitionWatermarkState(Long.MIN_VALUE, true)));
    updateWatermarkAndInformListeners(partition);
  }

  @Override
  public void onPartitionInitialization(T partition) {
    acc.remove(partition);
    listeners.remove(partition);
    updateWatermarkAndInformListeners(partition);
  }

  @Override
  public void addListener(T t, WatermarkTracker.Listener listener) {
    listeners.compute(
        t,
        (dontCare, oldValue) -> {
          ArrayList<WatermarkTracker.Listener> l = new ArrayList<>();
          if (oldValue != null) {
            l.addAll(oldValue);
          }
          l.add(listener);
          return l;
        });
  }

  @Override
  public void removeListener(T t, WatermarkTracker.Listener listener) {
    listeners.compute(
        t,
        (dontCare, oldValue) -> {
          Preconditions.checkArgument(
              oldValue != null && oldValue.contains(listener), "listener not found");
          oldValue.remove(listener);

          if (oldValue.isEmpty()) {
            return null;
          } else {
            return oldValue;
          }
        });
  }

  static class PartitionWatermarkState implements Serializable {

    Long watermark;

    boolean isComplete;

    public PartitionWatermarkState(Long watermark, boolean isComplete) {
      this.watermark = watermark;
      this.isComplete = isComplete;
    }

    public Long getWatermark() {
      return watermark;
    }

    public boolean isComplete() {
      return isComplete;
    }

    static PartitionWatermarkState max(PartitionWatermarkState a, PartitionWatermarkState b) {
      if (a == null) {
        return b;
      }
      if (b == null) {
        return a;
      }

      return new PartitionWatermarkState(
          Math.max(a.watermark, b.watermark), a.isComplete || b.isComplete);
    }
  }
}
