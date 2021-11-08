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
import javax.annotation.Nullable;

/**
 * Global Watermark Tracker is used to track watermarks across a set of partitions in Flink job.
 * Watermark in this context captures the per-partition timestamp.
 *
 * @param <Partition> parametric type of partition
 */
public interface GlobalWatermarkTracker<Partition> {

  /**
   * If none of the partitions have registered, then this returns null.
   *
   * @return global watermark of all registered partitions
   * @throws Exception if there are issues talking to the tracker.
   */
  @Nullable
  Long getGlobalWatermark() throws Exception;

  // updates the local watermark for a given partition
  Long updateWatermarkForPartition(Partition partition, long watermark) throws Exception;

  // updates the tracker that the partition has completed
  void onPartitionCompletion(Partition partition) throws Exception;

  /**
   * This method is expected to be invoked when the partition has to register itself. This can also
   * be used to get rid of any state assoc. with the partition from the last session.
   *
   * @param partition partition to be registered for the first time.
   */
  void onPartitionInitialization(Partition partition);

  /**
   * This method allows the user to subscribe to global watermark updates.
   *
   * @param partition partition for which you would like to listen to updates for.
   * @param listener listener implementation that needs to be invoked on global watermark changes.
   */
  void addListener(Partition partition, WatermarkTracker.Listener listener);

  void removeListener(Partition partition, WatermarkTracker.Listener listener);

  // gets a GlobalWatermarkTracker for a given partition
  default WatermarkTracker forPartition(Partition partition) {
    return new WatermarkTracker() {
      @Nullable
      @Override
      public Long getGlobalWatermark() throws Exception {
        return GlobalWatermarkTracker.this.getGlobalWatermark();
      }

      @Override
      public Long updateWatermark(long watermark) throws Exception {
        return GlobalWatermarkTracker.this.updateWatermarkForPartition(partition, watermark);
      }

      @Override
      public void onCompletion() throws Exception {
        GlobalWatermarkTracker.this.onPartitionCompletion(partition);
      }

      @Override
      public void onInitialization() {
        GlobalWatermarkTracker.this.onPartitionInitialization(partition);
      }

      @Override
      public void addListener(Listener listener) {
        GlobalWatermarkTracker.this.addListener(partition, listener);
      }

      @Override
      public void removeListener(Listener listener) {
        GlobalWatermarkTracker.this.removeListener(partition, listener);
      }
    };
  }

  // Factory to create global watermark trackers for a given partition type
  @FunctionalInterface
  interface Factory extends Serializable {
    <T> GlobalWatermarkTracker<T> apply(String name);
  }
}
