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

import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WatermarkUpdater
    implements EventTimeAlignmentAssignerState.StateChangeListener {

  private final WatermarkTracker tracker;
  private final SortedSet<IcebergSourceSplit> inCompleteSplits;
  private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;
  private static final Logger log = LoggerFactory.getLogger(WatermarkUpdater.class);

  WatermarkUpdater(
      WatermarkTracker tracker,
      TimestampAssigner<IcebergSourceSplit> timestampAssigner,
      EventTimeAlignmentAssignerState assignerState) {
    this.tracker = tracker;
    this.timestampAssigner = timestampAssigner;
    this.inCompleteSplits =
        new TreeSet<>(new AscendingTimestampSplitComparator(timestampAssigner));
    tracker.onInitialization();

    assignerState.register(this);
  }

  @Override
  public void onSplitsAdded(Collection<IcebergSourceSplit> splits) {
    inCompleteSplits.addAll(splits);
    updateWatermark();
  }

  @Override
  public void onSplitsAssigned(Collection<IcebergSourceSplit> splits) {
    inCompleteSplits.addAll(splits);
    updateWatermark();
  }

  @Override
  public void onSplitsUnassigned(Collection<IcebergSourceSplit> splits) {
    inCompleteSplits.addAll(splits);
    updateWatermark();
  }

  @Override
  public void onSplitsCompleted(Collection<IcebergSourceSplit> splits) {
    inCompleteSplits.removeAll(splits);
    updateWatermark();
  }

  @Override
  public void onNoMoreStatusChanges() {
    try {
      tracker.onCompletion();
    } catch (Exception e) {
      log.error("Failed to update watermark", e);
    }
  }

  private void updateWatermark() {
    if (!inCompleteSplits.isEmpty()) {
      try {
        tracker.updateWatermark(timestampAssigner.extractTimestamp(inCompleteSplits.first(), -1));
      } catch (Exception e) {
        log.error("Failed to update watermark", e);
      }
    }
  }
}

