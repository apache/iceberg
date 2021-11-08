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

import java.time.Duration;
import java.util.Collection;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.iceberg.flink.source.assigner.SplitAssignerFactory;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;

public class EventTimeAlignmentAssignerFactory implements SplitAssignerFactory {
  private final String sourceName;
  private final Duration maxMisalignmentThreshold;

  private final ClockFactory clockFactory;
  private final GlobalWatermarkTracker.Factory trackerFactory;
  private final TimestampAssigner<IcebergSourceSplit> timestampAssigner;

  public EventTimeAlignmentAssignerFactory(
      String sourceName,
      Duration maxMisalignmentThreshold,
      ClockFactory clockFactory,
      GlobalWatermarkTracker.Factory trackerFactory,
      TimestampAssigner<IcebergSourceSplit> timestampAssigner) {
    this.sourceName = sourceName;
    this.maxMisalignmentThreshold = maxMisalignmentThreshold;
    this.clockFactory = clockFactory;
    this.trackerFactory = trackerFactory;
    this.timestampAssigner = timestampAssigner;
  }

  @Override
  public EventTimeAlignmentAssigner createAssigner() {
    return new EventTimeAlignmentAssigner(
        maxMisalignmentThreshold,
        timestampAssigner,
        clockFactory.get(),
        trackerFactory.<String>apply("iceberg").forPartition(sourceName)
    );
  }

  @Override
  public EventTimeAlignmentAssigner createAssigner(Collection<IcebergSourceSplitState> assignerState) {
    return new EventTimeAlignmentAssigner(maxMisalignmentThreshold,
        assignerState,
        trackerFactory.<String>apply("iceberg").forPartition(sourceName),
        timestampAssigner,
        clockFactory.get());
  }
}
