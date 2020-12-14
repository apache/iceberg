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

package org.apache.iceberg.flink.source.enumerator;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousIcebergEnumerator extends AbstractIcebergEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext;
  private final SplitAssigner assigner;
  private final IcebergEnumeratorConfig enumeratorConfig;
  private final ContinuousSplitPlanner splitPlanner;

  /**
   * snapshotId for the last enumerated snapshot.
   * next incremental enumeration should based off this as starting position.
   */
  private volatile Optional<Long> lastEnumeratedSnapshotId;

  public ContinuousIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner,
      @Nullable IcebergEnumeratorState enumState,
      IcebergEnumeratorConfig enumeratorConfig,
      ContinuousSplitPlanner splitPlanner) {
    super(enumeratorContext, assigner, enumeratorConfig);

    this.enumeratorContext = enumeratorContext;
    this.assigner = assigner;
    this.enumeratorConfig = enumeratorConfig;
    this.splitPlanner = splitPlanner;

    if (enumState != null) {
      this.lastEnumeratedSnapshotId = enumState.lastEnumeratedSnapshotId();
    } else {
      this.lastEnumeratedSnapshotId = Optional.empty();
    }
  }

  @Override
  public void start() {
    enumeratorContext.callAsync(
        this::discoverSplits,
        this::processDiscoveredSplits,
        0L,
        enumeratorConfig.splitDiscoveryInterval().toMillis());
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public IcebergEnumeratorState snapshotState() throws Exception {
    return new IcebergEnumeratorState(lastEnumeratedSnapshotId, assigner.snapshotState());
  }

  private SplitPlanningResult discoverSplits() {
    return splitPlanner.planSplits(lastEnumeratedSnapshotId);
  }

  private void processDiscoveredSplits(SplitPlanningResult result, Throwable error) {
    if (error == null) {
      LOG.info("Enumerated {} splits from table snapshot: id = {}, timestamp = {}",
          result.splits().size(), result.lastEnumeratedSnapshotId(), result.lastEnumeratedSnapshotTimestampMs());
      if (!result.splits().isEmpty()) {
        assigner.onDiscoveredSplits(result.splits());
      }
      lastEnumeratedSnapshotId = Optional.of(result.lastEnumeratedSnapshotId());
    } else {
      LOG.error("Failed to enumerate splits", error);
    }
  }

}
