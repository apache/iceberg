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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.iceberg.flink.source.ScanContext;
import org.apache.iceberg.flink.source.assigner.SplitAssigner;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ContinuousIcebergEnumerator extends AbstractIcebergEnumerator {

  private static final Logger LOG = LoggerFactory.getLogger(ContinuousIcebergEnumerator.class);

  private final SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext;
  private final SplitAssigner assigner;
  private final ScanContext scanContext;
  private final ContinuousSplitPlanner splitPlanner;

  /**
   * snapshotId for the last enumerated snapshot. next incremental enumeration
   * should be based off this as the starting position.
   */
  private final AtomicReference<IcebergEnumeratorPosition> enumeratorPosition;

  public ContinuousIcebergEnumerator(
      SplitEnumeratorContext<IcebergSourceSplit> enumeratorContext,
      SplitAssigner assigner,
      ScanContext scanContext,
      ContinuousSplitPlanner splitPlanner,
      @Nullable IcebergEnumeratorState enumState) {
    super(enumeratorContext, assigner);

    this.enumeratorContext = enumeratorContext;
    this.assigner = assigner;
    this.scanContext = scanContext;
    this.splitPlanner = splitPlanner;
    this.enumeratorPosition = new AtomicReference<>();
    if (enumState != null) {
      this.enumeratorPosition.set(enumState.lastEnumeratedPosition());
    }
  }

  @Override
  public void start() {
    super.start();
    enumeratorContext.callAsync(
        this::discoverSplits,
        this::processDiscoveredSplits,
        0L,
        scanContext.monitorInterval().toMillis());
  }

  @Override
  protected boolean shouldWaitForMoreSplits() {
    return true;
  }

  @Override
  public IcebergEnumeratorState snapshotState(long checkpointId) {
    return new IcebergEnumeratorState(enumeratorPosition.get(), assigner.state());
  }

  private ContinuousEnumerationResult discoverSplits() {
    return splitPlanner.planSplits(enumeratorPosition.get());
  }

  private void processDiscoveredSplits(ContinuousEnumerationResult result, Throwable error) {
    if (error == null) {
      if (!result.splits().isEmpty()) {
        if (enumeratorPosition.get() != null &&
            Objects.equals(result.toPosition().snapshotId(), enumeratorPosition.get().snapshotId())) {
          // Multiple discoverSplits() may be triggered with the same starting snapshot to the I/O thread pool.
          // E.g., the splitDiscoveryInterval is very short (like 10 ms in some unit tests) or the thread
          // pool is busy and multiple discovery actions are executed concurrently. In this case, we need to
          // ignore the redundant discovery results. Otherwise, we can add duplicate splits to the assigner.
          LOG.info("Skip {} discovered splits with the same starting position: {}",
              result.splits().size(), result.toPosition());
        } else {
          LOG.info("Add {} discovered splits from position: {}",
              result.splits().size(), result.toPosition());
          assigner.onDiscoveredSplits(result.splits());
        }
      }
      // update the enumerator position when there is no error even if there is no split discovered
      // or the position is null (e.g. for empty table).
      enumeratorPosition.set(result.toPosition());
      LOG.info("Set enumerator position: {}", result.toPosition());
    } else {
      LOG.error("Failed to enumerate splits", error);
    }
  }

}
