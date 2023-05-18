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
package org.apache.iceberg.flink.source.assigner;

import java.util.Collection;
import java.util.Comparator;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link SplitAssigner} which assigns the splits ordered by the {@link
 * ContentFile#fileSequenceNumber()}.
 */
class SortedSplitAssigner implements SplitAssigner {
  private static final Logger LOG = LoggerFactory.getLogger(SortedSplitAssigner.class);

  // CHECKSTYLE.OFF: IllegalTypeCheck - We use TreeSet.pollFirst
  private final TreeSet<IcebergSourceSplit> pendingSplits;
  // CHECKSTYLE.ON: IllegalTypeCheck
  private CompletableFuture<Void> availableFuture;

  SortedSplitAssigner(Comparator<IcebergSourceSplit> comparator) {
    this(comparator, ImmutableList.of());
  }

  SortedSplitAssigner(
      Comparator<IcebergSourceSplit> comparator,
      Collection<IcebergSourceSplitState> assignerState) {
    this.pendingSplits = Sets.newTreeSet(comparator);
    // Because simple assigner only tracks unassigned splits,
    // there is no need to filter splits based on status (unassigned) here.
    assignerState.forEach(splitState -> pendingSplits.add(splitState.split()));
  }

  @Override
  public synchronized GetSplitResult getNext(@Nullable String hostname) {
    if (pendingSplits.isEmpty()) {
      return GetSplitResult.unavailable();
    } else {
      IcebergSourceSplit split = pendingSplits.pollFirst();
      LOG.info("Split served {}", split);
      return GetSplitResult.forSplit(split);
    }
  }

  @Override
  public void onDiscoveredSplits(Collection<IcebergSourceSplit> splits) {
    LOG.info("Splits discovered {}", splits);
    addSplits(splits);
  }

  @Override
  public void onUnassignedSplits(Collection<IcebergSourceSplit> splits) {
    LOG.info("Splits unassigned {}", splits);
    addSplits(splits);
  }

  private synchronized void addSplits(Collection<IcebergSourceSplit> splits) {
    if (!splits.isEmpty()) {
      pendingSplits.addAll(splits);
      // only complete pending future if new splits are discovered
      completeAvailableFuturesIfNeeded();
    }
  }

  /** Simple assigner only tracks unassigned splits */
  @Override
  public synchronized Collection<IcebergSourceSplitState> state() {
    return pendingSplits.stream()
        .map(split -> new IcebergSourceSplitState(split, IcebergSourceSplitStatus.UNASSIGNED))
        .collect(Collectors.toList());
  }

  @Override
  public synchronized CompletableFuture<Void> isAvailable() {
    if (availableFuture == null) {
      availableFuture = new CompletableFuture<>();
    }
    return availableFuture;
  }

  @Override
  public synchronized int pendingSplitCount() {
    return pendingSplits.size();
  }

  private synchronized void completeAvailableFuturesIfNeeded() {
    if (availableFuture != null && !pendingSplits.isEmpty()) {
      availableFuture.complete(null);
    }
    availableFuture = null;
  }
}
