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
import java.time.Clock;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitState;
import org.apache.iceberg.flink.source.split.IcebergSourceSplitStatus;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.MoreObjects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventTimeAlignmentAssignerState {
  private static final Logger log = LoggerFactory.getLogger(EventTimeAlignmentAssignerState.class);

  // split IDs to their states
  private final Map<String, SplitState> splitStateMap;

  // flag that informs the state that there are no more splits to be added by the Enumerator
  private volatile boolean noMoreSplits;

  // timestamp the state was last updated
  private volatile Instant lastUpdatedTs;

  private volatile int numAssignedSplits;
  private volatile int numCompletedSplits;
  private final Clock clock;
  private transient WeakHashMap<StateChangeListener, Void> listeners;

  EventTimeAlignmentAssignerState(Clock clock) {
    this(Collections.emptyList(), clock);
  }

  EventTimeAlignmentAssignerState(Collection<IcebergSourceSplitState> currentState, Clock clock) {
    this.splitStateMap = Maps.newConcurrentMap();
    currentState.forEach(icebergSourceSplitState -> splitStateMap.put(icebergSourceSplitState.split().splitId(),
        new SplitState(icebergSourceSplitState.split(), icebergSourceSplitState.status())));
    this.noMoreSplits = false;
    this.clock = clock;
    this.listeners = new WeakHashMap<>();
    this.lastUpdatedTs = clock.instant();
  }

  public synchronized void register(StateChangeListener handler) {
    listeners.put(handler, null);
  }

  public synchronized void unregister(StateChangeListener handler) {
    Preconditions.checkArgument(listeners.containsKey(handler), "unknown handler %s", handler);
    listeners.remove(handler);
  }

  public synchronized EventTimeAlignmentAssignerState addSplits(Collection<IcebergSourceSplit> splits) {
    Preconditions.checkArgument(!noMoreSplits, "no more splits can be added");
    List<IcebergSourceSplit> addedSplits =
        splits
            .stream()
            .filter(
                split -> splitStateMap.putIfAbsent(split.splitId(), SplitState.of(split)) == null)
            .collect(Collectors.toList());

    if (!addedSplits.isEmpty()) {
      updateTs();
      listeners.keySet().forEach(listener -> listener.onSplitsAdded(addedSplits));
    }
    return this;
  }

  public synchronized EventTimeAlignmentAssignerState onNoMoreSplits() {
    if (!noMoreSplits) {
      noMoreSplits = true;

      updateTs();
      // check if the state has reached the terminal condition and if this is the first
      // time that condition has been reached
      if (isTerminal()) {
        updateListenersOnTerminalCondition();
      }
    }

    return this;
  }

  public synchronized EventTimeAlignmentAssignerState assignSplits(
      List<IcebergSourceSplit> splits, @Nullable String hostName) {
    if (!splits.isEmpty()) {
      splits.forEach(split ->
          Preconditions.checkArgument(splitStateMap.get(split.splitId()).assignTo(hostName) ==
                  IcebergSourceSplitStatus.UNASSIGNED));

      numAssignedSplits += splits.size();
      updateTs();
      updateListeners(splits, IcebergSourceSplitStatus.UNASSIGNED, IcebergSourceSplitStatus.ASSIGNED);
    }

    return this;
  }

  public synchronized EventTimeAlignmentAssignerState unassignSplits(Collection<IcebergSourceSplit> splits) {
    if (splits.isEmpty()) {
      return this;
    }

    List<Tuple2<IcebergSourceSplitStatus, IcebergSourceSplit>> modifiedSplits =
        splits
            .stream()
            .map(
                split -> {
                  IcebergSourceSplitStatus res = splitStateMap.get(split.splitId()).unassign();
                  return new Tuple2<>(res, split);
                })
            .filter(t -> t.f0 == IcebergSourceSplitStatus.ASSIGNED || t.f0 == IcebergSourceSplitStatus.COMPLETED)
            .collect(Collectors.toList());

    List<IcebergSourceSplit> previousAssignedSplits =
        modifiedSplits
            .stream()
            .filter(status -> status.f0 == IcebergSourceSplitStatus.ASSIGNED)
            .map(t -> t.f1)
            .collect(Collectors.toList());

    List<IcebergSourceSplit> previousCompletedSplits =
        modifiedSplits
            .stream()
            .filter(status -> status.f0 == IcebergSourceSplitStatus.COMPLETED)
            .map(t -> t.f1)
            .collect(Collectors.toList());

    if (!previousAssignedSplits.isEmpty()) {
      numAssignedSplits -= previousAssignedSplits.size();
    }

    if (!previousCompletedSplits.isEmpty()) {
      numCompletedSplits -= previousCompletedSplits.size();
    }

    updateTs();

    updateListeners(previousAssignedSplits, IcebergSourceSplitStatus.ASSIGNED, IcebergSourceSplitStatus.UNASSIGNED);
    updateListeners(previousCompletedSplits, IcebergSourceSplitStatus.COMPLETED, IcebergSourceSplitStatus.UNASSIGNED);
    return this;
  }

  public synchronized EventTimeAlignmentAssignerState completeSplits(Collection<String> splitIds) {
    List<IcebergSourceSplit> completedSplitIds =
        splitIds
            .stream()
            .filter(splitId -> splitStateMap.get(splitId).complete() == IcebergSourceSplitStatus.ASSIGNED)
            .map(splitId -> splitStateMap.get(splitId).getSplit())
            .collect(Collectors.toList());

    if (!completedSplitIds.isEmpty()) {
      numAssignedSplits -= completedSplitIds.size();
      numCompletedSplits += completedSplitIds.size();
      updateTs();
      updateListeners(completedSplitIds, IcebergSourceSplitStatus.ASSIGNED, IcebergSourceSplitStatus.COMPLETED);

      // check if we have reached the terminal condition
      if (isTerminal()) {
        updateListenersOnTerminalCondition();
      }
    }
    return this;
  }

  private void updateListenersOnTerminalCondition() {
    listeners.keySet().forEach(listener -> listener.onNoMoreStatusChanges());
  }

  @VisibleForTesting
  synchronized Collection<IcebergSourceSplit> getUnassignedSplits() {
    return splitStateMap
        .values()
        .stream()
        .filter(SplitState::isUnassigned)
        .map(s -> s.getSplit())
        .collect(Collectors.toSet());
  }

  public synchronized Map<IcebergSourceSplit, String> getAssignedSplits() {
    return splitStateMap
        .values()
        .stream()
        .filter(SplitState::isAssigned)
        .collect(Collectors.toMap(SplitState::getSplit, SplitState::getSubtaskId));
  }

  public synchronized Collection<IcebergSourceSplitState> snapshotState() {
    return splitStateMap
        .values()
        .stream()
        .filter(SplitState::notCompleted)
        .map(splitState -> new IcebergSourceSplitState(splitState.getSplit(), splitState.getStatus()))
        .collect(Collectors.toList());
  }

  public int getTotalSplits() {
    return splitStateMap.size();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("assignedSplits", numAssignedSplits)
        .add("completedSplits", numCompletedSplits)
        .add("totalSplits", splitStateMap.size())
        .toString();
  }

  private void updateListeners(
      Collection<IcebergSourceSplit> splits, IcebergSourceSplitStatus oldStatus, IcebergSourceSplitStatus newStatus) {
    if (!splits.isEmpty()) {
      switch (newStatus) {
        case UNASSIGNED:
          if (oldStatus == IcebergSourceSplitStatus.COMPLETED) {
            listeners.keySet().forEach(listener -> listener.onSplitsAdded(splits));
          } else if (oldStatus == IcebergSourceSplitStatus.ASSIGNED) {
            listeners.keySet().forEach(listener -> listener.onSplitsUnassigned(splits));
          } else {
            throw new IllegalArgumentException();
          }
          break;
        case ASSIGNED:
          listeners.keySet().forEach(listener -> listener.onSplitsAssigned(splits));
          break;
        case COMPLETED:
          listeners.keySet().forEach(listener -> listener.onSplitsCompleted(splits));
          break;
        default:
          throw new IllegalArgumentException();
      }
    }
  }

  private void updateTs() {
    this.lastUpdatedTs = Instant.ofEpochMilli(clock.millis());
    log.info("state={}", this);
  }

  // this is an expensive function. Please use this carefully.
  public synchronized Map<String, List<IcebergSourceSplit>> getCompletedSplits() {
    Map<String, List<IcebergSourceSplit>> completedMap = Maps.newHashMap();
    splitStateMap
        .values()
        .forEach(
            splitState -> {
              if (splitState.isCompleted()) {
                String subtaskId = splitState.getSubtaskId();
                completedMap.compute(
                    subtaskId,
                    (dontCare, oldValue) -> {
                      if (oldValue == null) {
                        List<IcebergSourceSplit> temp = Lists.newArrayList();
                        temp.add(splitState.getSplit());
                        return temp;
                      } else {
                        oldValue.add(splitState.getSplit());
                        return oldValue;
                      }
                    });
              }
            });

    return completedMap;
  }

  // checks if the enumerator state has reached the terminal condition
  public boolean isTerminal() {
    // check if there are no more splits to be added by the system
    // check if all the splits have been completed
    return noMoreSplits && numCompletedSplits == splitStateMap.size();
  }

  public synchronized Stats getStats() {
    return new Stats(
        splitStateMap.size() - numAssignedSplits - numCompletedSplits,
        numAssignedSplits,
        numCompletedSplits,
        splitStateMap.size());
  }

  public static class Stats {

    private final int numUnassignedSplits;
    private final int numAssignedSplits;
    private final int numCompletedSplits;
    private final int numTotalSplits;

    Stats(int numUnassignedSplits, int numAssignedSplits, int numCompletedSplits, int numTotalSplits) {
      this.numUnassignedSplits = numUnassignedSplits;
      this.numAssignedSplits = numAssignedSplits;
      this.numCompletedSplits = numCompletedSplits;
      this.numTotalSplits = numTotalSplits;
    }

    public int getNumUnassignedSplits() {
      return numUnassignedSplits;
    }

    public int getNumAssignedSplits() {
      return numAssignedSplits;
    }

    public int getNumCompletedSplits() {
      return numCompletedSplits;
    }

    public int getNumTotalSplits() {
      return numTotalSplits;
    }
  }

  static class SplitState implements Serializable {

    private IcebergSourceSplit split;
    private IcebergSourceSplitStatus status;
    private String subtaskId;

    SplitState(IcebergSourceSplit split, IcebergSourceSplitStatus status) {
      this.split = split;
      this.status = status;
      this.subtaskId = null;
    }

    private static SplitState of(IcebergSourceSplit split) {
      return new SplitState(split, IcebergSourceSplitStatus.UNASSIGNED);
    }

    private synchronized IcebergSourceSplitStatus assignTo(@Nullable String hostName) {
      switch (status) {
        case ASSIGNED:
          Preconditions.checkArgument(hostName == null || this.subtaskId.equals(hostName));
          return IcebergSourceSplitStatus.ASSIGNED;
        case UNASSIGNED:
          this.status = IcebergSourceSplitStatus.ASSIGNED;
          this.subtaskId = hostName;
          return IcebergSourceSplitStatus.UNASSIGNED;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "This transition is not possible as the split is currently in %s state", this));
      }
    }

    private synchronized IcebergSourceSplitStatus complete() {
      switch (status) {
        case COMPLETED:
          return IcebergSourceSplitStatus.COMPLETED;
        case ASSIGNED:
          this.status = IcebergSourceSplitStatus.COMPLETED;
          return IcebergSourceSplitStatus.ASSIGNED;
        default:
          throw new IllegalArgumentException(
              String.format(
                  "This transition is not possible as the split is currently in %s state", this));
      }
    }

    private synchronized IcebergSourceSplitStatus unassign() {
      switch (status) {
        case UNASSIGNED:
          return IcebergSourceSplitStatus.UNASSIGNED;
        case ASSIGNED:
          this.status = IcebergSourceSplitStatus.UNASSIGNED;
          this.subtaskId = null;
          return IcebergSourceSplitStatus.ASSIGNED;
        case COMPLETED:
          this.status = IcebergSourceSplitStatus.UNASSIGNED;
          this.subtaskId = null;
          log.warn(
              "This transition should not be possible as the split is currently in {} state", this);
          return IcebergSourceSplitStatus.COMPLETED;
        default:
          throw new IllegalArgumentException(String.format("Unexpected status %s", status));
      }
    }

    private boolean isUnassigned() {
      return status.equals(IcebergSourceSplitStatus.UNASSIGNED);
    }

    private boolean isAssigned() {
      return status.equals(IcebergSourceSplitStatus.ASSIGNED);
    }

    private boolean isCompleted() {
      return status.equals(IcebergSourceSplitStatus.COMPLETED);
    }

    private boolean notCompleted() {
      return !isCompleted();
    }

    public IcebergSourceSplit getSplit() {
      return split;
    }

    public String getSubtaskId() {
      return subtaskId;
    }

    public IcebergSourceSplitStatus getStatus() {
      return status;
    }
  }

  public interface StateChangeListener {
    void onSplitsAdded(Collection<IcebergSourceSplit> splits);

    void onSplitsAssigned(Collection<IcebergSourceSplit> splits);

    void onSplitsUnassigned(Collection<IcebergSourceSplit> splits);

    void onSplitsCompleted(Collection<IcebergSourceSplit> splits);

    void onNoMoreStatusChanges();
  }
}
