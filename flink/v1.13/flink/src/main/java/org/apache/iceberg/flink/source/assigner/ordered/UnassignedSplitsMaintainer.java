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
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.iceberg.flink.source.split.IcebergSourceSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class UnassignedSplitsMaintainer
    implements EventTimeAlignmentAssignerState.StateChangeListener {
  private static final Logger log = LoggerFactory.getLogger(UnassignedSplitsMaintainer.class);

  private final SortedSet<IcebergSourceSplit> unassignedSplits;

  UnassignedSplitsMaintainer(
      Comparator<IcebergSourceSplit> comparator, EventTimeAlignmentAssignerState assignerState) {
    this.unassignedSplits = new TreeSet<>(comparator);
    assignerState.register(this);
  }

  public Collection<IcebergSourceSplit> getUnassignedSplits() {
    return Collections.unmodifiableCollection(unassignedSplits);
  }

  @Override
  public void onSplitsAdded(Collection<IcebergSourceSplit> splits) {
    unassignedSplits.addAll(splits);
  }

  @Override
  public void onSplitsAssigned(Collection<IcebergSourceSplit> splits) {
    for (IcebergSourceSplit split : splits) {
      if (!unassignedSplits.contains(split)) {
        log.error("split={} not found in unassigned splits", split);
        throw new IllegalArgumentException("split not found in unassigned splits");
      }
    }
    unassignedSplits.removeAll(splits);
  }

  @Override
  public void onSplitsUnassigned(Collection<IcebergSourceSplit> splits) {
    unassignedSplits.addAll(splits);
  }

  @Override
  public void onSplitsCompleted(Collection<IcebergSourceSplit> splits) {
  }

  @Override
  public void onNoMoreStatusChanges() {
  }
}
