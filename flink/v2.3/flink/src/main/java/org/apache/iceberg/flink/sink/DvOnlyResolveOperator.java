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
package org.apache.iceberg.flink.sink;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Resolves equality deletes against a primary key index, so that a delete can be expressed as a
 * position in the data file that holds the row. Keyed by the serialized equality values, so every
 * record for one key is handled by the same subtask.
 *
 * <p>The index keeps the location of the live row (or rows, if the source produced duplicates) for
 * each key. The deletes and the rows written during a checkpoint are accumulated per key and
 * applied when the checkpoint barrier arrives, in two phases: deletes resolve against the index
 * first, then the rows written during the checkpoint enter it. Applying them in this order is what
 * lets a key that was deleted and re-inserted in the same checkpoint survive, and it removes the
 * need for the event-time timers the post-commit converter relies on. Rows the table already holds
 * and the removal of positions take effect right away.
 *
 * <p>The accumulated records are held in memory rather than in keyed state: the barrier drains them
 * before the state snapshot is taken, so they are always empty when the snapshot happens and a
 * restore replays them from the writer. Memory therefore scales with the number of distinct keys
 * changed per checkpoint, not with the table size. The index itself is keyed state and is backed by
 * the configured state backend.
 *
 * <p>An indexed position stops being valid once the data file holding the row leaves the table,
 * which is what a compaction does. Such positions are removed as they are reported by {@link
 * DvOnlyFileIndexOperator}, and the rows of the replacement files are reported the same way as the
 * rows the index started with.
 */
class DvOnlyResolveOperator extends AbstractStreamOperator<DVPosition>
    implements OneInputStreamOperator<DvOnlyRecord, DVPosition> {

  private static final Logger LOG = LoggerFactory.getLogger(DvOnlyResolveOperator.class);

  private static final MapStateDescriptor<DVPosition, Boolean> LIVE_POSITIONS_DESCRIPTOR =
      new MapStateDescriptor<>(
          "dvOnlyLivePositions", TypeInformation.of(DVPosition.class), Types.BOOLEAN);

  /** Live positions of one key, kept as a set so that a file read again cannot duplicate one. */
  private transient MapState<DVPosition, Boolean> livePositions;

  /** Changes accumulated for the current checkpoint, keyed by equality values. */
  private transient Map<SerializedEqualityValues, PendingChange> pending;

  @Override
  public void open() throws Exception {
    super.open();
    DvOnlyExecution.checkStreaming(
        getOperatorConfig(), getRuntimeContext().getUserCodeClassLoader());
    livePositions = getRuntimeContext().getMapState(LIVE_POSITIONS_DESCRIPTOR);
    pending = Maps.newLinkedHashMap();
  }

  @Override
  public void processElement(StreamRecord<DvOnlyRecord> element) throws Exception {
    DvOnlyRecord record = element.getValue();
    switch (record.type()) {
      case DELETE -> changeFor(record).deleted = true;
      case ADD_ROW -> changeFor(record).added.add(record.position());
      case BOOTSTRAP_ROW -> indexCommittedRow(record.position());
      case DROP_POSITIONS -> dropPositions(record);
      default ->
          throw new IllegalStateException(
              "Unexpected record in the primary key index: " + record.type());
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    resolvePending();
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  /**
   * Resolves what is left when the input ends. No further barrier arrives then, and the files of
   * the last changes are committed from what the operators emit while they finish.
   */
  @Override
  public void finish() throws Exception {
    resolvePending();
    super.finish();
  }

  private void resolvePending() throws Exception {
    int resolved = 0;
    for (Map.Entry<SerializedEqualityValues, PendingChange> entry : pending.entrySet()) {
      // Neither the barrier nor the end of the input is delivered per key, so the key context has
      // to be set explicitly before touching the keyed index.
      setCurrentKey(entry.getKey());
      resolved += apply(entry.getValue());
    }

    if (!pending.isEmpty()) {
      LOG.debug("Resolved {} deleted row(s) from {} changed key(s)", resolved, pending.size());
    }

    pending.clear();
  }

  // A row the table already holds enters the index right away, so a delete of the current
  // checkpoint can resolve it. Reporting it again when its data file is read again.
  private void indexCommittedRow(DVPosition position) throws Exception {
    livePositions.put(position, true);
  }

  private int apply(PendingChange change) throws Exception {
    int resolved = 0;
    if (change.deleted) {
      for (DVPosition position : livePositions.keys()) {
        output.collect(new StreamRecord<>(position));
        resolved++;
      }

      livePositions.clear();
    }

    for (DVPosition position : change.added) {
      livePositions.put(position, true);
    }

    return resolved;
  }

  private void dropPositions(DvOnlyRecord record) throws Exception {
    List<DVPosition> dropped = Lists.newArrayList();
    for (DVPosition position : livePositions.keys()) {
      if (record.filePath().equals(position.dataFilePath())) {
        dropped.add(position);
      }
    }

    for (DVPosition position : dropped) {
      livePositions.remove(position);
    }
  }

  private PendingChange changeFor(DvOnlyRecord record) {
    return pending.computeIfAbsent(record.key(), key -> new PendingChange());
  }

  private static final class PendingChange {
    private final List<DVPosition> added = Lists.newArrayList();
    private boolean deleted;
  }
}
