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
package org.apache.iceberg.flink.maintenance.operator;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parallel operator that maintains a shard of the primary key index. Keyed by the full serialized
 * primary key ({@link SerializedEqualityValues}), each instance handles a subset of PK values.
 *
 * <p>Existing main data is indexed immediately on arrival. A staging snapshot's new data rows
 * ({@link IndexCommand.Type#ADD_STAGING_DATA_ROW}) and every RESOLVE_DELETE instead register an
 * event-time timer at their phase timestamp and act only when the watermark reaches it. Flink fires
 * timers in timestamp order, so a delete resolves before the cycle's own new staging rows (a
 * strictly later phase) join the index: a re-inserted key survives a same-cycle delete regardless
 * of the order the parallel readers deliver records. Main data is safe to apply eagerly because its
 * phase precedes the delete, so the delete's watermark only advances once every main row has
 * arrived.
 *
 * <p>On a shared staging/target branch, resolution is sequence-number aware: an equality delete
 * deletes an indexed row only when the row's data sequence number is below the delete's. A
 * re-insert of the same key with a higher sequence survives and stays indexed for a later delete.
 * On a separate target branch the committer reassigns data sequence numbers, so every match is
 * deleted; event-time ordering prevents over-deletion.
 *
 * <p>Stale-index protection runs on two levels. Each key tracks the main sequence number its stored
 * positions were indexed against. The sequence number is unique and monotonic per snapshot, so it
 * serves both the equality test below and the ordering test:
 *
 * <ul>
 *   <li><b>Lazy (per-key)</b>: any keyed command at the top of {@link #processElement} whose {@link
 *       IndexCommand#mainSequenceNumber()} differs from the stored one clears stale state and
 *       adopts the command's sequence number. Equality suffices here. Required because the
 *       broadcast and keyed inputs are independent streams with no ordering guarantee; without it,
 *       an ADD_DATA_ROW that arrived before the broadcast would be wrongly evicted.
 *   <li><b>Eager (all keys)</b>: a CLEAR_INDEX broadcast iterates all keys on the subtask via
 *       {@link KeyedBroadcastProcessFunction.Context#applyToKeyedState} and clears any whose stored
 *       sequence number is older than the broadcast's. Staleness is ordered by sequence number.
 *       Bounds state size for PKs that were removed from main by an external CoW commit and won't
 *       receive any keyed command next cycle.
 * </ul>
 */
@Internal
public class EqualityConvertPKIndex
    extends KeyedBroadcastProcessFunction<
        SerializedEqualityValues, IndexCommand, IndexCommand, DVPosition> {

  private static final Logger LOG = LoggerFactory.getLogger(EqualityConvertPKIndex.class);

  private static final String RESOLVED_DELETE_NUM_METRIC = "resolvedDeleteNum";
  private static final String INDEXED_KEY_NUM_METRIC = "indexedKeyNum";
  private static final String EAGERLY_EVICTED_KEY_NUM_METRIC = "eagerlyEvictedKeyNum";

  public static final MapStateDescriptor<Void, Void> CLEAR_BROADCAST_DESCRIPTOR =
      new MapStateDescriptor<>("eq-convert-clear-broadcast", Types.VOID, Types.VOID);

  private static final ValueStateDescriptor<Long> MAIN_SEQUENCE_VERSION_DESCRIPTOR =
      new ValueStateDescriptor<>("mainSequenceVersion", Types.LONG);
  private static final ListStateDescriptor<DVPosition> DATA_ROW_POSITIONS_DESCRIPTOR =
      new ListStateDescriptor<>("filePositions", TypeInformation.of(DVPosition.class));
  private static final ListStateDescriptor<DVPosition> BUFFERED_ROWS_DESCRIPTOR =
      new ListStateDescriptor<>("bufferedRows", TypeInformation.of(DVPosition.class));
  private static final ValueStateDescriptor<Long> RESOLVE_TIMESTAMP_DESCRIPTOR =
      new ValueStateDescriptor<>("resolveTimestamp", Types.LONG);
  private static final ValueStateDescriptor<Long> RESOLVE_SEQUENCE_NUMBER_DESCRIPTOR =
      new ValueStateDescriptor<>("resolveSequenceNumber", Types.LONG);
  private static final ListStateDescriptor<Integer> RESOLVE_SPEC_IDS_DESCRIPTOR =
      new ListStateDescriptor<>("resolveSpecIds", Types.INT);

  private transient ValueState<Long> mainSequenceVersion;
  // Resolvable rows for this key. Populated immediately for main data, or from onTimer for
  // staging rows once their phase watermark passes, so a delete never resolves against a row from a
  // later phase.
  private transient ListState<DVPosition> dataRowPositions;
  // Deferred staging-snapshot rows awaiting their event-time timer. onTimer moves them into
  // dataRowPositions after this cycle's delete has resolved.
  private transient ListState<DVPosition> bufferedRows;
  // Phase timestamp of the pending RESOLVE_DELETE; onTimer resolves when the firing timer matches.
  private transient ValueState<Long> resolveTimestamp;
  // Maximum sequence number among the cycle's RESOLVE_DELETEs for this key. Deletes a
  // data row only when the row's sequence number is below it.
  private transient ValueState<Long> resolveSequenceNumber;
  // Spec ids of the cycle's RESOLVE_DELETEs for this key. A delete phase may carry same-key deletes
  // from multiple specs, and each scope must apply.
  private transient ListState<Integer> resolveSpecIds;

  private transient Counter resolvedDeleteNumCounter;
  private transient Counter indexedKeyNumCounter;
  private transient Counter eagerlyEvictedKeyNumCounter;

  private final boolean stagingOnTargetBranch;

  public EqualityConvertPKIndex(boolean stagingOnTargetBranch) {
    this.stagingOnTargetBranch = stagingOnTargetBranch;
  }

  @Override
  public void open(OpenContext context) throws Exception {
    super.open(context);
    mainSequenceVersion = getRuntimeContext().getState(MAIN_SEQUENCE_VERSION_DESCRIPTOR);
    dataRowPositions = getRuntimeContext().getListState(DATA_ROW_POSITIONS_DESCRIPTOR);
    bufferedRows = getRuntimeContext().getListState(BUFFERED_ROWS_DESCRIPTOR);
    resolveTimestamp = getRuntimeContext().getState(RESOLVE_TIMESTAMP_DESCRIPTOR);
    resolveSequenceNumber = getRuntimeContext().getState(RESOLVE_SEQUENCE_NUMBER_DESCRIPTOR);
    resolveSpecIds = getRuntimeContext().getListState(RESOLVE_SPEC_IDS_DESCRIPTOR);
    resolvedDeleteNumCounter =
        getRuntimeContext().getMetricGroup().counter(RESOLVED_DELETE_NUM_METRIC);
    indexedKeyNumCounter = getRuntimeContext().getMetricGroup().counter(INDEXED_KEY_NUM_METRIC);
    eagerlyEvictedKeyNumCounter =
        getRuntimeContext().getMetricGroup().counter(EAGERLY_EVICTED_KEY_NUM_METRIC);
  }

  @Override
  public void processElement(IndexCommand cmd, ReadOnlyContext ctx, Collector<DVPosition> out)
      throws Exception {
    try {
      Long storedSequence = mainSequenceVersion.value();
      if (!Objects.equals(storedSequence, cmd.mainSequenceNumber())) {
        LOG.info(
            "Main sequence changed from {} to {} (snapshot {}), clearing state",
            storedSequence,
            cmd.mainSequenceNumber(),
            cmd.mainSnapshotId());
        clearKeyState();
        mainSequenceVersion.update(cmd.mainSequenceNumber());
      }

      long ts = ctx.timestamp();

      if (cmd.type() == IndexCommand.Type.ADD_DATA_ROW) {
        // Existing main data: index immediately so this cycle's delete can remove it.
        dataRowPositions.add(cmd.rowPosition());
        indexedKeyNumCounter.inc();
      } else if (cmd.type() == IndexCommand.Type.ADD_STAGING_DATA_ROW) {
        // Staging snapshot's new data: defer until after this cycle's delete resolves so a
        // re-inserted key survives. Applied by the event-time timer at this phase.
        bufferedRows.add(cmd.rowPosition());
        ctx.timerService().registerEventTimeTimer(ts);
      } else if (cmd.type() == IndexCommand.Type.RESOLVE_DELETE) {
        Long resolveTs = resolveTimestamp.value();
        if (resolveTs == null || ts > resolveTs) {
          resolveTimestamp.update(ts);
        }

        Long currentSeq = resolveSequenceNumber.value();
        if (currentSeq == null || cmd.deleteSequenceNumber() > currentSeq) {
          resolveSequenceNumber.update(cmd.deleteSequenceNumber());
        }

        // Accumulate every delete's scope.
        // One delete phase can carry same-key deletes from multiple specs.
        resolveSpecIds.add(cmd.deleteSpecId());

        ctx.timerService().registerEventTimeTimer(ts);
      } else {
        throw new IllegalStateException("Unexpected command type in keyed stream: " + cmd.type());
      }
    } catch (Exception e) {
      LOG.error("PKIndex failed to process command of type {}", cmd.type(), e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      out.collect(DVPosition.ABORT);
    }
  }

  @Override
  public void processBroadcastElement(IndexCommand cmd, Context ctx, Collector<DVPosition> out) {
    Preconditions.checkArgument(
        cmd.type() == IndexCommand.Type.CLEAR_INDEX,
        "Broadcast element must be %s",
        IndexCommand.Type.CLEAR_INDEX);

    final long broadcastSequenceNumber = cmd.mainSequenceNumber();
    try {
      ctx.applyToKeyedState(
          MAIN_SEQUENCE_VERSION_DESCRIPTOR,
          (key, sequenceState) -> {
            Long storedSequenceNumber = sequenceState.value();
            if (storedSequenceNumber != null && storedSequenceNumber < broadcastSequenceNumber) {
              clearKeyState();
              sequenceState.update(broadcastSequenceNumber);
              eagerlyEvictedKeyNumCounter.inc();
            }
          });
    } catch (Exception e) {
      LOG.error("PKIndex failed to apply CLEAR_INDEX for snapshot {}", cmd.mainSnapshotId(), e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      out.collect(DVPosition.ABORT);
    }
  }

  @Override
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<DVPosition> out) {
    try {
      // Timers fire in ascending timestamp order. The delete-phase timer resolves against the
      // index; any other timer is a staging phase whose buffered rows now join the index.
      // The delete fires before the (strictly later) staging phase, so the cycle's own new rows are
      // applied only after the delete and survive it.
      Long resolveTs = resolveTimestamp.value();
      if (resolveTs != null && timestamp == resolveTs) {
        resolveDeletes(out);
        resolveTimestamp.clear();
        resolveSequenceNumber.clear();
        resolveSpecIds.clear();
      } else {
        applyBufferedRows();
      }
    } catch (Exception e) {
      LOG.error("PKIndex failed in onTimer at ts={}", timestamp, e);
      ctx.output(TaskResultAggregator.ERROR_STREAM, e);
      out.collect(DVPosition.ABORT);
    }
  }

  private void applyBufferedRows() throws Exception {
    for (DVPosition position : bufferedRows.get()) {
      dataRowPositions.add(position);
      indexedKeyNumCounter.inc();
    }

    bufferedRows.clear();
  }

  private void resolveDeletes(Collector<DVPosition> out) throws Exception {
    // An equality delete with sequence S deletes only data rows with sequence < S. Rows at or
    // above S are re-inserts the delete does not affect; keep them indexed for a later delete.
    // Only valid on a shared staging/target branch, where data and deletes share one sequence
    // space. On a separate target branch the committer reassigns data sequence numbers, so the
    // indexed sequence is not comparable to the staging delete's; event-time ordering already
    // prevents false deletion.
    Long deleteSeq = resolveSequenceNumber.value();
    // A partitioned delete applies only to data rows of the same spec. An unpartitioned delete
    // (GLOBAL_DELETE_SPEC_ID) applies to every spec. deleteSpecIds holds every scope seen this
    // cycle (usually one).
    Set<Integer> deleteSpecIds = Sets.newHashSet(resolveSpecIds.get());
    boolean globalDelete = deleteSpecIds.contains(IndexCommand.GLOBAL_DELETE_SPEC_ID);
    List<DVPosition> nonEligible = Lists.newArrayList();
    int count = 0;
    for (DVPosition pos : dataRowPositions.get()) {
      boolean specMatch = globalDelete || deleteSpecIds.contains(pos.specId());
      boolean sequenceMatch =
          !stagingOnTargetBranch || deleteSeq == null || pos.dataSequenceNumber() < deleteSeq;
      if (specMatch && sequenceMatch) {
        out.collect(pos);
        count++;
      } else {
        nonEligible.add(pos);
      }
    }

    resolvedDeleteNumCounter.inc(count);
    dataRowPositions.update(nonEligible);
  }

  private void clearKeyState() {
    dataRowPositions.clear();
    bufferedRows.clear();
    resolveTimestamp.clear();
    resolveSequenceNumber.clear();
    resolveSpecIds.clear();
  }
}
