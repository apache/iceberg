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

import java.io.Serializable;
import java.util.List;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * What a sink writer produced for one checkpoint: the files it created plus, when the sink resolves
 * equality deletes to deletion vectors itself, the unresolved deletes, the locations of the rows it
 * wrote and the positions of the rows it already knows to be deleted.
 *
 * <p>{@code deleteKeys}, {@code liveRows} and {@code positionDeletes} are empty unless {@link
 * org.apache.iceberg.flink.FlinkWriteOptions#DV_ONLY_ENABLE} is set, so the default write path
 * carries no extra payload.
 *
 * <p>The DV-only write path also uses it downstream of the writer, to carry the deletion vectors it
 * writes as files, and to report the snapshot its primary key index reflects, see {@link
 * #baseline(Long)}.
 */
@Internal
public class SinkWriteResult implements Serializable {

  private static final WriteResult EMPTY = WriteResult.builder().build();

  private final WriteResult writeResult;
  private final List<SerializedEqualityValues> deleteKeys;
  private final List<PkIndexEntry> liveRows;
  private final List<DVPosition> positionDeletes;
  private final boolean baseline;
  private final Long baselineSnapshotId;

  public SinkWriteResult(WriteResult writeResult) {
    this(writeResult, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), false, null);
  }

  public SinkWriteResult(
      WriteResult writeResult,
      List<SerializedEqualityValues> deleteKeys,
      List<PkIndexEntry> liveRows,
      List<DVPosition> positionDeletes) {
    this(writeResult, deleteKeys, liveRows, positionDeletes, false, null);
  }

  private SinkWriteResult(
      WriteResult writeResult,
      List<SerializedEqualityValues> deleteKeys,
      List<PkIndexEntry> liveRows,
      List<DVPosition> positionDeletes,
      boolean baseline,
      Long baselineSnapshotId) {
    this.writeResult = writeResult;
    this.deleteKeys = deleteKeys;
    this.liveRows = liveRows;
    this.positionDeletes = positionDeletes;
    this.baseline = baseline;
    this.baselineSnapshotId = baselineSnapshotId;
  }

  /**
   * Carries no files, only the snapshot that the primary key index reflected when the deletes of a
   * checkpoint were resolved.
   *
   * @param snapshotId the snapshot, or null when the branch had none
   */
  static SinkWriteResult baseline(Long snapshotId) {
    return new SinkWriteResult(
        EMPTY, ImmutableList.of(), ImmutableList.of(), ImmutableList.of(), true, snapshotId);
  }

  /** Whether this result only reports the baseline snapshot, see {@link #baseline(Long)}. */
  boolean isBaseline() {
    return baseline;
  }

  public WriteResult writeResult() {
    return writeResult;
  }

  /** Deletes that matched no row written in the same checkpoint and still need to be resolved. */
  public List<SerializedEqualityValues> deleteKeys() {
    return deleteKeys;
  }

  /**
   * Locations of the rows this writer wrote that are still live when it completes, used to maintain
   * the primary key index.
   */
  public List<PkIndexEntry> liveRows() {
    return liveRows;
  }

  /** Rows written in the same checkpoint that a later change of the same key removed again. */
  public List<DVPosition> positionDeletes() {
    return positionDeletes;
  }

  /**
   * Snapshot the primary key index reflected when this checkpoint's deletes were resolved, or null
   * when the branch had none or this result does not carry one.
   */
  Long baselineSnapshotId() {
    return baselineSnapshotId;
  }
}
