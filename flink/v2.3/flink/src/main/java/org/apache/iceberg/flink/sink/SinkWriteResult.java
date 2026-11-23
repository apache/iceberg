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
import org.apache.iceberg.flink.maintenance.operator.SerializedEqualityValues;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * What a sink writer produced for one checkpoint: the files it created plus, when the sink resolves
 * equality deletes to deletion vectors itself, the unresolved deletes and the locations of the rows
 * it wrote.
 *
 * <p>{@code deleteKeys} and {@code liveRows} are empty unless {@link
 * org.apache.iceberg.flink.FlinkWriteOptions#DV_ONLY_ENABLE} is set, so the default write path
 * carries no extra payload.
 *
 * <p>The DV-only write path also uses it downstream of the writer, see {@link Kind}.
 */
@Internal
public class SinkWriteResult implements Serializable {

  private static final WriteResult EMPTY = WriteResult.builder().build();

  enum Kind {
    /** Files written by a sink writer, with the payload of the DV-only write path if any. */
    WRITER,
    /** No files, only the snapshot the primary key index reflected, see {@link #baseline}. */
    BASELINE,
    /** Deletion vectors and the snapshot they were merged with, see {@link #deletionVectors}. */
    DELETION_VECTORS
  }

  private final Kind kind;
  private final WriteResult writeResult;
  private final List<SerializedEqualityValues> deleteKeys;
  private final List<PkIndexEntry> liveRows;
  private final Long snapshotId;

  public SinkWriteResult(WriteResult writeResult) {
    this(writeResult, ImmutableList.of(), ImmutableList.of());
  }

  public SinkWriteResult(
      WriteResult writeResult,
      List<SerializedEqualityValues> deleteKeys,
      List<PkIndexEntry> liveRows) {
    this(Kind.WRITER, writeResult, deleteKeys, liveRows, null);
  }

  private SinkWriteResult(
      Kind kind,
      WriteResult writeResult,
      List<SerializedEqualityValues> deleteKeys,
      List<PkIndexEntry> liveRows,
      Long snapshotId) {
    this.kind = kind;
    this.writeResult = writeResult;
    this.deleteKeys = deleteKeys;
    this.liveRows = liveRows;
    this.snapshotId = snapshotId;
  }

  /**
   * Carries no files, only the snapshot that the primary key index reflected when the deletes of a
   * checkpoint were resolved.
   *
   * @param snapshotId the snapshot, or null when the branch had none
   */
  static SinkWriteResult baseline(Long snapshotId) {
    return new SinkWriteResult(
        Kind.BASELINE, EMPTY, ImmutableList.of(), ImmutableList.of(), snapshotId);
  }

  /**
   * Carries the deletion vectors written for a checkpoint, merged with the vectors their data files
   * carried in the given snapshot.
   *
   * @param snapshotId the snapshot, or null when the branch had none
   */
  static SinkWriteResult deletionVectors(WriteResult result, Long snapshotId) {
    return new SinkWriteResult(
        Kind.DELETION_VECTORS, result, ImmutableList.of(), ImmutableList.of(), snapshotId);
  }

  Kind kind() {
    return kind;
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

  /**
   * The snapshot of a {@link Kind#BASELINE} or {@link Kind#DELETION_VECTORS} result, or null when
   * the branch had none or the result is a {@link Kind#WRITER} one.
   */
  Long snapshotId() {
    return snapshotId;
  }
}
