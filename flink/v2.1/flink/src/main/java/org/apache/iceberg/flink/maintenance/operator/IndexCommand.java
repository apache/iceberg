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

import java.io.Serializable;
import org.apache.flink.annotation.Internal;

/**
 * Command from the {@code EqualityConvertPlanner} to the {@code EqualityConvertPKIndex}.
 *
 * <p>{@link Type#ADD_DATA_ROW} adds an existing main-data row to the worker's PK index shard
 * immediately, so this cycle's delete can remove it. {@link Type#ADD_STAGING_DATA_ROW} adds a
 * staging snapshot's new data row; the index applies it only after this cycle's delete resolves, so
 * a re-inserted key survives a same-cycle delete. Both carry the row location as {@link
 * #rowPosition}. {@link Type#RESOLVE_DELETE} resolves an equality delete key against the index and
 * emits {@link DVPosition}s for all matching rows. All three flow through the keyed stream and
 * route via {@link #key}.
 *
 * <p>{@link Type#CLEAR_INDEX} is emitted on the broadcast side when an external commit has advanced
 * the main branch and the worker must evict keyed entries that won't be re-added by the upcoming
 * reindex (e.g. PKs whose data file was removed by CoW). Has no {@link #key} or row position.
 * Carries the new {@link #mainSequenceNumber} as the staleness threshold.
 *
 * <p>{@link #rowPosition} is the data row's location, set for the two add types and null otherwise;
 * the data sequence number it carries lets the worker apply a delete only to older rows. {@code
 * deleteSequenceNumber} is the equality delete's sequence number, set for {@link
 * Type#RESOLVE_DELETE} and unused (-1) otherwise. {@code deleteSpecId} is the partition spec id of
 * the delete file, used to scope a partitioned delete to data rows of the same spec; {@link
 * #GLOBAL_DELETE_SPEC_ID} marks an unpartitioned delete that applies to every spec. Set for {@link
 * Type#RESOLVE_DELETE} and unused (-1) otherwise.
 */
@Internal
public record IndexCommand(
    Type type,
    Long mainSnapshotId,
    Long mainSequenceNumber,
    SerializedEqualityValues key,
    DVPosition rowPosition,
    long deleteSequenceNumber,
    int deleteSpecId)
    implements Serializable {

  /** Spec id sentinel for an unpartitioned equality delete, which applies as a global delete. */
  public static final int GLOBAL_DELETE_SPEC_ID = -1;

  public enum Type {
    ADD_DATA_ROW,
    ADD_STAGING_DATA_ROW,
    RESOLVE_DELETE,
    CLEAR_INDEX
  }

  public static IndexCommand addDataRow(
      Long mainSnapshotId,
      Long mainSequenceNumber,
      SerializedEqualityValues key,
      String filePath,
      long position,
      int specId,
      byte[] partition,
      long dataSequenceNumber,
      boolean staging) {
    return new IndexCommand(
        staging ? Type.ADD_STAGING_DATA_ROW : Type.ADD_DATA_ROW,
        mainSnapshotId,
        mainSequenceNumber,
        key,
        new DVPosition(filePath, position, specId, partition, dataSequenceNumber),
        -1,
        -1);
  }

  public static IndexCommand resolveDelete(
      Long mainSnapshotId,
      Long mainSequenceNumber,
      SerializedEqualityValues key,
      long deleteSequenceNumber,
      int deleteSpecId) {
    return new IndexCommand(
        Type.RESOLVE_DELETE,
        mainSnapshotId,
        mainSequenceNumber,
        key,
        null,
        deleteSequenceNumber,
        deleteSpecId);
  }

  public static IndexCommand clearBeforeReindex(long mainSnapshotId, long mainSequenceNumber) {
    return new IndexCommand(
        Type.CLEAR_INDEX, mainSnapshotId, mainSequenceNumber, null, null, -1, -1);
  }
}
