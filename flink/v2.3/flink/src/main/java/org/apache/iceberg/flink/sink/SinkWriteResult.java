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
 */
@Internal
public class SinkWriteResult implements Serializable {

  /** What a result carries. Others are added along with whatever produces them. */
  enum Kind {
    /** Files written by a sink writer, with the payload of the DV-only write path if any. */
    WRITER
  }

  private final Kind kind;
  private final WriteResult writeResult;
  private final List<SerializedEqualityValues> deleteKeys;
  private final List<PkIndexEntry> liveRows;

  public SinkWriteResult(WriteResult writeResult) {
    this(writeResult, ImmutableList.of(), ImmutableList.of());
  }

  public SinkWriteResult(
      WriteResult writeResult,
      List<SerializedEqualityValues> deleteKeys,
      List<PkIndexEntry> liveRows) {
    this.kind = Kind.WRITER;
    this.writeResult = writeResult;
    this.deleteKeys = deleteKeys;
    this.liveRows = liveRows;
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
}
