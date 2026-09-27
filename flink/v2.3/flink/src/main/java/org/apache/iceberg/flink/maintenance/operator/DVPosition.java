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
 * A deletion vector position emitted by {@code EqualityConvertPKIndex} when resolving an equality
 * delete. Identifies a specific row (by file path and position) to be marked as deleted, and
 * carries the data file's {@code specId} + encoded partition so the downstream resolver can write
 * the DV without re-reading data manifests.
 *
 * <p>{@code dataSequenceNumber} is the data file's sequence number. The worker keeps it in its
 * index so a resolving equality delete only deletes rows older than itself (only applies an
 * equality delete with sequence {@code S} to data with sequence {@code < S}).
 *
 * <p>{@code partition} is a {@code byte[]} (see {@link StructLikeSerializer#encodePartition}) so
 * Flink serializes it natively rather than falling back to Kryo, including in the worker's keyed
 * state.
 */
@Internal
public record DVPosition(
    String dataFilePath, long position, int specId, byte[] partition, long dataSequenceNumber)
    implements Serializable {

  /**
   * Sentinel filePath for the {@link #ABORT} record so {@code keyBy(dataFilePath)} can route it.
   */
  private static final String ABORT_FILE_PATH = "";

  public static final DVPosition ABORT =
      new DVPosition(ABORT_FILE_PATH, -1, -1, StructLikeSerializer.EMPTY_PARTITION, -1);

  public boolean isAbort() {
    return ABORT_FILE_PATH.equals(dataFilePath);
  }
}
