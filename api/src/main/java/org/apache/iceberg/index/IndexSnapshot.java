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
package org.apache.iceberg.index;

import java.nio.ByteBuffer;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * An immutable version of the index data corresponding to a specific source table snapshot.
 *
 * <p>Each index snapshot is tied to one table snapshot via {@link #sourceTableSnapshotId()}. The
 * tracking file referenced by {@link #trackingFile()} lists all leaf files belonging to this
 * snapshot.
 */
public interface IndexSnapshot {

  /** Unique identifier for this index snapshot. */
  long snapshotId();

  /** The source table snapshot this index snapshot was built from. */
  long sourceTableSnapshotId();

  /** Timestamp in milliseconds when this index snapshot was created. */
  long timestampMs();

  /**
   * Location of the tracking file for this index snapshot.
   *
   * <p>The tracking file is a Parquet file that lists all leaf files belonging to this snapshot,
   * with transform-value min/max bounds per leaf file for planning-time pruning.
   */
  String trackingFile();

  /**
   * Optional snapshot-level properties, supplied by the index maintenance process.
   *
   * @return an unmodifiable map of string properties, never null
   */
  Map<String, String> properties();

  /**
   * Optional implementation-specific key metadata for tracking file encryption.
   *
   * @return key metadata bytes, or null if not set
   */
  @Nullable
  ByteBuffer keyMetadata();
}
