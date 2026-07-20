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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Metadata for a secondary index on an Iceberg table.
 *
 * <p>Index metadata is stored as a JSON file in the index's {@link #location()}. It contains the
 * index definition (type, transform, key columns) and a list of {@link IndexSnapshot index
 * snapshots}, each corresponding to a specific source table snapshot.
 *
 * <p>This interface matches the index metadata format defined in the Iceberg secondary index
 * specification (format/index.md).
 */
public interface IndexMetadata {

  int SUPPORTED_INDEX_FORMAT_VERSION = 1;
  int DEFAULT_INDEX_FORMAT_VERSION = 1;

  /** Index metadata format version. Currently 1. */
  int formatVersion();

  /** Stable UUID assigned when the index is created. */
  String uuid();

  /** UUID of the source table this index is built on. */
  String tableUuid();

  /** Base location used to create index file paths (metadata, tracking, leaf files). */
  String location();

  /**
   * Logical index type, e.g. {@code "SCALAR"}, {@code "IVF"}, {@code "TERM"}.
   *
   * <p>Engines that do not recognize the type must skip this index and fall back to normal
   * planning.
   */
  String type();

  /**
   * Transform function applied to key columns, e.g. {@code "HASH"} or {@code "IDENTITY"}.
   *
   * <p>Determines how index entries are organized within leaf files.
   */
  String transformFunction();

  /**
   * Source-table column IDs that the transform is applied to (key columns).
   *
   * <p>Leaf files are organized by the transform value derived from these columns.
   */
  List<Integer> keyColumnIds();

  /**
   * Optional source-table column IDs copied into the index for read convenience (included
   * columns).
   *
   * <p>These columns are stored in leaf files but do not affect index organization. Used to serve
   * covering queries without reading the source table.
   */
  List<Integer> includedColumnIds();

  /**
   * Optional index-level properties, e.g. {@code hash.num-buckets}, {@code
   * ivf.distance-function}.
   *
   * @return an unmodifiable map of string properties, never null
   */
  Map<String, String> properties();

  /**
   * ID of the current index snapshot, or null if no snapshot has been committed yet.
   *
   * @return the current snapshot ID, or null
   */
  @Nullable
  Long currentSnapshotId();

  /** All index snapshots for this index, ordered by snapshot ID. */
  List<IndexSnapshot> snapshots();

  /**
   * Location of the index metadata file, or null if not persisted yet.
   *
   * @return the metadata file location, or null
   */
  @Nullable
  String metadataFileLocation();

  // ---------------------------------------------------------------------------
  // Derived helpers
  // ---------------------------------------------------------------------------

  /** Returns the current {@link IndexSnapshot}, or null if no snapshot has been committed. */
  default IndexSnapshot currentSnapshot() {
    Long id = currentSnapshotId();
    if (id == null) {
      return null;
    }
    for (IndexSnapshot snap : snapshots()) {
      if (snap.snapshotId() == id) {
        return snap;
      }
    }
    return null;
  }

  /**
   * Returns the {@link IndexSnapshot} whose {@link IndexSnapshot#sourceTableSnapshotId()} matches
   * the given table snapshot ID, or null if none exists.
   */
  default IndexSnapshot snapshotForTableSnapshot(long tableSnapshotId) {
    for (IndexSnapshot snap : snapshots()) {
      if (snap.sourceTableSnapshotId() == tableSnapshotId) {
        return snap;
      }
    }
    return null;
  }
}
