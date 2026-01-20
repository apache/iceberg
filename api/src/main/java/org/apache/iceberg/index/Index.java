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
import java.util.UUID;

/**
 * Interface for index definition.
 *
 * <p>An index is a specialized data structure that improves the speed of data retrieval operations
 * on a database table. The index can be computed either synchronously and committed along the
 * DDL/DML changes or asynchronously and updated by an index maintenance process.
 */
public interface Index {

  /**
   * Return the name of this index.
   *
   * @return the index name
   */
  String name();

  /**
   * Return the UUID that identifies this index.
   *
   * <p>Generated when the index is created. Implementations must throw an exception if an index's
   * UUID does not match the expected UUID after refreshing metadata.
   *
   * @return the index UUID
   */
  UUID uuid();

  /**
   * Return the format version for this index.
   *
   * <p>An integer version number for the index metadata format.
   *
   * @return the format version
   */
  int formatVersion();

  /**
   * Return the type of this index.
   *
   * <p>One of the supported index-types. Must be supplied during the creation of an index and must
   * not be changed.
   *
   * @return the index type
   */
  IndexType type();

  /**
   * Return the column IDs contained by this index.
   *
   * <p>The ids of the columns contained by the index.
   *
   * @return a list of column IDs
   */
  List<Integer> indexColumnIds();

  /**
   * Return the column IDs that this index is optimized for.
   *
   * <p>The ids of the columns that the index is designed to optimize for retrieval.
   *
   * @return a list of column IDs
   */
  List<Integer> optimizedColumnIds();

  /**
   * Return the index's base location.
   *
   * <p>Used to create index file locations.
   *
   * @return the index location
   */
  String location();

  /**
   * Return the ID of the current version of this index.
   *
   * @return the current version ID
   */
  int currentVersionId();

  /**
   * Get the current version for this index, or null if there are no versions.
   *
   * @return the current index version
   */
  IndexVersion currentVersion();

  /**
   * Get the versions of this index.
   *
   * <p>A list of known versions of the index, the number of versions retained is
   * implementation-specific. current-version-id must be present in this list.
   *
   * @return an Iterable of versions of this index
   */
  Iterable<IndexVersion> versions();

  /**
   * Get a version in this index by ID.
   *
   * @param versionId version ID
   * @return a version, or null if the ID cannot be found
   */
  IndexVersion version(int versionId);

  /**
   * Get the version history of this index.
   *
   * <p>A list of version log entries with the timestamp and version-id for every change to
   * current-version-id. The number of entries retained is implementation-specific.
   * current-version-id may or may not be present in this list.
   *
   * @return a list of {@link IndexHistoryEntry}
   */
  List<IndexHistoryEntry> history();

  /**
   * Get the snapshots of this index.
   *
   * <p>During index maintenance a new index snapshot is generated for the specific Table snapshot,
   * and it is added to the snapshots list.
   *
   * @return a list of {@link IndexSnapshot}
   */
  List<IndexSnapshot> snapshots();

  /**
   * Get a snapshot in this index by index snapshot ID.
   *
   * @param indexSnapshotId index snapshot ID
   * @return a snapshot, or null if the ID cannot be found
   */
  IndexSnapshot snapshot(long indexSnapshotId);

  /**
   * Get a snapshot in this index by table snapshot ID.
   *
   * @param tableSnapshotId table snapshot ID
   * @return a snapshot, or null if no index snapshot exists for the table snapshot
   */
  IndexSnapshot snapshotForTableSnapshot(long tableSnapshotId);
}
