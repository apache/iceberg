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

import java.util.Map;

/**
 * Index snapshot linking an index snapshot to a specific table snapshot.
 *
 * <p>Index data is versioned using snapshots, similar to table data. Each index snapshot is derived
 * from a specific table snapshot, ensuring consistency. When an engine queries a table snapshot, it
 * must determine whether a corresponding index snapshot exists and, if so, determine which
 * properties should be applied for index evaluation.
 *
 * <p>This relationship is maintained in the index's metadata file through an index-snapshot list.
 * This list is updated whenever an index maintenance process creates a new snapshot for the index
 * and links it to the corresponding base table snapshot, or when the index maintenance process
 * decides to expire index snapshots.
 */
public interface IndexSnapshot {

  /**
   * Return the table snapshot ID which is the base of the index snapshot.
   *
   * @return the table snapshot ID
   */
  long tableSnapshotId();

  /**
   * Return the index snapshot ID.
   *
   * @return the index snapshot ID
   */
  long indexSnapshotId();

  /**
   * Return the index version ID when the snapshot was created.
   *
   * @return the version ID
   */
  int versionId();

  /**
   * Return the properties for this snapshot.
   *
   * <p>A map of index properties, represented as string-to-string pairs, supplied by the Index
   * Maintenance process.
   *
   * @return a map of string properties, or null if not set
   */
  Map<String, String> properties();
}
