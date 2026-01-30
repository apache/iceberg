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
import org.apache.iceberg.Table;

/**
 * {@link Index} snapshot linking an index snapshot to a specific {@link Table} snapshot.
 *
 * <p>Index data is versioned using snapshots, in a manner similar to table data. Each index
 * snapshot is derived from a specific table snapshot, ensuring consistency between the index and
 * the table state.
 *
 * <p>When an engine queries a particular table snapshot, it must determine which index snapshots
 * are available for that snapshot. If a corresponding index snapshot is not available, the engine
 * may choose to use a different index snapshot, provided that the semantics of the given index type
 * allow it. When a specific index snapshot is selected, the snapshot parameters and user‑provided
 * parameters stored with the referenced index version are used when evaluating the index.
 *
 * <p>This relationship is tracked in the index metadata file via an index–snapshot mapping. The
 * mapping is updated whenever an index maintenance process creates a new index snapshot and
 * associates it with a base table snapshot using {@link AddIndexSnapshot}, or when index snapshots
 * are expired as part of index maintenance and removed through {@link RemoveIndexSnapshots}.
 */
public interface IndexSnapshot {

  /** Return the table snapshot ID which is the base of the index snapshot. */
  long tableSnapshotId();

  /** Return the index snapshot ID. */
  long indexSnapshotId();

  /** Return the index version ID when the snapshot was created. */
  int versionId();

  /**
   * Return the properties for this snapshot.
   *
   * <p>A map of index snapshot properties, represented as string-to-string pairs, supplied by the
   * Index Maintenance process.
   *
   * @return an unmodifiable map of string properties, or empty if none set
   */
  Map<String, String> properties();
}
