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
import java.util.Map;
import org.apache.flink.annotation.Experimental;

/**
 * Callback invoked after each successful Iceberg snapshot commit.
 *
 * <p>Implementations can react to committed snapshots for use cases such as:
 *
 * <ul>
 *   <li>Audit logging of committed snapshots
 *   <li>Synchronizing metadata to external catalogs
 *   <li>Triggering compaction or maintenance workflows
 *   <li>Tracking custom metadata across commits, e.g. watermark
 * </ul>
 *
 * <p><b>Why PostCommitHook instead of {@code Listeners}/{@code CreateSnapshotEvent}?</b>
 *
 * <p>Iceberg provides a static {@code Listeners} registry that fires {@code CreateSnapshotEvent}
 * after every snapshot commit. However, that mechanism has limitations in a Flink environment:
 *
 * <ul>
 *   <li>{@code Listeners} is JVM-global with no {@code unregister()} method. A single Flink
 *       TaskManager JVM can host multiple sink instances for different tables, making it impossible
 *       to scope a listener to a specific sink or clean it up on job restart.
 *   <li>{@code SnapshotProducer} silently swallows listener exceptions ({@code LOG.warn}), so the
 *       caller has no control over error propagation.
 *   <li>{@code CreateSnapshotEvent} provides only a table name string with no handle to the {@code
 *       Table}, {@code TableLoader}, or catalog, preventing follow-up operations without
 *       independently bootstrapping catalog access.
 * </ul>
 *
 * <p>{@code PostCommitHook} addresses all three: it is per-sink-instance scoped, lets the
 * implementer control error propagation, and runs inside {@code IcebergCommitter} where {@code
 * TableLoader} and the Flink checkpoint lifecycle are available.
 *
 * <p><b>Error semantics</b>
 *
 * <p>The hook is called after the Iceberg commit succeeds but before the Flink checkpoint
 * completes. If the hook throws an exception, the Flink checkpoint fails but the Iceberg commit has
 * already succeeded. On recovery, the committed checkpoint is detected via {@code
 * MAX_COMMITTED_CHECKPOINT_ID} in the snapshot summary and will not be re-committed. The hook will
 * <b>not</b> be re-invoked for that snapshot.
 */
@Experimental
@FunctionalInterface
public interface PostCommitHook extends Serializable {
  void afterCommit(long snapshotId, Map<String, String> snapshotSummary);
}
