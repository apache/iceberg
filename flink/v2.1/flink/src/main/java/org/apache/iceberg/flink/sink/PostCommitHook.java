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
 * Best-effort callback invoked after a successful Iceberg snapshot commit.
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
 * <p>{@code PostCommitHook} addresses the first and third limitations directly: it is
 * per-sink-instance scoped and runs inside {@code IcebergCommitter} where {@code TableLoader} and
 * the Flink checkpoint lifecycle are available. Unlike {@code Listeners}, the sink can also make
 * hook failures explicit in its own logs.
 *
 * <p><b>Invocation semantics</b>
 *
 * <p>The hook is invoked inline after a commit becomes visible on the target branch. A single Flink
 * checkpoint may produce multiple snapshots, in which case the hook may be invoked multiple times
 * in commit order. Checkpoints that do not produce a snapshot do not invoke the hook.
 *
 * <p><b>Error semantics</b>
 *
 * <p>The hook is called after the Iceberg commit succeeds but before the Flink checkpoint
 * completes. If the hook throws a {@link RuntimeException}, the sink logs the failure and ignores
 * it so the successful Iceberg commit can still complete its normal cleanup path. The hook failure
 * does not fail the Flink checkpoint and is not retried for an already-committed snapshot.
 *
 * <p>On recovery, committed checkpoints are detected via {@code MAX_COMMITTED_CHECKPOINT_ID} in the
 * snapshot summary and will not be re-committed. If the process fails after the Iceberg commit
 * succeeds but before the hook runs, the hook is not replayed for that snapshot. {@link Error}s
 * thrown by the hook are not intercepted.
 */
@Experimental
@FunctionalInterface
public interface PostCommitHook extends Serializable {
  void afterCommit(long snapshotId, Map<String, String> snapshotSummary);
}
