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

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotUpdate;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The record of the last checkpoint each Flink writer committed to a table branch, which the
 * committers read to skip committables they already committed.
 *
 * <p>Every commit records the writer's (branch, job id, operator id) and checkpoint id twice, in
 * one metadata commit: in the snapshot summary, and in a table property keyed by the writer. The
 * summary is found by walking the branch's ancestry, which stops at the first expired snapshot, so
 * a writer that stays idle while others commit loses it once snapshot expiry removes the chain. The
 * table property survives expiry and other writers' commits. Readers take the larger of the two, so
 * tables last written before the property existed, or by an older committer of the same job, still
 * resolve through the walk.
 *
 * <p>A value found only through the walk can be kept with {@link #recordCommittedCheckpoint}. It
 * goes into a separate property whose key includes the checkpoint, so recording never overwrites a
 * value: a late or repeated request can only add a checkpoint the writer did commit, on any
 * catalog.
 *
 * <p>Writers are identified by job id, so a job id must not be reused by a run that starts without
 * state: such a run would find the earlier run's markers and skip its own commits (apache/iceberg
 * #18098).
 *
 * <p>Committers never remove markers. {@link #removeMarkers} retires them.
 */
@Internal
public final class FlinkCommitMarkers {
  static final long INITIAL_CHECKPOINT_ID = -1L;

  private static final Logger LOG = LoggerFactory.getLogger(FlinkCommitMarkers.class);
  private static final String PROPERTY_PREFIX = SinkUtil.MAX_COMMITTED_CHECKPOINT_ID + ".";
  private static final String CHECKPOINT_ID = "checkpoint-id";
  private static final String UPDATED_AT_MS = "updated-at-ms";

  private FlinkCommitMarkers() {}

  /**
   * One marker of a writer. A writer has the marker its commits write, plus one for each checkpoint
   * recorded from the summaries. Its last committed checkpoint is the largest.
   */
  public record CommitMarker(
      String branch, String jobId, String operatorId, long checkpointId, long updatedAtMillis) {}

  private record MarkerValue(long checkpointId, long updatedAtMillis) {}

  /**
   * Returns the last checkpoint the writer committed to the branch, or -1 if none is recorded: the
   * larger of its table properties and its summaries in the branch's ancestry.
   */
  public static long maxCommittedCheckpointId(
      Table table, String branch, String jobId, String operatorId) {
    return maxCommittedCheckpointId(
        table.properties(), ancestors(table, branch), branch, jobId, operatorId);
  }

  /**
   * Same as {@link #maxCommittedCheckpointId(Table, String, String, String)}, from the given
   * properties and ancestry of the branch. Commit validators use it, since they see metadata that
   * isn't published yet.
   */
  public static long maxCommittedCheckpointId(
      Map<String, String> properties,
      Iterable<Snapshot> ancestors,
      String branch,
      String jobId,
      String operatorId) {
    return Math.max(
        durableCheckpointId(properties, branch, jobId, operatorId),
        summaryCheckpointId(ancestors, branch, jobId, operatorId));
  }

  /**
   * Records in the table properties that the writer committed {@code checkpointId}, unless they
   * already record it or a later one, so that it outlives the summaries it was found in. Commits a
   * property-only metadata update when it records. Like {@link #commit}, it never records the
   * end-of-input checkpoint.
   */
  public static void recordCommittedCheckpoint(
      Table table, String branch, String jobId, String operatorId, long checkpointId) {
    if (checkpointId == IcebergStreamWriter.END_INPUT_CHECKPOINT_ID
        || checkpointId <= durableCheckpointId(table.properties(), branch, jobId, operatorId)) {
      return;
    }

    LOG.info(
        "Recording committed checkpoint {} of job {}, operator {}, branch {} in table {}",
        checkpointId,
        jobId,
        operatorId,
        branch,
        table.name());
    table
        .updateProperties()
        .set(
            recordedKey(branch, jobId, operatorId, checkpointId),
            propertyValue(checkpointId, System.currentTimeMillis()))
        .commit();
  }

  /**
   * Commits {@code operation}, which must come from {@code transaction}, to the branch together
   * with the writer's markers, in one metadata commit. Internal summary keys override any the
   * caller set.
   *
   * <p>The end-of-input checkpoint, {@link IcebergStreamWriter#END_INPUT_CHECKPOINT_ID}, goes into
   * the summary only, as before. In the properties it would outlive the walk and stop a job
   * restored from a drained savepoint from ever committing again.
   */
  public static void commit(
      Transaction transaction,
      SnapshotUpdate<?> operation,
      String branch,
      String jobId,
      String operatorId,
      long checkpointId) {
    operation.set(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID, Long.toString(checkpointId));
    operation.set(SinkUtil.FLINK_JOB_ID, jobId);
    operation.set(SinkUtil.OPERATOR_ID, operatorId);
    operation.set(SinkUtil.BRANCH, branch);
    operation.toBranch(branch);
    operation.commit();
    if (checkpointId != IcebergStreamWriter.END_INPUT_CHECKPOINT_ID) {
      transaction
          .updateProperties()
          .set(
              committedKey(branch, jobId, operatorId),
              propertyValue(checkpointId, System.currentTimeMillis()))
          .commit();
    }

    transaction.commitTransaction();
  }

  /** Lists the table's markers, skipping properties that don't parse. */
  public static List<CommitMarker> markers(Table table) {
    List<CommitMarker> markers = Lists.newArrayList();
    table
        .properties()
        .forEach(
            (key, value) -> {
              CommitMarker marker = parseOrSkip(key, value);
              if (marker != null) {
                markers.add(marker);
              }
            });
    return markers;
  }

  /**
   * Removes the markers {@code retire} selects, in one metadata commit. Retire only markers of
   * writers that no restorable savepoint or checkpoint can still hold committables for.
   *
   * <p>The selection is made again on the latest metadata whenever the publication conflicts, so a
   * marker its writer rewrote in the meantime is judged on its new value. That relies on the
   * catalog comparing the whole base metadata (Glue, Hive, JDBC, Hadoop). A REST catalog checks
   * only the requirements an update implies, which for property changes is just the table's
   * identity, so it removes the selected keys without that second look. There, a {@link
   * #recordCommittedCheckpoint} request still in flight can also add a key back for a retired
   * writer; it is harmless, and the next retirement removes it.
   */
  public static void removeMarkers(Table table, Predicate<CommitMarker> retire) {
    Preconditions.checkArgument(
        table instanceof HasTableOperations,
        "Cannot remove Flink commit markers of table %s: no table operations",
        table.name());
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata current = ops.current();
    Tasks.foreach(ops)
        .retry(current.propertyTryAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            current.propertyTryAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            current.propertyTryAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            current.propertyTryAsInt(
                COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              // UpdateProperties would replay a removal decided on older metadata.
              TableMetadata base = taskOps.refresh();
              Set<String> retired = Sets.newHashSet();
              base.properties()
                  .forEach(
                      (key, value) -> {
                        CommitMarker marker = parseOrSkip(key, value);
                        if (marker != null && retire.test(marker)) {
                          retired.add(key);
                        }
                      });
              if (!retired.isEmpty()) {
                taskOps.commit(
                    base, TableMetadata.buildFrom(base).removeProperties(retired).build());
              }
            });
  }

  @VisibleForTesting
  static long durableCheckpointId(
      Map<String, String> properties, String branch, String jobId, String operatorId) {
    String committedKey = committedKey(branch, jobId, operatorId);
    String recordedPrefix = committedKey + ".";
    long checkpointId = INITIAL_CHECKPOINT_ID;
    for (Map.Entry<String, String> property : properties.entrySet()) {
      String key = property.getKey();
      if (key.equals(committedKey)) {
        checkpointId = Math.max(checkpointId, parse(key, property.getValue()).checkpointId());
      } else if (key.startsWith(recordedPrefix)) {
        long recorded = parse(key, property.getValue()).checkpointId();
        Preconditions.checkArgument(
            key.equals(recordedKey(branch, jobId, operatorId, recorded)),
            "Invalid Flink commit marker %s=%s",
            key,
            property.getValue());
        checkpointId = Math.max(checkpointId, recorded);
      }
    }

    return checkpointId;
  }

  @VisibleForTesting
  static long summaryCheckpointId(
      Iterable<Snapshot> ancestors, String branch, String jobId, String operatorId) {
    for (Snapshot ancestor : ancestors) {
      Map<String, String> summary = ancestor.summary();
      if (jobId.equals(summary.get(SinkUtil.FLINK_JOB_ID))
          && matchesIfRecorded(summary.get(SinkUtil.OPERATOR_ID), operatorId)
          && matchesIfRecorded(summary.get(SinkUtil.BRANCH), branch)) {
        String value = summary.get(SinkUtil.MAX_COMMITTED_CHECKPOINT_ID);
        if (value != null) {
          return Long.parseLong(value);
        }
      }
    }

    return INITIAL_CHECKPOINT_ID;
  }

  /** The key a writer's commits write. */
  @VisibleForTesting
  static String committedKey(String branch, String jobId, String operatorId) {
    return PROPERTY_PREFIX + escape(operatorId) + "." + escape(jobId) + "." + escape(branch);
  }

  /** The key that records a checkpoint found in the summaries. */
  @VisibleForTesting
  static String recordedKey(String branch, String jobId, String operatorId, long checkpointId) {
    return committedKey(branch, jobId, operatorId) + "." + checkpointId;
  }

  private static Iterable<Snapshot> ancestors(Table table, String branch) {
    Snapshot head = table.snapshot(branch);
    return head != null ? SnapshotUtil.ancestorsOf(head.snapshotId(), table::snapshot) : List.of();
  }

  // Older snapshots don't record the operator or the branch, and match any.
  private static boolean matchesIfRecorded(String recorded, String expected) {
    return recorded == null || recorded.equals(expected);
  }

  /** Parses a marker, or returns null for other properties and markers that don't parse. */
  private static CommitMarker parseOrSkip(String key, String value) {
    if (!key.startsWith(PROPERTY_PREFIX)) {
      return null;
    }

    try {
      String[] parts = key.substring(PROPERTY_PREFIX.length()).split("\\.", -1);
      Preconditions.checkArgument(
          parts.length == 3 || parts.length == 4, "Expected operator.job.branch[.checkpoint]");
      String operatorId = unescape(parts[0]);
      String jobId = unescape(parts[1]);
      String branch = unescape(parts[2]);
      MarkerValue marker = parse(key, value);
      // Another spelling may decode to the same writer; only the keys written here count.
      String canonical =
          parts.length == 3
              ? committedKey(branch, jobId, operatorId)
              : recordedKey(branch, jobId, operatorId, marker.checkpointId());
      Preconditions.checkArgument(key.equals(canonical), "Non-canonical key");
      return new CommitMarker(
          branch, jobId, operatorId, marker.checkpointId(), marker.updatedAtMillis());
    } catch (RuntimeException e) {
      LOG.warn("Skipping unparseable Flink commit marker {}={}", key, value, e);
      return null;
    }
  }

  private static MarkerValue parse(String key, String value) {
    try {
      return JsonUtil.parse(
          value,
          node ->
              new MarkerValue(
                  JsonUtil.getLong(CHECKPOINT_ID, node), JsonUtil.getLong(UPDATED_AT_MS, node)));
    } catch (RuntimeException e) {
      throw new IllegalArgumentException(
          String.format("Invalid Flink commit marker %s=%s", key, value), e);
    }
  }

  private static String propertyValue(long checkpointId, long updatedAtMillis) {
    return JsonUtil.generate(
        generator -> {
          generator.writeStartObject();
          generator.writeNumberField(CHECKPOINT_ID, checkpointId);
          generator.writeNumberField(UPDATED_AT_MS, updatedAtMillis);
          generator.writeEndObject();
        },
        false);
  }

  // Ids and branch names may contain dots, which separate the key's parts.
  private static String escape(String part) {
    return part.replace("%", "%25").replace(".", "%2E");
  }

  private static String unescape(String part) {
    return part.replace("%2E", ".").replace("%25", "%");
  }
}
