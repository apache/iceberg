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
package org.apache.iceberg.spark.actions;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManageSnapshots;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.DropPartitionFromRefs;
import org.apache.iceberg.actions.ImmutableDropPartitionFromRefs;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ManifestEvaluator;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Spark implementation of {@link DropPartitionFromRefs} that uses {@code DeleteFiles} scoped to a
 * temporary branch and deduplicates work across refs sharing the same snapshot.
 */
public class DropPartitionFromRefsSparkAction
    extends BaseSparkAction<DropPartitionFromRefsSparkAction> implements DropPartitionFromRefs {

  private static final Logger LOG = LoggerFactory.getLogger(DropPartitionFromRefsSparkAction.class);

  private final Table table;

  private Expression filter = null;
  private RefType refType = RefType.TAGS;
  private Set<String> targetRefs = null;
  private boolean dryRun = false;

  DropPartitionFromRefsSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected DropPartitionFromRefsSparkAction self() {
    return this;
  }

  @Override
  public DropPartitionFromRefsSparkAction filter(Expression expr) {
    this.filter = expr;
    return this;
  }

  @Override
  public DropPartitionFromRefsSparkAction refType(RefType type) {
    this.refType = type;
    return this;
  }

  @Override
  public DropPartitionFromRefsSparkAction refs(Collection<String> refNames) {
    this.targetRefs = Set.copyOf(refNames);
    return this;
  }

  @Override
  public DropPartitionFromRefsSparkAction dryRun(boolean enabled) {
    this.dryRun = enabled;
    return this;
  }

  @Override
  public DropPartitionFromRefs.Result execute() {
    Preconditions.checkNotNull(
        filter, "filter() must be called with a partition expression before execute()");

    table.refresh();

    Map<String, SnapshotRef> matchingRefs = collectMatchingRefs();
    if (matchingRefs.isEmpty()) {
      LOG.info("No refs matched refType={} — nothing to do", refType);
      return emptyResult();
    }

    // group refs by snapshot ID so that refs sharing the same snapshot are processed together:
    // the delete runs once per unique snapshot, not once per ref
    Map<Long, List<String>> refsBySnapshot =
        matchingRefs.entrySet().stream()
            .collect(
                Collectors.groupingBy(
                    e -> e.getValue().snapshotId(),
                    Collectors.mapping(Map.Entry::getKey, Collectors.toList())));

    LOG.info(
        "Processing {} ref(s) across {} unique snapshot(s)",
        matchingRefs.size(),
        refsBySnapshot.size());

    Map<String, Long> updatedRefs = new LinkedHashMap<>();
    List<DataFile> removedFiles = Lists.newArrayList();
    long rewrittenManifestCount = 0;

    for (Map.Entry<Long, List<String>> entry : refsBySnapshot.entrySet()) {
      long snapshotId = entry.getKey();
      List<String> refsForSnapshot = entry.getValue();

      if (dryRun) {
        long estimatedFiles = estimateMatchingFileCount(snapshotId);
        LOG.info(
            "DRY RUN: snapshot {} (refs: {}) — ~{} file(s) would be removed",
            snapshotId,
            refsForSnapshot,
            estimatedFiles);
        refsForSnapshot.forEach(ref -> updatedRefs.put(ref, -1L));
        continue;
      }

      String tmpBranch = "tmp_drop_partition_" + snapshotId;
      LOG.info(
          "Deleting partition from snapshot {} via branch '{}' (refs: {})",
          snapshotId,
          tmpBranch,
          refsForSnapshot);

      try {
        table.manageSnapshots().createBranch(tmpBranch, snapshotId).commit();

        table.newDelete().toBranch(tmpBranch).deleteFromRowFilter(filter).commit();

        table.refresh();
        long newSnapshotId = table.snapshot(tmpBranch).snapshotId();

        rewrittenManifestCount += countDroppedManifests(snapshotId, newSnapshotId);

        ManageSnapshots manage = table.manageSnapshots();
        for (String ref : refsForSnapshot) {
          SnapshotRef original = matchingRefs.get(ref);
          if (original.isTag()) {
            manage = manage.replaceTag(ref, newSnapshotId);
          } else {
            // branch: fast-forward is safe because newSnapshotId descends from snapshotId
            manage = manage.fastForwardBranch(ref, tmpBranch);
          }
          updatedRefs.put(ref, newSnapshotId);
        }
        manage.removeBranch(tmpBranch).commit();

      } catch (Exception e) {
        // best-effort cleanup of the temp branch so it does not linger
        try {
          table.manageSnapshots().removeBranch(tmpBranch).commit();
        } catch (Exception cleanupEx) {
          LOG.warn(
              "Failed to clean up temporary branch '{}': {}", tmpBranch, cleanupEx.getMessage());
        }
        throw e;
      }
    }

    return ImmutableDropPartitionFromRefs.Result.builder()
        .updatedRefs(updatedRefs)
        .removedFiles(removedFiles)
        .rewrittenManifestCount(rewrittenManifestCount)
        .processedSnapshotCount(refsBySnapshot.size())
        .build();
  }

  private Map<String, SnapshotRef> collectMatchingRefs() {
    return table.refs().entrySet().stream()
        .filter(
            e -> {
              String name = e.getKey();
              SnapshotRef ref = e.getValue();

              // main is never touched regardless of refType
              if (SnapshotRef.MAIN_BRANCH.equals(name)) {
                return false;
              }
              if (targetRefs != null && !targetRefs.contains(name)) {
                return false;
              }
              return switch (refType) {
                case TAGS -> ref.isTag();
                case BRANCHES -> ref.isBranch();
                case ALL -> true;
              };
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // use manifest partition-field summaries to skip reading manifests that provably have no overlap
  private long estimateMatchingFileCount(long snapshotId) {
    return table.snapshot(snapshotId).dataManifests(table.io()).stream()
        .filter(this::manifestOverlaps)
        .mapToLong(ManifestFile::existingFilesCount)
        .sum();
  }

  private boolean manifestOverlaps(ManifestFile manifest) {
    if (manifest.partitions() == null || manifest.partitions().isEmpty()) {
      return true;
    }
    ManifestEvaluator evaluator =
        ManifestEvaluator.forRowFilter(filter, table.specs().get(manifest.partitionSpecId()), true);
    return evaluator.eval(manifest);
  }

  private long countDroppedManifests(long beforeSnapshotId, long afterSnapshotId) {
    Set<String> afterPaths =
        table.snapshot(afterSnapshotId).dataManifests(table.io()).stream()
            .map(ManifestFile::path)
            .collect(Collectors.toSet());
    return table.snapshot(beforeSnapshotId).dataManifests(table.io()).stream()
        .filter(m -> !afterPaths.contains(m.path()))
        .count();
  }

  private DropPartitionFromRefs.Result emptyResult() {
    return ImmutableDropPartitionFromRefs.Result.builder()
        .updatedRefs(ImmutableMap.of())
        .removedFiles(ImmutableList.of())
        .rewrittenManifestCount(0)
        .processedSnapshotCount(0)
        .build();
  }
}
