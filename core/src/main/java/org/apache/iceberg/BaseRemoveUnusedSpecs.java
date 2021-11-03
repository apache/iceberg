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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.ParallelIterable;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;

/**
 * Implementation of RemoveUnusedSpecs API to remove unused partition specs.
 *
 * <p>When committing, these changes will be applied to the latest table metadata. Commit conflicts
 * will be resolved by recalculating which specs are no longer in use again in the latest metadata
 * and retrying.
 */
class BaseRemoveUnusedSpecs implements RemoveUnusedSpecs {
  private final TableOperations ops;

  private TableMetadata base;

  BaseRemoveUnusedSpecs(TableOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public List<PartitionSpec> apply() {
    TableMetadata newMetadata = internalApply();
    return newMetadata.specs();
  }

  @Override
  public void commit() {
    this.base = ops.refresh();
    Tasks.foreach(ops)
        .retry(base.propertyAsInt(COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            base.propertyAsInt(COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            base.propertyAsInt(COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              TableMetadata newMetadata = internalApply();
              taskOps.commit(base, newMetadata);
            });
  }

  private Iterable<ManifestFile> reachableManifests() {
    Iterable<Snapshot> snapshots = base.snapshots();
    Iterable<Iterable<ManifestFile>> manifestIterables =
        Iterables.transform(snapshots, snapshot -> snapshot.allManifests(ops.io()));
    try (CloseableIterable<ManifestFile> iterable =
        new ParallelIterable<>(manifestIterables, ThreadPools.getWorkerPool())) {
      return Sets.newHashSet(iterable);
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close parallel iterable", e);
    }
  }

  private TableMetadata internalApply() {
    this.base = ops.refresh();
    List<PartitionSpec> specs = base.specs();
    int currentSpecId = base.defaultSpecId();

    Set<Integer> specsInUse =
        Sets.newHashSet(Iterables.transform(reachableManifests(), ManifestFile::partitionSpecId));

    specsInUse.add(currentSpecId);

    List<PartitionSpec> specsToRemove =
        specs.stream()
            .filter(spec -> !specsInUse.contains(spec.specId()))
            .collect(Collectors.toList());

    return TableMetadata.buildFrom(base).removeUnusedSpecs(specsToRemove).build();
  }
}
