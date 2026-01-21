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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class IndexSnapshotsRemove implements RemoveIndexSnapshots {
  private final IndexOperations ops;
  private final Set<Long> snapshotIdsToRemove = Sets.newHashSet();
  private IndexMetadata base;

  IndexSnapshotsRemove(IndexOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public List<IndexSnapshot> apply() {
    List<IndexSnapshot> snapshotsToRemove = Lists.newArrayList();
    for (Long snapshotId : snapshotIdsToRemove) {
      IndexSnapshot snapshot = base.snapshot(snapshotId);
      if (snapshot != null) {
        snapshotsToRemove.add(snapshot);
      }
    }

    return snapshotsToRemove;
  }

  private IndexMetadata internalApply() {
    this.base = ops.refresh();

    return IndexMetadata.buildFrom(base).removeSnapshots(snapshotIdsToRemove).build();
  }

  @Override
  public void commit() {
    Map<String, String> properties =
        base.currentVersion().properties() != null
            ? base.currentVersion().properties()
            : Maps.newHashMap();
    Tasks.foreach(ops)
        .retry(
            PropertyUtil.propertyAsInt(properties, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(
                properties, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                properties, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                properties, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> taskOps.commit(base, internalApply()));
  }

  @Override
  public RemoveIndexSnapshots removeSnapshotById(long indexSnapshotId) {
    snapshotIdsToRemove.add(indexSnapshotId);
    return this;
  }

  @Override
  public RemoveIndexSnapshots removeSnapshotsByIds(Set<Long> indexSnapshotIds) {
    snapshotIdsToRemove.addAll(indexSnapshotIds);
    return this;
  }

  @Override
  public RemoveIndexSnapshots removeSnapshotsByIds(long... indexSnapshotIds) {
    for (long id : indexSnapshotIds) {
      snapshotIdsToRemove.add(id);
    }

    return this;
  }
}
