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

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class IndexPropertiesUpdate implements UpdateIndexProperties {
  private final IndexOperations ops;
  private final Map<String, String> updates = Maps.newHashMap();
  private final Set<String> removals = Sets.newHashSet();
  private IndexMetadata base;

  IndexPropertiesUpdate(IndexOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public Map<String, String> apply() {
    return internalApply().currentVersion().properties();
  }

  @VisibleForTesting
  IndexMetadata internalApply() {
    this.base = ops.refresh();

    Map<String, String> newProperties = Maps.newHashMap(base.currentVersion().properties());
    removals.forEach(newProperties::remove);
    newProperties.putAll(updates);

    return IndexMetadata.buildFrom(base)
        .setCurrentVersion(
            ImmutableIndexVersion.builder()
                .timestampMillis(System.currentTimeMillis())
                .versionId(base.currentVersionId())
                .properties(newProperties)
                .build())
        .build();
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
  public UpdateIndexProperties set(String key, String value) {
    Preconditions.checkArgument(null != key, "Invalid key: null");
    Preconditions.checkArgument(null != value, "Invalid value: null");
    Preconditions.checkArgument(
        !removals.contains(key), "Cannot remove and update the same key: %s", key);

    updates.put(key, value);
    return this;
  }

  @Override
  public UpdateIndexProperties remove(String key) {
    Preconditions.checkArgument(null != key, "Invalid key: null");
    Preconditions.checkArgument(
        !updates.containsKey(key), "Cannot remove and update the same key: %s", key);

    removals.add(key);
    return this;
  }
}
