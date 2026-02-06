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
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class IndexVersionAdd implements AddIndexVersion {
  private final IndexOperations ops;
  private final Map<String, String> properties = Maps.newHashMap();
  private IndexMetadata base;

  IndexVersionAdd(IndexOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public IndexVersion apply() {
    return internalApply().currentVersion();
  }

  @VisibleForTesting
  IndexMetadata internalApply() {
    this.base = ops.refresh();

    return IndexMetadata.buildFrom(base)
        .addVersion(
            ImmutableIndexVersion.builder()
                .timestampMillis(System.currentTimeMillis())
                .versionId(base.currentVersionId())
                .properties(properties)
                .build())
        .setCurrentVersion(base.currentVersionId())
        .build();
  }

  @Override
  public void commit() {
    Map<String, String> currentProperties =
        base.currentVersion().properties() != null
            ? base.currentVersion().properties()
            : Maps.newHashMap();
    Tasks.foreach(ops)
        .retry(
            PropertyUtil.propertyAsInt(
                currentProperties, COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(
                currentProperties, COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                currentProperties, COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                currentProperties, COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> taskOps.commit(base, internalApply()));
  }

  @Override
  public AddIndexVersion withProperties(Map<String, String> newProperties) {
    newProperties.forEach(this::withProperty);
    return this;
  }

  @Override
  public AddIndexVersion withProperty(String key, String value) {
    Preconditions.checkArgument(null != key, "Invalid key: null");
    Preconditions.checkArgument(null != value, "Invalid value: null");

    properties.put(key, value);
    return this;
  }
}
