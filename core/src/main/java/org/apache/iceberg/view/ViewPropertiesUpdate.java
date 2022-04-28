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

package org.apache.iceberg.view;

import java.util.Map;
import java.util.Set;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class ViewPropertiesUpdate implements ViewUpdateProperties {
  private final ViewOperations ops;
  private final Map<String, String> updates = Maps.newHashMap();
  private final Set<String> removals = Sets.newHashSet();
  private ViewMetadata base;

  ViewPropertiesUpdate(ViewOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ViewUpdateProperties set(String key, String value) {
    Preconditions.checkNotNull(key, "Key cannot be null");
    Preconditions.checkNotNull(key, "Value cannot be null");
    Preconditions.checkArgument(!removals.contains(key),
        "Cannot remove and update the same key: %s", key);

    updates.put(key, value);

    return this;
  }

  @Override
  public ViewUpdateProperties remove(String key) {
    Preconditions.checkNotNull(key, "Key cannot be null");
    Preconditions.checkArgument(!updates.containsKey(key),
        "Cannot remove and update the same key: %s", key);

    removals.add(key);

    return this;
  }

  @Override
  public Map<String, String> apply() {
    this.base = ops.refresh();

    Map<String, String> newProperties = Maps.newHashMap();
    for (Map.Entry<String, String> entry : base.properties().entrySet()) {
      if (!removals.contains(entry.getKey())) {
        newProperties.put(entry.getKey(), entry.getValue());
      }
    }

    newProperties.putAll(updates);

    return newProperties;
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(
            PropertyUtil.propertyAsInt(base.properties(),
                ViewProperties.COMMIT_NUM_RETRIES,
                ViewProperties.COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(base.properties(),
                ViewProperties.COMMIT_MIN_RETRY_WAIT_MS,
                ViewProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(base.properties(),
                ViewProperties.COMMIT_MAX_RETRY_WAIT_MS,
                ViewProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(base.properties(),
                ViewProperties.COMMIT_TOTAL_RETRY_TIME_MS,
                ViewProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(taskOps -> {
          Map<String, String> newProperties = apply();
          ViewMetadata updated = base.replaceProperties(newProperties);
          taskOps.commit(base, updated, Maps.newHashMap());
        });
  }
}
