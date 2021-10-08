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

package org.apache.iceberg.util;

import java.util.Date;
import java.util.Map;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotUtil.class);

  private ExpireSnapshotUtil() {

  }

  /**
   * Returns whether expire snapshot files should be removed.
   */
  public static boolean shouldExpireSnapshot(Table table) {
    boolean shouldExpire = false;
    long currentTimeMillis = System.currentTimeMillis();
    Map<String, String> props = table.properties();
    boolean autoExpireSnapshotEnable = PropertyUtil.propertyAsBoolean(
        props,
        TableProperties.SNAPSHOT_FLINK_AUTO_EXPIRE_ENABLED,
        TableProperties.SNAPSHOT_FLINK_AUTO_EXPIRE_ENABLED_DEFAULT);

    long expireIntervalMillis = PropertyUtil.propertyAsLong(
        props,
        TableProperties.SNAPSHOT_FLINK_AUTO_EXPIRE_INTERVAL_MS,
        TableProperties.SNAPSHOT_FLINK_AUTO_EXPIRE_INTERVAL_MS_DEFAULT);

    long lastExpireTimestamp = PropertyUtil.propertyAsLong(
        props,
        TableProperties.FLINK_LAST_EXPIRE_SNAPSHOT_MS,
        TableProperties.FLINK_LAST_EXPIRE_SNAPSHOT_MS_DEFAULT);

    LOG.info("Summary: actual expire interval: {}s, expire.auto.enabled: {}, " +
            "expire.interval: {}s, current time: {}, last expire time: {}",
        (currentTimeMillis - lastExpireTimestamp) / 1000,
        autoExpireSnapshotEnable,
        expireIntervalMillis / 1000,
        new Date(currentTimeMillis),
        new Date(lastExpireTimestamp));

    if (autoExpireSnapshotEnable &&
        currentTimeMillis - lastExpireTimestamp >= expireIntervalMillis) {
      LOG.info("Should expire snapshot");
      shouldExpire = true;
    }
    return shouldExpire;
  }
}
