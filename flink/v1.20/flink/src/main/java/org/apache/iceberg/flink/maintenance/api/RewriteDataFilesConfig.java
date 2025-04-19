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
package org.apache.iceberg.flink.maintenance.api;

import java.util.Map;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;

public class RewriteDataFilesConfig {
  public static final String CONFIG_PREFIX = "flink-maintenance.rewrite.";

  private final Map<String, String> properties;

  public RewriteDataFilesConfig(Map<String, String> newProperties) {
    this.properties = Maps.newHashMap();
    newProperties.forEach(
        (key, value) -> {
          if (key.startsWith(CONFIG_PREFIX)) {
            properties.put(key.substring(CONFIG_PREFIX.length()), value);
          }
        });
  }

  public static final String PARTIAL_PROGRESS_ENABLE =
      org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_ENABLED;

  public static final String PARTIAL_PROGRESS_MAX_COMMITS =
      org.apache.iceberg.actions.RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS;

  public static final String MAX_BYTES = "max-bytes";

  public static final String SCHEDULE_ON_COMMIT_COUNT = "schedule.commit-count";
  public static final int SCHEDULE_ON_COMMIT_COUNT_DEFAULT = 10;

  public static final String SCHEDULE_ON_DATA_FILE_COUNT = "schedule.data-file-count";
  public static final int SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT = 1000;

  public static final String SCHEDULE_ON_DATA_FILE_SIZE = "schedule.data-file-size";
  public static final long SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT = 100L * 1024 * 1024 * 1024; // 100G

  public static final String SCHEDULE_ON_INTERVAL_SECOND = "schedule.interval-second";
  public static final int SCHEDULE_ON_INTERVAL_SECOND_DEFAULT = 10 * 60; // 10 minutes

  public int scheduleOnCommitCount() {
    return PropertyUtil.propertyAsInt(
        properties, SCHEDULE_ON_COMMIT_COUNT, SCHEDULE_ON_COMMIT_COUNT_DEFAULT);
  }

  public int scheduleOnDataFileCount() {
    return PropertyUtil.propertyAsInt(
        properties, SCHEDULE_ON_DATA_FILE_COUNT, SCHEDULE_ON_DATA_FILE_COUNT_DEFAULT);
  }

  public long scheduleOnDataFileSize() {
    return PropertyUtil.propertyAsLong(
        properties, SCHEDULE_ON_DATA_FILE_SIZE, SCHEDULE_ON_DATA_FILE_SIZE_DEFAULT);
  }

  public int scheduleOnIntervalSecond() {
    return PropertyUtil.propertyAsInt(
        properties, SCHEDULE_ON_INTERVAL_SECOND, SCHEDULE_ON_INTERVAL_SECOND_DEFAULT);
  }

  public boolean partialProgressEnable() {
    return PropertyUtil.propertyAsBoolean(
        properties, PARTIAL_PROGRESS_ENABLE, RewriteDataFiles.PARTIAL_PROGRESS_ENABLED_DEFAULT);
  }

  public int partialProgressMaxCommits() {
    return PropertyUtil.propertyAsInt(
        properties,
        PARTIAL_PROGRESS_MAX_COMMITS,
        RewriteDataFiles.PARTIAL_PROGRESS_MAX_COMMITS_DEFAULT);
  }

  public long maxRewriteBytes() {
    return PropertyUtil.propertyAsLong(properties, MAX_BYTES, Long.MAX_VALUE);
  }

  public Map<String, String> properties() {
    return properties;
  }
}
