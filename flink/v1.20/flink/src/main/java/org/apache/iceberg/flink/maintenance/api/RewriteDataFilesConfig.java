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
import org.apache.curator.shaded.com.google.common.collect.Maps;
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

  public static final String SCHEDULE_ON_COMMIT_COUNT = "schedule-on-commit-count";

  public static final String SCHEDULE_ON_DATA_FILE_COUNT = "schedule-on-data-file-count";

  public static final String SCHEDULE_ON_DATA_FILE_SIZE = "schedule-on-data-file-size";

  public static final String SCHEDULE_ON_INTERVAL_SECOND = "schedule-on-interval-second";

  public Integer getScheduleOnCommitCount() {
    return PropertyUtil.propertyAsNullableInt(properties, SCHEDULE_ON_COMMIT_COUNT);
  }

  public Integer getScheduleOnDataFileCount() {
    return PropertyUtil.propertyAsNullableInt(properties, SCHEDULE_ON_DATA_FILE_COUNT);
  }

  public Integer getScheduleOnDataFileSize() {
    return PropertyUtil.propertyAsNullableInt(properties, SCHEDULE_ON_DATA_FILE_SIZE);
  }

  public Integer getScheduleOnIntervalSecond() {
    return PropertyUtil.propertyAsNullableInt(properties, SCHEDULE_ON_INTERVAL_SECOND);
  }

  public Boolean getPartialProgressEnable() {
    return PropertyUtil.propertyAsNullableBoolean(properties, PARTIAL_PROGRESS_ENABLE);
  }

  public Integer getPartialProgressMaxCommits() {
    return PropertyUtil.propertyAsNullableInt(properties, PARTIAL_PROGRESS_MAX_COMMITS);
  }

  public Long getMaxRewriteBytes() {
    return PropertyUtil.propertyAsNullableLong(properties, MAX_BYTES);
  }

  public Map<String, String> getProperties() {
    return properties;
  }
}
