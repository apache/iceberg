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
import org.apache.iceberg.util.PropertyUtil;

public class RewriteConfig {
  private static final String CONFIG_PREFIX = "flink-maintenance.rewrite.";

  private final Map<String, String> properties;

  public RewriteConfig(Map<String, String> properties) {
    this.properties = properties;
  }

  public static final String DATA_FILE_COUNT = CONFIG_PREFIX + "data-file-count";

  public static final String PARTIAL_PROGRESS_ENABLE = CONFIG_PREFIX + "partial-progress-enabled";

  public static final String PARTIAL_PROGRESS_MAX_COMMITS =
      CONFIG_PREFIX + "partial-progress-max-commits";

  public static final String MAX_REWRITE_BYTES = CONFIG_PREFIX + "max-bytes";

  public static final String TARGET_FILE_SIZE_BYTES = CONFIG_PREFIX + "target-file-size-bytes";

  public static final String MIN_FILE_SIZE_BYTES = CONFIG_PREFIX + "min-file-size-bytes";

  public static final String MAX_FILE_SIZE_BYTES = CONFIG_PREFIX + "max-file-size-bytes";

  public static final String MIN_INPUT_FILES = CONFIG_PREFIX + "min-input-files";

  public static final String DELETE_FILE_THRESHOLD = CONFIG_PREFIX + "delete-file-threshold";

  public static final String REWRITE_ALL = CONFIG_PREFIX + "rewrite-all";

  public Integer getDataFileCount() {
    return PropertyUtil.propertyAsNullableInt(properties, DATA_FILE_COUNT);
  }

  public Boolean getPartialProgressEnable() {
    return PropertyUtil.propertyAsNullableBoolean(properties, PARTIAL_PROGRESS_ENABLE);
  }

  public Integer getPartialProgressMaxCommits() {
    return PropertyUtil.propertyAsNullableInt(properties, PARTIAL_PROGRESS_MAX_COMMITS);
  }

  public Long getMaxRewriteBytes() {
    return PropertyUtil.propertyAsNullableLong(properties, MAX_REWRITE_BYTES);
  }

  public Long getTargetFileSizeBytes() {
    return PropertyUtil.propertyAsNullableLong(properties, TARGET_FILE_SIZE_BYTES);
  }

  public Long getMinFileSizeBytes() {
    return PropertyUtil.propertyAsNullableLong(properties, MIN_FILE_SIZE_BYTES);
  }

  public Long getMaxFileSizeBytes() {
    return PropertyUtil.propertyAsNullableLong(properties, MAX_FILE_SIZE_BYTES);
  }

  public Integer getMinInputFiles() {
    return PropertyUtil.propertyAsNullableInt(properties, MIN_INPUT_FILES);
  }

  public Integer getDeleteFileThreshold() {
    return PropertyUtil.propertyAsNullableInt(properties, DELETE_FILE_THRESHOLD);
  }

  public Boolean getRewriteAll() {
    return PropertyUtil.propertyAsNullableBoolean(properties, REWRITE_ALL);
  }
}
