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
package org.apache.iceberg.rest;

import java.util.concurrent.TimeUnit;

public final class RESTCatalogProperties {

  private RESTCatalogProperties() {}

  public static final String SNAPSHOT_LOADING_MODE = "snapshot-loading-mode";
  public static final String SNAPSHOT_LOADING_MODE_DEFAULT = SnapshotMode.ALL.name();
  public static final String SNAPSHOTS_QUERY_PARAMETER = "snapshots";

  public static final String METRICS_REPORTING_ENABLED = "rest-metrics-reporting-enabled";
  public static final boolean METRICS_REPORTING_ENABLED_DEFAULT = true;

  // for backwards compatibility with older REST servers where it can be assumed that a particular
  // server supports view endpoints but doesn't send the "endpoints" field in the ConfigResponse
  public static final String VIEW_ENDPOINTS_SUPPORTED = "view-endpoints-supported";
  public static final boolean VIEW_ENDPOINTS_SUPPORTED_DEFAULT = false;

  public static final String PAGE_SIZE = "rest-page-size";

  public static final String NAMESPACE_SEPARATOR = "namespace-separator";

  // Configure scan planning mode
  // Can be set by server in LoadTableResponse.config() for table-level override
  public static final String SCAN_PLANNING_MODE = "scan-planning-mode";
  public static final String SCAN_PLANNING_MODE_DEFAULT = ScanPlanningMode.CLIENT.modeName();

  public static final String REST_SCAN_PLAN_ID = "rest-scan-plan-id";

  // Properties that control the behaviour of the table cache used for freshness-aware table
  // loading.
  public static final String TABLE_CACHE_EXPIRE_AFTER_WRITE_MS =
      "rest-table-cache.expire-after-write-ms";
  public static final long TABLE_CACHE_EXPIRE_AFTER_WRITE_MS_DEFAULT = TimeUnit.MINUTES.toMillis(5);

  public static final String TABLE_CACHE_MAX_ENTRIES = "rest-table-cache.max-entries";
  public static final int TABLE_CACHE_MAX_ENTRIES_DEFAULT = 100;

  public enum SnapshotMode {
    ALL,
    REFS
  }

  /**
   * Enum to represent scan planning mode.
   *
   * <ul>
   *   <li>CLIENT - Use client-side scan planning
   *   <li>CATALOG - Use server-side scan planning
   * </ul>
   */
  public enum ScanPlanningMode {
    CLIENT("client"),
    CATALOG("catalog");

    private final String modeName;

    ScanPlanningMode(String modeName) {
      this.modeName = modeName;
    }

    public String modeName() {
      return modeName;
    }

    public static ScanPlanningMode fromString(String mode) {
      for (ScanPlanningMode planningMode : values()) {
        if (planningMode.modeName.equalsIgnoreCase(mode)) {
          return planningMode;
        }
      }

      throw new IllegalArgumentException(
          String.format("Invalid scan planning mode: %s. Valid values are: client, catalog", mode));
    }
  }
}
