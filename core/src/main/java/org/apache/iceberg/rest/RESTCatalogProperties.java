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

public final class RESTCatalogProperties {

  private RESTCatalogProperties() {}

  public static final String SNAPSHOT_LOADING_MODE = "snapshot-loading-mode";
  public static final String SNAPSHOT_LOADING_MODE_DEFAULT = SnapshotMode.ALL.name();

  public static final String METRICS_REPORTING_ENABLED = "rest-metrics-reporting-enabled";
  public static final boolean METRICS_REPORTING_ENABLED_DEFAULT = true;

  // for backwards compatibility with older REST servers where it can be assumed that a particular
  // server supports view endpoints but doesn't send the "endpoints" field in the ConfigResponse
  public static final String VIEW_ENDPOINTS_SUPPORTED = "view-endpoints-supported";
  public static final boolean VIEW_ENDPOINTS_SUPPORTED_DEFAULT = false;

  public static final String PAGE_SIZE = "rest-page-size";

  public static final String NAMESPACE_SEPARATOR = "namespace-separator";

  // Configure scan planning mode on the REST server side
  public static final String REST_SCAN_PLANNING_MODE = "rest-scan-planning-mode";
  public static final String REST_SCAN_PLANNING_MODE_DEFAULT = ScanPlanningMode.NONE.modeName();

  public enum SnapshotMode {
    ALL,
    REFS
  }

  /**
   * Enum to represent the scan planning mode for REST catalog.
   *
   * <ul>
   *   <li>NONE - Server-side scan planning is not supported/allowed
   *   <li>OPTIONAL - Client can choose between client-side or server-side planning
   *   <li>REQUIRED - Server-side planning is required (client-side planning not allowed)
   * </ul>
   */
  public enum ScanPlanningMode {
    NONE("none"),
    OPTIONAL("optional"),
    REQUIRED("required");

    private final String modeName;

    ScanPlanningMode(String modeName) {
      this.modeName = modeName;
    }

    public String modeName() {
      return modeName;
    }

    public static ScanPlanningMode fromString(String mode) {
      if (mode == null) {
        return NONE;
      }
      for (ScanPlanningMode planningMode : values()) {
        if (planningMode.modeName.equalsIgnoreCase(mode)) {
          return planningMode;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Invalid scan planning mode: %s. Valid values are: none, optional, required", mode));
    }
  }
}
