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

import java.util.Arrays;
import java.util.stream.Collectors;

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

  // Configure scan planning mode
  // Can be set by server in LoadTableResponse.config() or by client in catalog properties
  // Negotiation rules: ONLY beats PREFERRED, both PREFERRED = client wins
  // Default when neither client nor server provides: client-preferred
  public static final String SCAN_PLANNING_MODE = "scan-planning-mode";
  public static final String SCAN_PLANNING_MODE_DEFAULT =
      ScanPlanningMode.CLIENT_PREFERRED.modeName();

  public enum SnapshotMode {
    ALL,
    REFS
  }

  /**
   * Enum to represent scan planning mode configuration.
   *
   * <p>Can be configured by:
   *
   * <ul>
   *   <li>Server: Returned in LoadTableResponse.config() to advertise server preference/requirement
   *   <li>Client: Set in catalog properties to set client preference/requirement
   * </ul>
   *
   * <p>When both client and server configure this property, the values are negotiated:
   *
   * <p>Values:
   *
   * <ul>
   *   <li>CLIENT_ONLY - MUST use client-side planning. Fails if paired with CATALOG_ONLY from other
   *       side.
   *   <li>CLIENT_PREFERRED (default) - Prefer client-side planning but flexible.
   *   <li>CATALOG_PREFERRED - Prefer server-side planning but flexible. Falls back to client if
   *       server doesn't support planning endpoints.
   *   <li>CATALOG_ONLY - MUST use server-side planning. Requires server support. Fails if paired
   *       with CLIENT_ONLY from other side.
   * </ul>
   *
   * <p>Negotiation rules when both sides are configured:
   *
   * <ul>
   *   <li><b>Incompatible</b>: CLIENT_ONLY + CATALOG_ONLY = FAIL
   *   <li><b>ONLY beats PREFERRED</b>: One "ONLY" + opposite "PREFERRED" = ONLY wins (inflexible
   *       beats flexible)
   *   <li><b>Both PREFERRED</b>: Different PREFERRED types = Client config wins
   *   <li><b>Both same</b>: Use that planning type
   *   <li><b>Only one configured</b>: Use the configured side
   * </ul>
   */
  public enum ScanPlanningMode {
    CLIENT_ONLY("client-only"),
    CLIENT_PREFERRED("client-preferred"),
    CATALOG_PREFERRED("catalog-preferred"),
    CATALOG_ONLY("catalog-only");

    private final String modeName;

    ScanPlanningMode(String modeName) {
      this.modeName = modeName;
    }

    public String modeName() {
      return modeName;
    }

    public boolean isClientOnly() {
      return this == CLIENT_ONLY;
    }

    public boolean isCatalogOnly() {
      return this == CATALOG_ONLY;
    }

    public boolean isOnly() {
      return this == CLIENT_ONLY || this == CATALOG_ONLY;
    }

    public boolean isPreferred() {
      return this == CLIENT_PREFERRED || this == CATALOG_PREFERRED;
    }

    public boolean prefersClient() {
      return this == CLIENT_ONLY || this == CLIENT_PREFERRED;
    }

    public boolean prefersCatalog() {
      return this == CATALOG_ONLY || this == CATALOG_PREFERRED;
    }

    public static ScanPlanningMode fromString(String mode) {
      if (mode == null) {
        return CLIENT_PREFERRED;
      }
      for (ScanPlanningMode planningMode : values()) {
        if (planningMode.modeName.equalsIgnoreCase(mode)) {
          return planningMode;
        }
      }
      String validModes =
          Arrays.stream(values()).map(ScanPlanningMode::modeName).collect(Collectors.joining(", "));
      throw new IllegalArgumentException(
          String.format("Invalid scan planning mode: %s. Valid values are: %s", mode, validModes));
    }
  }
}
