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

  // Configure scan planning mode on the REST catalog/server side
  public static final String REST_SCAN_PLANNING_MODE = "rest-scan-planning-mode";
  public static final String REST_SCAN_PLANNING_MODE_DEFAULT =
      ScanPlanningMode.CLIENT_PREFERRED.modeName();

  // Configure client-side scan planning preference
  public static final String CLIENT_SCAN_PLANNING_PREFERENCE =
      "rest-scan-planning-client-preference";
  public static final String CLIENT_SCAN_PLANNING_PREFERENCE_DEFAULT =
      ClientScanPlanningPreference.NONE.preferenceName();

  public enum SnapshotMode {
    ALL,
    REFS
  }

  /**
   * Enum to represent the scan planning mode for REST catalog (server-side configuration).
   *
   * <p>This defines what the catalog/server requires or prefers for scan planning using RFC 2119
   * semantics (MUST, SHOULD, MAY):
   *
   * <ul>
   *   <li>CLIENT_ONLY - Client MUST use client-side planning. Server MUST NOT provide server-side
   *       planning endpoints.
   *   <li>CLIENT_PREFERRED (default) - Client SHOULD use client-side planning. Server MAY provide
   *       server-side planning if explicitly requested.
   *   <li>CATALOG_PREFERRED - Client SHOULD use server-side planning when available. Server MAY
   *       fall back to client-side if server doesn't support planning or client explicitly requests
   *       it.
   *   <li>CATALOG_ONLY - Client MUST use server-side planning. Server MUST provide server-side
   *       planning endpoints. Client MUST NOT use client-side planning.
   * </ul>
   *
   * <p>For the complete decision matrix showing how this negotiates with {@link
   * ClientScanPlanningPreference}, see {@link
   * org.apache.iceberg.rest.ScanPlanningNegotiator#negotiate}.
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

    public boolean isClientAllowed() {
      return this == CLIENT_ONLY || this == CLIENT_PREFERRED;
    }

    public boolean isCatalogAllowed() {
      return this == CATALOG_PREFERRED || this == CATALOG_ONLY;
    }

    public boolean requiresClient() {
      return this == CLIENT_ONLY;
    }

    public boolean requiresCatalog() {
      return this == CATALOG_ONLY;
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
      throw new IllegalArgumentException(
          String.format(
              "Invalid scan planning mode: %s. Valid values are: client-only, client-preferred, catalog-preferred, catalog-only",
              mode));
    }
  }

  /**
   * Enum to represent the client-side scan planning preference.
   *
   * <p>This defines what the client wants to do for scan planning using RFC 2119 semantics (MUST,
   * SHOULD):
   *
   * <ul>
   *   <li>NONE (default) - Client SHOULD follow catalog's {@link ScanPlanningMode} recommendation.
   *       No explicit preference set. This is the cooperative default behavior.
   *   <li>CLIENT_PLANNING - Client MUST use client-side planning. Throws {@link
   *       IllegalStateException} if catalog mode is {@link ScanPlanningMode#CATALOG_ONLY}.
   *   <li>CATALOG_PLANNING - Client MUST use server-side planning. Throws {@link
   *       IllegalStateException} if catalog mode is {@link ScanPlanningMode#CLIENT_ONLY}, or {@link
   *       UnsupportedOperationException} if server doesn't support planning endpoints.
   * </ul>
   *
   * <p>For the complete decision matrix showing how this negotiates with {@link ScanPlanningMode},
   * see {@link org.apache.iceberg.rest.ScanPlanningNegotiator#negotiate}.
   */
  public enum ClientScanPlanningPreference {
    NONE("none"),
    CLIENT_PLANNING("client"),
    CATALOG_PLANNING("catalog");

    private final String preferenceName;

    ClientScanPlanningPreference(String preferenceName) {
      this.preferenceName = preferenceName;
    }

    public String preferenceName() {
      return preferenceName;
    }

    public static ClientScanPlanningPreference fromString(String pref) {
      if (pref == null || pref.trim().isEmpty()) {
        return NONE;
      }
      for (ClientScanPlanningPreference preference : values()) {
        if (preference.preferenceName.equalsIgnoreCase(pref)) {
          return preference;
        }
      }
      throw new IllegalArgumentException(
          String.format(
              "Invalid client scan planning preference: %s. Valid values are: none, client, catalog",
              pref));
    }
  }
}
