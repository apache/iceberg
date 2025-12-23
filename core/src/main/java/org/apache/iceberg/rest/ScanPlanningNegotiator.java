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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalogProperties.ClientScanPlanningPreference;
import org.apache.iceberg.rest.RESTCatalogProperties.ScanPlanningMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles negotiation between client scan planning preferences and catalog scan planning mode
 * requirements.
 *
 * <p>This class encapsulates the decision logic for determining whether to use client-side or
 * server-side scan planning based on:
 *
 * <ul>
 *   <li>Catalog mode (CLIENT_ONLY, CLIENT_PREFERRED, CATALOG_PREFERRED, CATALOG_ONLY)
 *   <li>Client preference (NONE, CLIENT_PLANNING, CATALOG_PLANNING)
 *   <li>Server capabilities (supports planning endpoint or not)
 * </ul>
 *
 * <h3>Decision Matrix</h3>
 *
 * The following table shows the final decision based on catalog mode and client preference:
 *
 * <table border="1">
 *   <tr>
 *     <th>Client Preference</th>
 *     <th>CLIENT_ONLY</th>
 *     <th>CLIENT_PREFERRED</th>
 *     <th>CATALOG_PREFERRED</th>
 *     <th>CATALOG_ONLY</th>
 *   </tr>
 *   <tr>
 *     <td><b>NONE</b> (follow catalog)</td>
 *     <td>Client-side</td>
 *     <td>Client-side</td>
 *     <td>Server-side (fallback to client if unavailable)</td>
 *     <td>Server-side (fail if unavailable)</td>
 *   </tr>
 *   <tr>
 *     <td><b>CLIENT_PLANNING</b> (MUST client)</td>
 *     <td>Client-side</td>
 *     <td>Client-side</td>
 *     <td>Client-side</td>
 *     <td><b>IllegalStateException</b></td>
 *   </tr>
 *   <tr>
 *     <td><b>CATALOG_PLANNING</b> (MUST server)</td>
 *     <td><b>IllegalStateException</b></td>
 *     <td>Server-side (fail if unavailable)</td>
 *     <td>Server-side (fail if unavailable)</td>
 *     <td>Server-side (fail if unavailable)</td>
 *   </tr>
 * </table>
 *
 * <p>Notes:
 *
 * <ul>
 *   <li>"Server-side (fallback to client if unavailable)" - Attempts server-side planning with
 *       graceful fallback to client-side if server doesn't advertise planning endpoints.
 *   <li>"Server-side (fail if unavailable)" - Throws {@link UnsupportedOperationException} if
 *       server doesn't advertise planning endpoints.
 *   <li>"IllegalStateException" - Thrown when client and catalog MUST requirements conflict.
 * </ul>
 */
public class ScanPlanningNegotiator {
  private static final Logger LOG = LoggerFactory.getLogger(ScanPlanningNegotiator.class);

  private ScanPlanningNegotiator() {}

  /** Result of scan planning negotiation. */
  public enum PlanningDecision {
    USE_CLIENT_PLANNING,
    USE_CATALOG_PLANNING
  }

  /**
   * Negotiates scan planning strategy between client preference and catalog mode.
   *
   * @param catalogMode the catalog's scan planning mode (from server config or table config)
   * @param clientPref the client's scan planning preference
   * @param serverSupportsPlanning whether the server advertises the scan planning endpoint
   * @param tableIdentifier the table identifier (for error messages and logging)
   * @return the negotiated planning decision
   * @throws IllegalStateException if client and catalog requirements are incompatible
   * @throws UnsupportedOperationException if required server-side planning but server doesn't
   *     support it
   */
  public static PlanningDecision negotiate(
      ScanPlanningMode catalogMode,
      ClientScanPlanningPreference clientPref,
      boolean serverSupportsPlanning,
      TableIdentifier tableIdentifier) {

    switch (clientPref) {
      case NONE:
        // Client follows catalog's decision
        return decideByCatalogMode(catalogMode, serverSupportsPlanning, tableIdentifier);

      case CLIENT_PLANNING:
        // Client explicitly wants client-side planning
        if (catalogMode == ScanPlanningMode.CATALOG_ONLY) {
          throw new IllegalStateException(
              String.format(
                  "Client requires client-side planning but catalog requires server-side planning for table %s. "
                      + "Either change client preference or update catalog mode.",
                  tableIdentifier));
        }
        LOG.debug(
            "Using client-side planning for table {} per client preference (catalog mode: {})",
            tableIdentifier,
            catalogMode);
        return PlanningDecision.USE_CLIENT_PLANNING;

      case CATALOG_PLANNING:
        // Client explicitly wants server-side planning
        if (catalogMode == ScanPlanningMode.CLIENT_ONLY) {
          throw new IllegalStateException(
              String.format(
                  "Client requires server-side planning but catalog requires client-side planning for table %s. "
                      + "Either change client preference or update catalog mode.",
                  tableIdentifier));
        }
        if (!serverSupportsPlanning) {
          throw new UnsupportedOperationException(
              String.format(
                  "Client requires server-side planning for table %s but server does not support planning endpoint. "
                      + "Either change client preference or upgrade server to support scan planning.",
                  tableIdentifier));
        }
        LOG.debug(
            "Using server-side planning for table {} per client preference (catalog mode: {})",
            tableIdentifier,
            catalogMode);
        return PlanningDecision.USE_CATALOG_PLANNING;

      default:
        throw new IllegalStateException("Unknown client preference: " + clientPref);
    }
  }

  private static PlanningDecision decideByCatalogMode(
      ScanPlanningMode catalogMode,
      boolean serverSupportsPlanning,
      TableIdentifier tableIdentifier) {

    switch (catalogMode) {
      case CLIENT_ONLY:
        LOG.debug(
            "Using client-side planning for table {} per catalog requirement (mode: CLIENT_ONLY)",
            tableIdentifier);
        return PlanningDecision.USE_CLIENT_PLANNING;

      case CLIENT_PREFERRED:
        LOG.debug(
            "Using client-side planning for table {} per catalog preference (mode: CLIENT_PREFERRED)",
            tableIdentifier);
        return PlanningDecision.USE_CLIENT_PLANNING;

      case CATALOG_PREFERRED:
        // Catalog prefers server-side, use it if available
        if (!serverSupportsPlanning) {
          LOG.warn(
              "Table {} prefers server-side planning (mode: CATALOG_PREFERRED) but server doesn't support it. "
                  + "Falling back to client-side planning. Consider upgrading server or changing mode to CLIENT_PREFERRED.",
              tableIdentifier);
          return PlanningDecision.USE_CLIENT_PLANNING;
        }
        LOG.debug(
            "Using server-side planning for table {} per catalog preference (mode: CATALOG_PREFERRED)",
            tableIdentifier);
        return PlanningDecision.USE_CATALOG_PLANNING;

      case CATALOG_ONLY:
        if (!serverSupportsPlanning) {
          throw new UnsupportedOperationException(
              String.format(
                  "Catalog requires server-side planning (mode: CATALOG_ONLY) for table %s but server does not support planning endpoint. "
                      + "Either change catalog mode or upgrade server to support scan planning.",
                  tableIdentifier));
        }
        LOG.debug(
            "Using server-side planning for table {} per catalog requirement (mode: CATALOG_ONLY)",
            tableIdentifier);
        return PlanningDecision.USE_CATALOG_PLANNING;

      default:
        throw new IllegalStateException("Unknown catalog mode: " + catalogMode);
    }
  }
}
