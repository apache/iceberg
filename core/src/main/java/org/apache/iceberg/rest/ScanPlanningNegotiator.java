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
import org.apache.iceberg.rest.RESTCatalogProperties.ScanPlanningMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles negotiation between client and server scan planning mode configurations.
 *
 * <p>This class encapsulates the decision logic for determining whether to use client-side or
 * server-side scan planning based on:
 *
 * <ul>
 *   <li>Client mode (CLIENT_ONLY, CLIENT_PREFERRED, CATALOG_PREFERRED, CATALOG_ONLY, or null)
 *   <li>Server mode (CLIENT_ONLY, CLIENT_PREFERRED, CATALOG_PREFERRED, CATALOG_ONLY, or null)
 *   <li>Server capabilities (supports planning endpoint or not)
 * </ul>
 *
 * <p><b>Negotiation Rules</b>
 *
 * <p>When both client and server configure scan planning mode, the values are negotiated:
 *
 * <ul>
 *   <li><b>Incompatible hard requirements</b>: CLIENT_ONLY + CATALOG_ONLY = FAIL
 *   <li><b>ONLY wins over PREFERRED</b>: When one side has "ONLY" and the other has "PREFERRED",
 *       the ONLY requirement wins (inflexible beats flexible)
 *   <li><b>Both PREFERRED</b>: When both are PREFERRED (different types), client config wins
 *   <li><b>Both same</b>: When both have the same value, use that planning type
 *   <li><b>Only one configured</b>: Use the configured side (client or server)
 *   <li><b>Neither configured</b>: Use default (CLIENT_PREFERRED)
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
   * Negotiates scan planning strategy between client and server configurations.
   *
   * <p>Precedence: Client config &gt; Server config &gt; Default (CLIENT_PREFERRED)
   *
   * @param clientMode the client's scan planning mode (from catalog properties), may be null
   * @param serverMode the server's scan planning mode (from LoadTableResponse.config()), may be
   *     null
   * @param serverSupportsPlanning whether the server advertises the scan planning endpoint
   * @param tableIdentifier the table identifier (for error messages and logging)
   * @return the negotiated planning decision
   * @throws IllegalStateException if client and server requirements are incompatible (CLIENT_ONLY
   *     vs CATALOG_ONLY)
   * @throws UnsupportedOperationException if required server-side planning but server doesn't
   *     support it
   */
  public static PlanningDecision negotiate(
      ScanPlanningMode clientMode,
      ScanPlanningMode serverMode,
      boolean serverSupportsPlanning,
      TableIdentifier tableIdentifier) {

    // Determine effective mode through negotiation
    ScanPlanningMode effectiveMode;
    String modeSource;

    if (clientMode != null && serverMode != null) {
      // Both client and server have configured modes - negotiate
      effectiveMode = negotiateBetweenClientAndServer(clientMode, serverMode, tableIdentifier);
      modeSource =
          String.format(
              "negotiated (client: %s, server: %s)", clientMode.modeName(), serverMode.modeName());
    } else if (clientMode != null) {
      // Only client configured
      effectiveMode = clientMode;
      modeSource = "client config";
    } else if (serverMode != null) {
      // Only server configured
      effectiveMode = serverMode;
      modeSource = "server config";
    } else {
      // Neither configured, use default
      effectiveMode = ScanPlanningMode.CLIENT_PREFERRED;
      modeSource = "default";
    }

    // Apply the effective mode
    return applyMode(effectiveMode, serverSupportsPlanning, tableIdentifier, modeSource);
  }

  /**
   * Negotiate between client and server modes when both are configured.
   *
   * <p>Rules:
   *
   * <ul>
   *   <li>Both same = Use that mode
   *   <li>Both ONLY but want opposite planning = FAIL (incompatible)
   *   <li>One ONLY = ONLY wins (inflexible beats flexible)
   *   <li>Both PREFERRED = Client config wins
   * </ul>
   *
   * @return the negotiated mode
   * @throws IllegalStateException if both are ONLY but want opposite planning locations
   */
  private static ScanPlanningMode negotiateBetweenClientAndServer(
      ScanPlanningMode clientMode, ScanPlanningMode serverMode, TableIdentifier tableIdentifier) {

    // Fast path: both are the same - no negotiation needed
    if (clientMode == serverMode) {
      LOG.debug(
          "Client and server agree on scan planning mode {} for table {}",
          clientMode.modeName(),
          tableIdentifier);
      return clientMode;
    }

    // Check for incompatible hard requirements: both are ONLY but want opposite planning locations
    if (clientMode.isOnly() && serverMode.isOnly()) {
      // Both have hard requirements but want different things
      throw new IllegalStateException(
          String.format(
              "Incompatible scan planning requirements for table %s: "
                  + "client requires %s but server requires %s. "
                  + "Either change client config or update server mode.",
              tableIdentifier,
              clientMode.prefersClient() ? "client-side planning" : "server-side planning",
              serverMode.prefersClient() ? "client-side planning" : "server-side planning"));
    }

    // ONLY wins over PREFERRED (inflexible beats flexible)
    if (clientMode.isOnly()) {
      LOG.debug(
          "Client mode {} (hard requirement) wins over server mode {} (flexible) for table {}",
          clientMode.modeName(),
          serverMode.modeName(),
          tableIdentifier);
      return clientMode;
    }

    if (serverMode.isOnly()) {
      LOG.debug(
          "Server mode {} (hard requirement) wins over client mode {} (flexible) for table {}",
          serverMode.modeName(),
          clientMode.modeName(),
          tableIdentifier);
      return serverMode;
    }

    // Both are PREFERRED - client config wins
    LOG.debug(
        "Both client ({}) and server ({}) are flexible (PREFERRED). Client config wins for table {}",
        clientMode.modeName(),
        serverMode.modeName(),
        tableIdentifier);
    return clientMode;
  }

  /**
   * Apply the effective mode and determine planning decision.
   *
   * @return the planning decision based on effective mode and server capabilities
   * @throws UnsupportedOperationException if CATALOG_ONLY but server doesn't support planning
   */
  private static PlanningDecision applyMode(
      ScanPlanningMode effectiveMode,
      boolean serverSupportsPlanning,
      TableIdentifier tableIdentifier,
      String modeSource) {

    switch (effectiveMode) {
      case CLIENT_ONLY:
      case CLIENT_PREFERRED:
        LOG.debug(
            "Using client-side planning for table {} (mode: {}, source: {})",
            tableIdentifier,
            effectiveMode.modeName(),
            modeSource);
        return PlanningDecision.USE_CLIENT_PLANNING;

      case CATALOG_PREFERRED:
        // Prefer server-side, but fall back to client if unavailable
        if (!serverSupportsPlanning) {
          LOG.warn(
              "Table {} prefers server-side planning (mode: CATALOG_PREFERRED from {}) "
                  + "but server doesn't support it. Falling back to client-side planning. "
                  + "Consider upgrading server or changing mode to CLIENT_PREFERRED.",
              tableIdentifier,
              modeSource);
          return PlanningDecision.USE_CLIENT_PLANNING;
        }
        LOG.debug(
            "Using server-side planning for table {} (mode: CATALOG_PREFERRED, source: {})",
            tableIdentifier,
            modeSource);
        return PlanningDecision.USE_CATALOG_PLANNING;

      case CATALOG_ONLY:
        // Must use server-side, fail if unavailable
        if (!serverSupportsPlanning) {
          throw new UnsupportedOperationException(
              String.format(
                  "Scan planning mode requires server-side planning (CATALOG_ONLY from %s) "
                      + "for table %s but server does not support planning endpoint. "
                      + "Either change scan planning mode or upgrade server to support scan planning.",
                  modeSource, tableIdentifier));
        }
        LOG.debug(
            "Using server-side planning for table {} (mode: CATALOG_ONLY, source: {})",
            tableIdentifier,
            modeSource);
        return PlanningDecision.USE_CATALOG_PLANNING;

      default:
        throw new IllegalStateException("Unknown scan planning mode: " + effectiveMode);
    }
  }
}
