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

  public enum SnapshotMode {
    ALL,
    REFS
  }
}
