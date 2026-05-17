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
import org.apache.iceberg.relocated.com.google.common.base.Joiner;

/**
 * Capabilities the REST client SDK declares it supports to the catalog server.
 *
 * <p>The set of supported capabilities is sent on every REST request via the {@code
 * X-Iceberg-Client-Capabilities} header as a comma-separated list of {@link #headerValue() header
 * values}. The header is purely informational: the server MAY use it to tailor responses, but MUST
 * NOT require it and MUST NOT fail when it is absent.
 *
 * <p>Capabilities are independent of one another. A client that supports {@link
 * #VENDED_CREDENTIALS} does not preclude support for {@link #REMOTE_SIGNING} or {@link
 * #SCAN_PLANNING}; clients should advertise every capability they support.
 */
public enum ClientCapability {
  /**
   * Client supports receiving and using storage credentials vended by the catalog server.
   *
   * <p>When advertised, the server MAY return a {@code storage-credentials} array in {@code
   * LoadTableResult} (and equivalent load responses) where each entry contains a storage location
   * prefix and a config map of credentials (e.g. {@code s3.access-key-id}, {@code
   * s3.session-token}, GCS or ADLS equivalents). The client passes these to its {@link
   * org.apache.iceberg.io.FileIO} when the implementation also implements {@link
   * org.apache.iceberg.io.SupportsStorageCredentials}. Vended credentials take precedence over
   * inline credentials in the response {@code config} map.
   */
  VENDED_CREDENTIALS("vended-credentials"),

  /**
   * Client supports delegating S3 request signing to a remote signing service exposed by the
   * catalog server.
   *
   * <p>When advertised, the client may issue per-request signing calls to {@code POST
   * /v1/{prefix}/namespaces/{namespace}/tables/{table}/sign} with a {@link
   * org.apache.iceberg.rest.requests.RemoteSignRequest} (region, method, URI, headers, optional
   * body) and use the signed URI and headers returned in {@link
   * org.apache.iceberg.rest.responses.RemoteSignResponse} to perform the actual S3 operation. This
   * allows the catalog to centrally control AWS credentials without distributing them to clients.
   */
  REMOTE_SIGNING("remote-signing"),

  /**
   * Client supports server-side scan planning via the REST API.
   *
   * <p>When advertised, the client can drive the asynchronous planning protocol: submit a {@link
   * org.apache.iceberg.rest.requests.PlanTableScanRequest} to {@code POST
   * /v1/{prefix}/namespaces/{namespace}/tables/{table}/plan}, poll {@code GET .../plan/{plan-id}}
   * while the plan status is {@code SUBMITTED}, and fetch paginated {@link
   * org.apache.iceberg.rest.requests.FetchScanTasksRequest} results once the plan is {@code
   * COMPLETED}. This shifts manifest reading and task generation from the client to the server.
   */
  SCAN_PLANNING("scan-planning");

  /**
   * Comma-separated list of every capability's {@link #headerValue()}, suitable for use as the
   * {@code X-Iceberg-Client-Capabilities} request header value.
   */
  public static final String HEADER_VALUE =
      Joiner.on(',').join(Arrays.stream(values()).map(ClientCapability::headerValue).iterator());

  private final String headerValue;

  ClientCapability(String headerValue) {
    this.headerValue = headerValue;
  }

  /** Returns the header token used to advertise this capability. */
  public String headerValue() {
    return headerValue;
  }
}
