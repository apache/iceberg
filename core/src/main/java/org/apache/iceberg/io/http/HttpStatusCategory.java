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
package org.apache.iceberg.io.http;

import org.apache.hc.core5.http.HttpStatus;

/**
 * Coarse, vendor-agnostic classification of an HTTP status code for the pre-signed-URL read path.
 *
 * <p>This is the single place that maps a raw status code to a category; call sites choose the
 * policy for each one (read the body, treat as end-of-file, retry, or fail). Only {@link
 * #SERVER_ERROR} (5xx) is treated as transient and retried by the read path today. Concentrating
 * the mapping here keeps the response-handling seam easy to find, so a store-specific step can
 * later inspect the response body and reclassify an otherwise-terminal status that is actually
 * transient (for example a throttling {@code 403}, which some object stores return under heavy
 * load) so that it is retried too.
 */
enum HttpStatusCategory {
  OK,
  PARTIAL_CONTENT,
  NOT_FOUND,
  FORBIDDEN,
  RANGE_NOT_SATISFIABLE,
  SERVER_ERROR,
  UNEXPECTED;

  static HttpStatusCategory classify(int statusCode) {
    return switch (statusCode) {
      case HttpStatus.SC_OK -> OK;
      case HttpStatus.SC_PARTIAL_CONTENT -> PARTIAL_CONTENT;
      case HttpStatus.SC_NOT_FOUND -> NOT_FOUND;
      case HttpStatus.SC_FORBIDDEN -> FORBIDDEN;
      case HttpStatus.SC_REQUESTED_RANGE_NOT_SATISFIABLE -> RANGE_NOT_SATISFIABLE;
      default -> statusCode >= 500 ? SERVER_ERROR : UNEXPECTED;
    };
  }
}
