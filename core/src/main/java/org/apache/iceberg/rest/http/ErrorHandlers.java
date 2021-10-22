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


package org.apache.iceberg.rest.http;

import java.util.function.Consumer;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.AuthorizationDeniedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorHandlers {

  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  private ErrorHandlers() {

  }

  /**
   * Table level error handlers.
   */
  public static Consumer<CloseableHttpResponse> tableErrorHandler() {
    return (errorResponse) -> {
      String responseBody = getResponseBody(errorResponse);
      String responseException = getIcebergExceptionHeader(errorResponse);

      switch (errorResponse.getCode()) {
        case HttpStatus.SC_NOT_FOUND:
          // TODO: Exception handling here could be better
          // some methods can result in different resource not found exceptions, so here we need to
          // differentiate between 404 for non-existent Namespace/Database by looking at X-Iceberg-Exception header.
          if (NoSuchNamespaceException.class.getSimpleName().equals(responseException)) {
            throw new NoSuchNamespaceException("Resource not found: %s", responseBody);
          } else {
            throw new NoSuchTableException("Resource not found: %s", responseBody);
          }
        case HttpStatus.SC_CONFLICT:
          throw new AlreadyExistsException("Already exists: %s", responseBody);
        case HttpStatus.SC_FORBIDDEN:
        case HttpStatus.SC_UNAUTHORIZED:
          throw new AuthorizationDeniedException("Not Authorized: %s", responseBody);
        default:
          throw new RestException("Unknown error: %s", errorResponse);
      }
    };
  }

  /**
   * Request error handlers specifically for CRUD ops on databases / namespaces.
   */
  public static Consumer<CloseableHttpResponse> databaseErrorHandler() {
    return (errorResponse) -> {
      String responseBody = getResponseBody(errorResponse);

      switch (errorResponse.getCode()) {
        case HttpStatus.SC_NOT_FOUND:
          throw new NoSuchNamespaceException("Namespace not found: %s", responseBody);
        case HttpStatus.SC_CONFLICT:
          throw new AlreadyExistsException("Already exists: %s", responseBody);
        case HttpStatus.SC_FORBIDDEN:
        case HttpStatus.SC_UNAUTHORIZED:
          throw new AuthorizationDeniedException("Not Authorized: %s", responseBody);
        default:
          throw new RestException("Unknown error: %s", errorResponse);
      }
    };
  }

  static String getResponseBody(CloseableHttpResponse response) {
    String responseBody = "Non-parseable Response Body";
    try {
      responseBody = EntityUtils.toString(response.getEntity());
    } catch (Exception e) {
      LOG.error("Encountered an exception getting response body", e);
    }

    return responseBody;
  }

  static String getIcebergExceptionHeader(CloseableHttpResponse response) {
    String icebergException = null;
    try {
      // TODO - Extract more specific exception from a header or from the response body?
      //        Some servers/proxies will strip headers that aren't in the whitelist so possibly headers
      //        aren't the way to go (though that's typically more on the Request end than the Response end).
      icebergException = response.getHeader("X-Iceberg-Exception").getValue();
    } catch (Exception e) {
      // TODO - Better error message and handling. Will be refactoring this anyway.
      LOG.error("Encountered an error when getting the X-Iceberg-Exception header", e);
    }

    return icebergException;
  }
}
