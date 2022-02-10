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

import java.io.IOException;
import java.util.function.Consumer;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * A set of consumers to handle errors for requests for table entities or for namespace entities,
 * to throw the correct exception.
 */
public class ErrorHandlers {

  private ErrorHandlers() {
  }

  public static Consumer<ErrorResponse> namespaceErrorHandler() {
    return baseNamespaceErrorHandler().andThen(defaultErrorHandler());
  }

  public static Consumer<ErrorResponse> tableErrorHandler() {
    return baseTableErrorHandler().andThen(defaultErrorHandler());
  }

  /**
   * Table level error handlers.
   * Should be chained wih the {@linkplain ErrorHandlers#defaultErrorHandler}, which takes care of common cases.
   */
  private static Consumer<ErrorResponse> baseTableErrorHandler() {
    return error -> {

      switch (error.code()) {
        // Some table resource routes can encounter namespace not found or table not found.
        // Those routes might need a separate error handler first.
        case HttpStatus.SC_NOT_FOUND:
          if (NoSuchNamespaceException.class.getSimpleName().equals(error.type())) {
            throw new NoSuchNamespaceException("Resource not found: %s", error);
          } else {
            throw new NoSuchTableException("Resource not found: %s", error);
          }
        case HttpStatus.SC_CONFLICT:
          throw new AlreadyExistsException("The table already exists: %s", error);
      }
    };
  }

  /**
   * Request error handlers specifically for CRUD ops on namespaces.
   * Should be chained wih the {@linkplain ErrorHandlers#defaultErrorHandler}, which takes care of common cases.
   */
  private static Consumer<ErrorResponse> baseNamespaceErrorHandler() {
    return error -> {
      switch (error.code()) {
        case HttpStatus.SC_NOT_FOUND:
          throw new NoSuchNamespaceException("Namespace not found: %s", error);
        case HttpStatus.SC_CONFLICT:
          throw new AlreadyExistsException("Namespace already exists: %s", error);
        case HttpStatus.SC_UNPROCESSABLE_ENTITY:
          throw new RESTException("Unable to process request due to bad property updates");
      }
    };
  }

  /**
   * Request error handler that handles the common cases that are included with all responses,
   * such as 400, 401, 403, 500, etc.
   */
  public static Consumer<ErrorResponse> defaultErrorHandler() {
    return errorResponse -> {
      switch (errorResponse.code()) {
        case HttpStatus.SC_CLIENT_ERROR:
          throw new BadRequestException("Unable to process the request as it is somehow malformed: %s", errorResponse);
        case HttpStatus.SC_UNAUTHORIZED:
          throw new NotAuthorizedException("Not Authorized: %s", errorResponse);
        case HttpStatus.SC_FORBIDDEN:
          throw new ForbiddenException("Forbidden: %s", errorResponse);
        case HttpStatus.SC_METHOD_NOT_ALLOWED:
        case HttpStatus.SC_NOT_ACCEPTABLE:
        case HttpStatus.SC_NOT_IMPLEMENTED:
          throw new UnsupportedOperationException(String.format("Not supported: %s", errorResponse));
        case HttpStatus.SC_SERVER_ERROR:
          throw new ServiceFailureException("Server error: %s", errorResponse);
        default:
          throw new RESTException("Unknown error: %s", errorResponse);
      }
    };
  }

  static String extractResponseBodyAsString(CloseableHttpResponse response) {
    try {
      if (response.getEntity() == null) {
        return null;
      }
      return EntityUtils.toString(response.getEntity());
    } catch (IOException | ParseException e) {
      throw new RESTException(e, "Encountered an exception converting HTTP response body to string");
    }
  }
}
