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

import java.util.function.Consumer;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.rest.responses.ErrorResponse;

/**
 * A set of consumers to handle errors for requests for table entities or for namespace entities, to
 * throw the correct exception.
 */
public class ErrorHandlers {

  private ErrorHandlers() {}

  public static Consumer<ErrorResponse> namespaceErrorHandler() {
    return baseNamespaceErrorHandler().andThen(defaultErrorHandler());
  }

  public static Consumer<ErrorResponse> tableErrorHandler() {
    return baseTableErrorHandler().andThen(defaultErrorHandler());
  }

  public static Consumer<ErrorResponse> tableCommitHandler() {
    return baseCommitErrorHandler().andThen(defaultErrorHandler());
  }

  private static Consumer<ErrorResponse> baseCommitErrorHandler() {
    return error -> {
      switch (error.code()) {
        case 404:
          throw new NoSuchTableException("%s", error.message());
        case 409:
          throw new CommitFailedException("Commit failed: %s", error.message());
      }
    };
  }

  /**
   * Table level error handlers. Should be chained wih the {@link #defaultErrorHandler}, which takes
   * care of common cases.
   */
  private static Consumer<ErrorResponse> baseTableErrorHandler() {
    return error -> {
      switch (error.code()) {
        case 404:
          if (NoSuchNamespaceException.class.getSimpleName().equals(error.type())) {
            throw new NoSuchNamespaceException("%s", error.message());
          } else {
            throw new NoSuchTableException("%s", error.message());
          }
        case 409:
          throw new AlreadyExistsException("%s", error.message());
      }
    };
  }

  /**
   * Request error handlers specifically for CRUD ops on namespaces. Should be chained wih the
   * {@link #defaultErrorHandler}, which takes care of common cases.
   */
  private static Consumer<ErrorResponse> baseNamespaceErrorHandler() {
    return error -> {
      switch (error.code()) {
        case 404:
          throw new NoSuchNamespaceException("%s", error.message());
        case 409:
          throw new AlreadyExistsException("%s", error.message());
        case 422:
          throw new RESTException("Unable to process: %s", error.message());
      }
    };
  }

  /**
   * Request error handler that handles the common cases that are included with all responses, such
   * as 400, 500, etc.
   */
  public static Consumer<ErrorResponse> defaultErrorHandler() {
    return error -> {
      switch (error.code()) {
        case 400:
          throw new BadRequestException("Malformed request: %s", error.message());
        case 401:
          throw new NotAuthorizedException("Not authorized: %s", error.message());
        case 403:
          throw new ForbiddenException("Forbidden: %s", error.message());
        case 405:
        case 406:
          break;
        case 501:
          throw new UnsupportedOperationException(error.message());
        case 500:
          throw new ServiceFailureException("Server error: %s: %s", error.type(), error.message());
      }

      throw new RESTException("Unable to process: %s", error.message());
    };
  }
}
