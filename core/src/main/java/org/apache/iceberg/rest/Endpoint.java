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

import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.hc.core5.http.Method;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.base.Supplier;

/**
 * Holds an endpoint definition that consists of the HTTP method (GET, POST, DELETE, ...) and the
 * resource path as defined in the Iceberg OpenAPI REST specification without parameter
 * substitution, such as <b>/v1/{prefix}/namespaces/{namespace}</b>.
 */
public class Endpoint {

  // namespace endpoints
  public static final Endpoint V1_LIST_NAMESPACES =
      Endpoint.create("GET", ResourcePaths.V1_NAMESPACES);
  public static final Endpoint V1_LOAD_NAMESPACE =
      Endpoint.create("GET", ResourcePaths.V1_NAMESPACE);
  public static final Endpoint V1_CREATE_NAMESPACE =
      Endpoint.create("POST", ResourcePaths.V1_NAMESPACES);
  public static final Endpoint V1_UPDATE_NAMESPACE =
      Endpoint.create("POST", ResourcePaths.V1_NAMESPACE_PROPERTIES);
  public static final Endpoint V1_DELETE_NAMESPACE =
      Endpoint.create("DELETE", ResourcePaths.V1_NAMESPACE);
  public static final Endpoint V1_COMMIT_TRANSACTION =
      Endpoint.create("POST", ResourcePaths.V1_TRANSACTIONS_COMMIT);

  // table endpoints
  public static final Endpoint V1_LIST_TABLES = Endpoint.create("GET", ResourcePaths.V1_TABLES);
  public static final Endpoint V1_LOAD_TABLE = Endpoint.create("GET", ResourcePaths.V1_TABLE);
  public static final Endpoint V1_CREATE_TABLE = Endpoint.create("POST", ResourcePaths.V1_TABLES);
  public static final Endpoint V1_UPDATE_TABLE = Endpoint.create("POST", ResourcePaths.V1_TABLE);
  public static final Endpoint V1_DELETE_TABLE = Endpoint.create("DELETE", ResourcePaths.V1_TABLE);
  public static final Endpoint V1_RENAME_TABLE =
      Endpoint.create("POST", ResourcePaths.V1_TABLE_RENAME);
  public static final Endpoint V1_REGISTER_TABLE =
      Endpoint.create("POST", ResourcePaths.V1_TABLE_REGISTER);
  public static final Endpoint V1_REPORT_METRICS =
      Endpoint.create("POST", ResourcePaths.V1_TABLE_METRICS);

  // view endpoints
  public static final Endpoint V1_LIST_VIEWS = Endpoint.create("GET", ResourcePaths.V1_VIEWS);
  public static final Endpoint V1_LOAD_VIEW = Endpoint.create("GET", ResourcePaths.V1_VIEW);
  public static final Endpoint V1_CREATE_VIEW = Endpoint.create("POST", ResourcePaths.V1_VIEWS);
  public static final Endpoint V1_UPDATE_VIEW = Endpoint.create("POST", ResourcePaths.V1_VIEW);
  public static final Endpoint V1_DELETE_VIEW = Endpoint.create("DELETE", ResourcePaths.V1_VIEW);
  public static final Endpoint V1_RENAME_VIEW =
      Endpoint.create("POST", ResourcePaths.V1_VIEW_RENAME);

  private static final Splitter ENDPOINT_SPLITTER = Splitter.on(" ");
  private static final Joiner ENDPOINT_JOINER = Joiner.on(" ");
  private final String httpMethod;
  private final String path;

  private Endpoint(String httpMethod, String path) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(httpMethod), "Invalid HTTP method: null or empty");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path), "Invalid path: null or empty");
    this.httpMethod = Method.normalizedValueOf(httpMethod).toString();
    this.path = path;
  }

  public String httpMethod() {
    return httpMethod;
  }

  public String path() {
    return path;
  }

  public static Endpoint create(String httpMethod, String path) {
    return new Endpoint(httpMethod, path);
  }

  @Override
  public String toString() {
    return ENDPOINT_JOINER.join(httpMethod(), path());
  }

  public static Endpoint fromString(String endpoint) {
    List<String> elements = ENDPOINT_SPLITTER.splitToList(endpoint);
    Preconditions.checkArgument(
        elements.size() == 2,
        "Invalid endpoint (must consist of two elements separated by a single space): %s",
        endpoint);
    return create(elements.get(0), elements.get(1));
  }

  /**
   * Checks if the set of endpoints support the given {@link Endpoint}.
   *
   * @param supportedEndpoints The set of supported endpoints to check
   * @param endpoint The endpoint to check against the set of supported endpoints
   * @throws UnsupportedOperationException if the given {@link Endpoint} is not included in the set
   *     of endpoints.
   */
  public static void check(Set<Endpoint> supportedEndpoints, Endpoint endpoint) {
    if (!supportedEndpoints.contains(endpoint)) {
      throw new UnsupportedOperationException(
          String.format("Server does not support endpoint: %s", endpoint));
    }
  }

  /**
   * Checks if the set of endpoints support the given {@link Endpoint}.
   *
   * @param supportedEndpoints The set of supported endpoints to check
   * @param endpoint The endpoint to check against the set of supported endpoints
   * @param supplier The supplier throwing a {@link RuntimeException} if the given {@link Endpoint}
   *     is not included in the set of endpoints.
   */
  public static void check(
      Set<Endpoint> supportedEndpoints, Endpoint endpoint, Supplier<RuntimeException> supplier) {
    if (!supportedEndpoints.contains(endpoint)) {
      throw supplier.get();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof Endpoint)) {
      return false;
    }

    Endpoint endpoint = (Endpoint) o;
    return Objects.equals(httpMethod, endpoint.httpMethod) && Objects.equals(path, endpoint.path);
  }

  @Override
  public int hashCode() {
    return Objects.hash(httpMethod, path);
  }
}
