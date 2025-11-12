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
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.requests.CommitTransactionRequest;
import org.apache.iceberg.rest.requests.CreateNamespaceRequest;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.CreateViewRequest;
import org.apache.iceberg.rest.requests.FetchScanTasksRequest;
import org.apache.iceberg.rest.requests.PlanTableScanRequest;
import org.apache.iceberg.rest.requests.RegisterTableRequest;
import org.apache.iceberg.rest.requests.RenameTableRequest;
import org.apache.iceberg.rest.requests.ReportMetricsRequest;
import org.apache.iceberg.rest.requests.UpdateNamespacePropertiesRequest;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.apache.iceberg.rest.responses.CreateNamespaceResponse;
import org.apache.iceberg.rest.responses.FetchPlanningResultResponse;
import org.apache.iceberg.rest.responses.FetchScanTasksResponse;
import org.apache.iceberg.rest.responses.GetNamespaceResponse;
import org.apache.iceberg.rest.responses.ListNamespacesResponse;
import org.apache.iceberg.rest.responses.ListTablesResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.LoadViewResponse;
import org.apache.iceberg.rest.responses.OAuthTokenResponse;
import org.apache.iceberg.rest.responses.PlanTableScanResponse;
import org.apache.iceberg.rest.responses.UpdateNamespacePropertiesResponse;
import org.apache.iceberg.util.Pair;

enum Route {
  TOKENS(HTTPRequest.HTTPMethod.POST, ResourcePaths.tokens(), null, OAuthTokenResponse.class),
  SEPARATE_AUTH_TOKENS_URI(
      HTTPRequest.HTTPMethod.POST, "https://auth-server.com/token", null, OAuthTokenResponse.class),
  CONFIG(HTTPRequest.HTTPMethod.GET, ResourcePaths.config(), null, ConfigResponse.class),
  LIST_NAMESPACES(
      HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_NAMESPACES, null, ListNamespacesResponse.class),
  CREATE_NAMESPACE(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_NAMESPACES,
      CreateNamespaceRequest.class,
      CreateNamespaceResponse.class),
  NAMESPACE_EXISTS(HTTPRequest.HTTPMethod.HEAD, ResourcePaths.V1_NAMESPACE),
  LOAD_NAMESPACE(
      HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_NAMESPACE, null, GetNamespaceResponse.class),
  DROP_NAMESPACE(HTTPRequest.HTTPMethod.DELETE, ResourcePaths.V1_NAMESPACE),
  UPDATE_NAMESPACE(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_NAMESPACE_PROPERTIES,
      UpdateNamespacePropertiesRequest.class,
      UpdateNamespacePropertiesResponse.class),
  LIST_TABLES(HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_TABLES, null, ListTablesResponse.class),
  CREATE_TABLE(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLES,
      CreateTableRequest.class,
      LoadTableResponse.class),
  TABLE_EXISTS(HTTPRequest.HTTPMethod.HEAD, ResourcePaths.V1_TABLE),
  LOAD_TABLE(HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_TABLE, null, LoadTableResponse.class),
  REGISTER_TABLE(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLE_REGISTER,
      RegisterTableRequest.class,
      LoadTableResponse.class),
  UPDATE_TABLE(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLE,
      UpdateTableRequest.class,
      LoadTableResponse.class),
  DROP_TABLE(HTTPRequest.HTTPMethod.DELETE, ResourcePaths.V1_TABLE),
  RENAME_TABLE(
      HTTPRequest.HTTPMethod.POST, ResourcePaths.V1_TABLE_RENAME, RenameTableRequest.class, null),
  REPORT_METRICS(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLE_METRICS,
      ReportMetricsRequest.class,
      null),
  COMMIT_TRANSACTION(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TRANSACTIONS_COMMIT,
      CommitTransactionRequest.class,
      null),
  LIST_VIEWS(HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_VIEWS, null, ListTablesResponse.class),
  VIEW_EXISTS(HTTPRequest.HTTPMethod.HEAD, ResourcePaths.V1_VIEW),
  LOAD_VIEW(HTTPRequest.HTTPMethod.GET, ResourcePaths.V1_VIEW, null, LoadViewResponse.class),
  CREATE_VIEW(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_VIEWS,
      CreateViewRequest.class,
      LoadViewResponse.class),
  UPDATE_VIEW(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_VIEW,
      UpdateTableRequest.class,
      LoadViewResponse.class),
  RENAME_VIEW(
      HTTPRequest.HTTPMethod.POST, ResourcePaths.V1_VIEW_RENAME, RenameTableRequest.class, null),
  DROP_VIEW(HTTPRequest.HTTPMethod.DELETE, ResourcePaths.V1_VIEW),
  PLAN_TABLE_SCAN(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLE_SCAN_PLAN_SUBMIT,
      PlanTableScanRequest.class,
      PlanTableScanResponse.class),
  FETCH_PLANNING_RESULT(
      HTTPRequest.HTTPMethod.GET,
      ResourcePaths.V1_TABLE_SCAN_PLAN,
      null,
      FetchPlanningResultResponse.class),
  FETCH_SCAN_TASKS(
      HTTPRequest.HTTPMethod.POST,
      ResourcePaths.V1_TABLE_SCAN_PLAN_TASKS,
      FetchScanTasksRequest.class,
      FetchScanTasksResponse.class),
  CANCEL_PLAN_TABLE_SCAN(
      HTTPRequest.HTTPMethod.DELETE, ResourcePaths.V1_TABLE_SCAN_PLAN, null, null);

  private final HTTPRequest.HTTPMethod method;
  private final int requiredLength;
  private final Map<Integer, String> requirements;
  private final Map<Integer, String> variables;
  private final Class<? extends RESTRequest> requestClass;
  private final Class<? extends RESTResponse> responseClass;
  private final String resourcePath;

  Route(HTTPRequest.HTTPMethod method, String pattern) {
    this(method, pattern, null, null);
  }

  Route(
      HTTPRequest.HTTPMethod method,
      String pattern,
      Class<? extends RESTRequest> requestClass,
      Class<? extends RESTResponse> responseClass) {
    this.method = method;
    this.resourcePath = pattern;

    // parse the pattern into requirements and variables
    List<String> parts =
        Splitter.on('/').splitToList(pattern.replaceFirst("/v1/", "v1/").replace("/{prefix}", ""));
    ImmutableMap.Builder<Integer, String> requirementsBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<Integer, String> variablesBuilder = ImmutableMap.builder();
    for (int pos = 0; pos < parts.size(); pos += 1) {
      String part = parts.get(pos);
      if (part.startsWith("{") && part.endsWith("}")) {
        variablesBuilder.put(pos, part.substring(1, part.length() - 1));
      } else {
        requirementsBuilder.put(pos, part);
      }
    }

    this.requestClass = requestClass;
    this.responseClass = responseClass;

    this.requiredLength = parts.size();
    this.requirements = requirementsBuilder.build();
    this.variables = variablesBuilder.build();
  }

  private boolean matches(HTTPRequest.HTTPMethod requestMethod, List<String> requestPath) {
    return method == requestMethod
        && requiredLength == requestPath.size()
        && requirements.entrySet().stream()
            .allMatch(
                requirement ->
                    requirement.getValue().equalsIgnoreCase(requestPath.get(requirement.getKey())));
  }

  private Map<String, String> variables(List<String> requestPath) {
    ImmutableMap.Builder<String, String> vars = ImmutableMap.builder();
    variables.forEach((key, value) -> vars.put(value, requestPath.get(key)));
    return vars.build();
  }

  public static Pair<Route, Map<String, String>> from(HTTPRequest.HTTPMethod method, String path) {
    List<String> parts = Splitter.on('/').splitToList(path);
    for (Route candidate : Route.values()) {
      if (candidate.matches(method, parts)) {
        return Pair.of(candidate, candidate.variables(parts));
      }
    }

    return null;
  }

  public Class<? extends RESTRequest> requestClass() {
    return requestClass;
  }

  public Class<? extends RESTResponse> responseClass() {
    return responseClass;
  }

  HTTPRequest.HTTPMethod method() {
    return method;
  }

  String resourcePath() {
    return resourcePath;
  }
}
