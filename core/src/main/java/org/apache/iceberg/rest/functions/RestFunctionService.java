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
package org.apache.iceberg.rest.functions;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.ErrorHandlers;
import org.apache.iceberg.rest.HTTPClient;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.auth.DefaultAuthSession;
import org.apache.iceberg.rest.responses.ListFunctionsResponse;
import org.apache.iceberg.rest.responses.LoadFunctionResponse;

/**
 * Minimal REST client to list and fetch UDF function specs for the POC, using Iceberg HTTPClient.
 */
public class RestFunctionService {

  private final String baseUri; // e.g., http://localhost:8181
  private final HTTPClient http;

  public RestFunctionService(String baseUri, String authHeader) {
    this.baseUri = baseUri.endsWith("/") ? baseUri.substring(0, baseUri.length() - 1) : baseUri;

    Map<String, String> props = Maps.newHashMap();
    HTTPHeaders headers =
        authHeader != null && !authHeader.isEmpty()
            ? HTTPHeaders.of(java.util.Map.of("Authorization", authHeader))
            : HTTPHeaders.EMPTY;
    this.http =
        HTTPClient.builder(props)
            .uri(this.baseUri)
            .withAuthSession(DefaultAuthSession.of(headers))
            .build();
  }

  public List<String> listFunctions(String[] namespace) {
    String ns = String.join(".", namespace);
    Map<String, String> params = Map.of("namespace", ns);
    ListFunctionsResponse resp =
        http.get(
            "v1/functions",
            params,
            ListFunctionsResponse.class,
            java.util.Map.of(),
            ErrorHandlers.defaultErrorHandler());
    List<String> names = Lists.newArrayList();
    names.addAll(resp.names());
    return names;
  }

  public String getFunctionSpecJson(String[] namespace, String name) {
    String ns = String.join(".", namespace);
    if (ns.isEmpty()) {
      ns = "default";
    }
    String path = String.format("v1/functions/%s/%s", ns, name);
    LoadFunctionResponse resp =
        http.get(
            path,
            LoadFunctionResponse.class,
            java.util.Map.of(),
            ErrorHandlers.defaultErrorHandler());
    ObjectNode spec = resp.spec();
    return spec.toString();
  }
}
