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
package org.apache.iceberg.rest.auth.oauth2.test.expectation;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.Endpoint;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.apache.iceberg.rest.responses.ConfigResponse;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@Value.Immutable
public abstract class ConfigEndpointExpectation extends BaseExpectation {

  @Override
  @SuppressWarnings("resource")
  public void create() {
    List<Endpoint> endpoints =
        ImmutableList.<Endpoint>builder()
            .add(Endpoint.V1_LIST_NAMESPACES)
            .add(Endpoint.V1_LOAD_NAMESPACE)
            .add(Endpoint.V1_CREATE_NAMESPACE)
            .add(Endpoint.V1_UPDATE_NAMESPACE)
            .add(Endpoint.V1_DELETE_NAMESPACE)
            .add(Endpoint.V1_LIST_TABLES)
            .add(Endpoint.V1_LOAD_TABLE)
            .add(Endpoint.V1_CREATE_TABLE)
            .add(Endpoint.V1_UPDATE_TABLE)
            .add(Endpoint.V1_DELETE_TABLE)
            .add(Endpoint.V1_RENAME_TABLE)
            .add(Endpoint.V1_REGISTER_TABLE)
            .add(Endpoint.V1_REPORT_METRICS)
            .add(Endpoint.V1_LIST_VIEWS)
            .add(Endpoint.V1_LOAD_VIEW)
            .add(Endpoint.V1_CREATE_VIEW)
            .add(Endpoint.V1_UPDATE_VIEW)
            .add(Endpoint.V1_DELETE_VIEW)
            .add(Endpoint.V1_RENAME_VIEW)
            .add(Endpoint.V1_COMMIT_TRANSACTION)
            .build();
    ConfigResponse response =
        ConfigResponse.builder()
            .withDefaults(Maps.newHashMap())
            .withOverrides(Maps.newHashMap())
            .withEndpoints(endpoints)
            .build();
    TestServer.instance()
        .when(
            HttpRequest.request()
                .withMethod("GET")
                .withPath(testEnvironment().configEndpoint().getPath())
                .withHeader("Accept", "application/json")
                .withHeader("Authorization", "Bearer access_initial"))
        .respond(HttpResponse.response().withBody(jsonBody(response)));
  }
}
