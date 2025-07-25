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

import java.net.URI;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.auth.oauth2.test.TestServer;
import org.immutables.value.Value;
import org.mockserver.model.HttpRequest;
import org.mockserver.model.HttpResponse;

@Value.Immutable
public abstract class MetadataDiscoveryExpectation extends BaseExpectation {

  @Override
  @SuppressWarnings("resource")
  public void create() {
    if (testEnvironment().discoveryEnabled()) {
      URI issuerUrl = testEnvironment().authorizationServerUrl();
      URI discoveryEndpoint = testEnvironment().discoveryEndpoint();
      ImmutableMap.Builder<String, Object> builder =
          ImmutableMap.<String, Object>builder()
              .put("issuer", issuerUrl.toString())
              .put("token_endpoint", testEnvironment().tokenEndpoint().toString());

      TestServer.instance()
          .when(HttpRequest.request().withMethod("GET").withPath(discoveryEndpoint.getPath()))
          .respond(HttpResponse.response().withBody(jsonBody(builder.build())));
    }
  }
}
