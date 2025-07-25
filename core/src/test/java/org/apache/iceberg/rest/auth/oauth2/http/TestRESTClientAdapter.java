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
package org.apache.iceberg.rest.auth.oauth2.http;

import static org.assertj.core.api.Assertions.assertThat;

import com.nimbusds.oauth2.sdk.auth.ClientAuthenticationMethod;
import com.nimbusds.oauth2.sdk.http.HTTPRequest;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.http.ReadOnlyHTTPResponse;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.oauth2.test.TestEnvironment;
import org.junit.jupiter.api.Test;

public class TestRESTClientAdapter {

  @Test
  void testGETSuccess() throws IOException {
    try (TestEnvironment env = TestEnvironment.builder().build()) {

      HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.GET, env.discoveryEndpoint());
      httpRequest.setHeader("Accept", "application/json");

      RESTClientAdapter adapter = env.restClientAdapter();
      ReadOnlyHTTPResponse response = adapter.send(httpRequest);

      assertThat(response.getStatusCode()).isEqualTo(HTTPResponse.SC_OK);
      assertThat(response.getBody()).contains("\"issuer\" : \"" + env.authorizationServerUrl());
      assertThat(response.getHeaderMap())
          .containsEntry("content-type", List.of("application/json; charset=utf-8"));
    }
  }

  @Test
  void testGETFailure() throws IOException {
    try (TestEnvironment env = TestEnvironment.builder().createDefaultExpectations(false).build()) {

      HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.GET, env.discoveryEndpoint());
      httpRequest.setHeader("Accept", "application/json");

      RESTClientAdapter adapter = env.restClientAdapter();
      ReadOnlyHTTPResponse response = adapter.send(httpRequest);

      assertThat(response.getStatusCode()).isEqualTo(404);
      assertThat(response.getBody()).isNull();
      assertThat(response.getHeaderMap()).containsEntry("content-length", List.of("0"));
    }
  }

  @Test
  void testPOSTSuccess() throws IOException {
    try (TestEnvironment env =
        TestEnvironment.builder()
            .clientAuthenticationMethod(ClientAuthenticationMethod.CLIENT_SECRET_POST)
            .build()) {

      HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.POST, env.tokenEndpoint());
      Map<String, String> formData =
          ImmutableMap.<String, String>builder()
              .put("grant_type", "client_credentials")
              .put("client_id", TestEnvironment.CLIENT_ID1.getValue())
              .put("client_secret", TestEnvironment.CLIENT_SECRET1.getValue())
              .put("scope", TestEnvironment.SCOPE1.toString())
              .put("extra1", "value1")
              .build();
      httpRequest.setBody(RESTUtil.encodeFormData(formData));
      httpRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
      httpRequest.setHeader("Accept", "application/json");

      RESTClientAdapter adapter = env.restClientAdapter();
      ReadOnlyHTTPResponse response = adapter.send(httpRequest);

      assertThat(response.getStatusCode()).isEqualTo(HTTPResponse.SC_OK);
      assertThat(response.getBody()).contains("\"access_token\" : \"access_initial\"");
      assertThat(response.getHeaderMap())
          .containsEntry("content-type", List.of("application/json; charset=utf-8"));
    }
  }

  @Test
  void testPOSTFailure() throws IOException {
    try (TestEnvironment env = TestEnvironment.builder().build()) {

      HTTPRequest httpRequest = new HTTPRequest(HTTPRequest.Method.POST, env.tokenEndpoint());
      httpRequest.setBody("grant_type=invalid");
      httpRequest.setHeader("Content-Type", "application/x-www-form-urlencoded");
      httpRequest.setHeader("Accept", "application/json");

      RESTClientAdapter adapter = env.restClientAdapter();
      ReadOnlyHTTPResponse response = adapter.send(httpRequest);

      assertThat(response.getStatusCode()).isEqualTo(400);
      assertThat(response.getBody()).contains("\"error\"").contains("\"error_description\"");
      assertThat(response.getHeaderMap())
          .containsEntry("content-type", List.of("application/json"));
    }
  }
}
