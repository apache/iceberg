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
import java.util.Map;
import java.util.function.Consumer;
import org.apache.hc.core5.http.Method;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestRESTClientBuilder {

  @Test
  public void testBuildHttpClient() {
    RESTClient client = RESTClient.buildFrom(ImmutableMap.of()).build();
    Assertions.assertThat(client).isInstanceOf(HTTPClient.class);
  }

  @Test
  public void testBuildAlternativeClient() {
    RESTClient client =
        RESTClient.buildFrom(
                ImmutableMap.of(RESTClientProperties.REST_CLIENT_IMPL, TestClient.class.getName()))
            .build();
    Assertions.assertThat(client).isInstanceOf(TestClient.class);
  }

  @Test
  public void testBuildClientBadConstructor() {
    Assertions.assertThatThrownBy(
            () ->
                RESTClient.buildFrom(
                        ImmutableMap.of(
                            RESTClientProperties.REST_CLIENT_IMPL,
                            BadConstructorClient.class.getName()))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find no-arg constructor for: " + BadConstructorClient.class.getName());
  }

  @Test
  public void testBuildClientNotSubclass() {
    Assertions.assertThatThrownBy(
            () ->
                RESTClient.buildFrom(
                        ImmutableMap.of(
                            RESTClientProperties.REST_CLIENT_IMPL,
                            NotSubclassClient.class.getName()))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot initialize new instance of: "
                + NotSubclassClient.class.getName()
                + ", not a subclass of RESTClient");
  }

  static class TestClient extends BaseRESTClient {
    @Override
    protected <T> T execute(
        Method method,
        String path,
        Map<String, String> queryParams,
        Object requestBody,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders) {
      return null;
    }

    @Override
    public void close() throws IOException {}
  }

  static class BadConstructorClient extends BaseRESTClient {

    private final String arg;

    BadConstructorClient(String arg) {
      this.arg = arg;
    }

    @Override
    protected <T> T execute(
        Method method,
        String path,
        Map<String, String> queryParams,
        Object requestBody,
        Class<T> responseType,
        Map<String, String> headers,
        Consumer<ErrorResponse> errorHandler,
        Consumer<Map<String, String>> responseHeaders) {
      return null;
    }

    @Override
    public void close() throws IOException {}
  }

  static class NotSubclassClient {}
}
