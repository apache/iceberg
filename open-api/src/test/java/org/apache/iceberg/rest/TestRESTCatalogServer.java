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

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

public class TestRESTCatalogServer {
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  @Test
  public void serversOnFreePortBindDistinctPorts() throws Exception {
    RESTCatalogServer first = startOnFreePort();
    RESTCatalogServer second = startOnFreePort();
    try {
      assertThat(first.port()).isPositive();
      assertThat(second.port()).isPositive().isNotEqualTo(first.port());
      assertThat(configStatus(first.port())).isEqualTo(200);
      assertThat(configStatus(second.port())).isEqualTo(200);
    } finally {
      first.stop();
      second.stop();
    }
  }

  private static RESTCatalogServer startOnFreePort() throws Exception {
    Map<String, String> config = Maps.newHashMap();
    config.put(RESTCatalogServer.REST_PORT, RESTServerExtension.FREE_PORT);
    RESTCatalogServer server = new RESTCatalogServer(config);
    server.start(false);
    return server;
  }

  private static int configStatus(int port) throws Exception {
    HttpRequest request =
        HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/config")).build();
    return HTTP.send(request, HttpResponse.BodyHandlers.discarding()).statusCode();
  }
}
