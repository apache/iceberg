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
package org.apache.iceberg.connect;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.awaitility.Awaitility;

public class KafkaConnectUtils {

  private static final HttpClient HTTP = HttpClients.createDefault();

  // JavaBean-style for serialization
  public static class Config {

    private final String name;
    private final Map<String, Object> config = Maps.newHashMap();

    public Config(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public Map<String, Object> getConfig() {
      return config;
    }

    public Config config(String key, Object value) {
      config.put(key, value);
      return this;
    }
  }

  public static void startConnector(Config config) {
    try {
      HttpPost request =
          new HttpPost(
              String.format(
                  Locale.ROOT, "http://localhost:%d/connectors", TestContext.CONNECT_PORT));
      String body = TestContext.MAPPER.writeValueAsString(config);
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new StringEntity(body));
      HTTP.execute(request, response -> null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void ensureConnectorRunning(String name) {
    HttpGet request =
        new HttpGet(
            String.format(
                Locale.ROOT,
                "http://localhost:%d/connectors/%s/status",
                TestContext.CONNECT_PORT,
                name));
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .until(
            () ->
                HTTP.execute(
                    request,
                    response -> {
                      if (response.getCode() == HttpStatus.SC_OK) {
                        JsonNode root =
                            TestContext.MAPPER.readTree(response.getEntity().getContent());
                        String connectorState = root.get("connector").get("state").asText();
                        ArrayNode taskNodes = (ArrayNode) root.get("tasks");
                        List<String> taskStates = Lists.newArrayList();
                        taskNodes.forEach(node -> taskStates.add(node.get("state").asText()));
                        return "RUNNING".equals(connectorState)
                            && taskStates.stream().allMatch("RUNNING"::equals);
                      }
                      return false;
                    }));
  }

  public static void stopConnector(String name) {
    try {
      HttpDelete request =
          new HttpDelete(
              String.format(
                  Locale.ROOT,
                  "http://localhost:%d/connectors/%s",
                  TestContext.CONNECT_PORT,
                  name));
      HTTP.execute(request, response -> null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private KafkaConnectUtils() {}
}
