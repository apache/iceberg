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
package io.tabular.iceberg.connect;

import static io.tabular.iceberg.connect.TestConstants.MAPPER;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.api.client.util.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.awaitility.Awaitility;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public class KafkaConnectContainer extends GenericContainer<KafkaConnectContainer> {

  private static final HttpClient HTTP = HttpClients.createDefault();
  private static final int PORT = 8083;

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

  public KafkaConnectContainer(DockerImageName dockerImageName) {
    super(dockerImageName);
    this.withExposedPorts(PORT);
    this.withEnv("CONNECT_GROUP_ID", "kc");
    this.withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "kc_config");
    this.withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "kc_offsets");
    this.withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_STATUS_STORAGE_TOPIC", "kc_status");
    this.withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
    this.withEnv("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    this.withEnv("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
    this.withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "localhost");
    this.setWaitStrategy(
        new HttpWaitStrategy()
            .forPath("/connectors")
            .forPort(PORT)
            .withStartupTimeout(Duration.ofSeconds(30)));
  }

  public void startConnector(Config config) {
    try {
      HttpPost request =
          new HttpPost(String.format("http://localhost:%d/connectors", getMappedPort(PORT)));
      String body = MAPPER.writeValueAsString(config);
      request.setHeader("Content-Type", "application/json");
      request.setEntity(new StringEntity(body));
      HTTP.execute(request, response -> null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void ensureConnectorRunning(String name) {
    HttpGet request =
        new HttpGet(
            String.format("http://localhost:%d/connectors/%s/status", getMappedPort(PORT), name));
    Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .until(
            () ->
                HTTP.execute(
                    request,
                    response -> {
                      if (response.getCode() == HttpStatus.SC_OK) {
                        JsonNode root = MAPPER.readTree(response.getEntity().getContent());
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

  public void stopConnector(String name) {
    try {
      HttpDelete request =
          new HttpDelete(
              String.format("http://localhost:%d/connectors/%s", getMappedPort(PORT), name));
      HTTP.execute(request, response -> null);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
