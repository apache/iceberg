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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestContext {

  private static final Logger LOG = LoggerFactory.getLogger(TestContext.class);

  private static volatile TestContext instance;

  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static final int CONNECT_PORT = 8083;

  private static final int MINIO_PORT = 9000;
  private static final int CATALOG_PORT = 8181;
  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final String AWS_ACCESS_KEY = "minioadmin";
  private static final String AWS_SECRET_KEY = "minioadmin";
  private static final String AWS_REGION = "us-east-1";

  private static final File COMPOSE_FILE = new File("./docker/docker-compose.yml");
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(2);

  public static synchronized TestContext instance() {
    if (instance == null) {
      instance = new TestContext();
    }
    return instance;
  }

  private TestContext() {
    ContainerRuntimeUtil.RuntimeType runtime = ContainerRuntimeUtil.detectRuntime();
    ContainerRuntimeUtil.ComposeType compose = ContainerRuntimeUtil.detectCompose();

    if (runtime == ContainerRuntimeUtil.RuntimeType.PODMAN) {
      ContainerRuntimeUtil.configurePodman();
    }

    if (compose == ContainerRuntimeUtil.ComposeType.PODMAN_COMPOSE) {
      startWithPodmanCompose();
    } else {
      startWithComposeContainer();
    }
  }

  private void startWithComposeContainer() {
    LOG.info("Starting compose environment using Testcontainers ComposeContainer");
    ComposeContainer container =
        new ComposeContainer(COMPOSE_FILE)
            .withStartupTimeout(STARTUP_TIMEOUT)
            .waitingFor("connect", Wait.forHttp("/connectors"));
    container.start();
  }

  private void startWithPodmanCompose() {
    LOG.info("Starting compose environment using podman compose");
    List<String> composeBase = podmanComposeBaseCommand();

    try {
      List<String> upCommand = Lists.newArrayList(composeBase);
      upCommand.add("-f");
      upCommand.add(COMPOSE_FILE.getAbsolutePath());
      upCommand.add("up");
      upCommand.add("-d");

      ProcessBuilder upBuilder = new ProcessBuilder(upCommand);
      upBuilder.redirectErrorStream(true);
      Process upProcess = upBuilder.start();
      try (BufferedReader reader =
          new BufferedReader(
              new InputStreamReader(upProcess.getInputStream(), StandardCharsets.UTF_8))) {
        reader.lines().forEach(line -> LOG.info("compose: {}", line));
      }

      if (!upProcess.waitFor(STARTUP_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
          || upProcess.exitValue() != 0) {
        throw new RuntimeException(
            "Failed to start compose environment, exit code: " + upProcess.exitValue());
      }

      waitForConnectService();

      // Register shutdown hook for cleanup
      Runtime.getRuntime()
          .addShutdownHook(
              new Thread(
                  () -> {
                    try {
                      LOG.info("Stopping compose environment");
                      List<String> downCommand = Lists.newArrayList(composeBase);
                      downCommand.add("-f");
                      downCommand.add(COMPOSE_FILE.getAbsolutePath());
                      downCommand.add("down");
                      downCommand.add("-v");

                      ProcessBuilder downBuilder = new ProcessBuilder(downCommand);
                      downBuilder.redirectErrorStream(true);
                      Process downProcess = downBuilder.start();
                      downProcess.waitFor(60, TimeUnit.SECONDS);
                    } catch (Exception e) {
                      LOG.warn("Error stopping compose environment", e);
                    }
                  }));
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException("Failed to start compose environment", e);
    }
  }

  @SuppressWarnings("JavaUtilDate")
  private void waitForConnectService() {
    long deadline = System.currentTimeMillis() + STARTUP_TIMEOUT.toMillis();
    String connectUrl = "http://localhost:" + CONNECT_PORT + "/connectors";

    LOG.info("Waiting for Kafka Connect to be ready at {}", connectUrl);
    while (System.currentTimeMillis() < deadline) {
      try {
        HttpURLConnection conn = (HttpURLConnection) new URL(connectUrl).openConnection();
        conn.setConnectTimeout(1000);
        conn.setReadTimeout(1000);
        conn.setRequestMethod("GET");
        int responseCode = conn.getResponseCode();
        conn.disconnect();
        if (responseCode == 200) {
          LOG.info("Kafka Connect is ready");
          return;
        }
      } catch (Exception e) {
        // service not ready yet
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Interrupted while waiting for connect service", e);
      }
    }

    throw new RuntimeException(
        "Connect service did not become ready within " + STARTUP_TIMEOUT.toSeconds() + " seconds");
  }

  private static List<String> podmanComposeBaseCommand() {
    // Prefer "podman compose" (built-in subcommand) over standalone "podman-compose"
    try {
      ProcessBuilder pb = new ProcessBuilder("podman", "compose", "version");
      pb.redirectErrorStream(true);
      Process process = pb.start();
      process.getInputStream().readAllBytes();
      if (process.waitFor(10, TimeUnit.SECONDS) && process.exitValue() == 0) {
        List<String> cmd = Lists.newArrayList();
        cmd.add("podman");
        cmd.add("compose");
        return cmd;
      }
    } catch (Exception e) {
      // fall through
    }

    List<String> cmd = Lists.newArrayList();
    cmd.add("podman-compose");
    return cmd;
  }

  public void startConnector(KafkaConnectUtils.Config config) {
    KafkaConnectUtils.startConnector(config);
    KafkaConnectUtils.ensureConnectorRunning(config.getName());
  }

  public void stopConnector(String name) {
    KafkaConnectUtils.stopConnector(name);
  }

  public Catalog initLocalCatalog() {
    String localCatalogUri = "http://localhost:" + CATALOG_PORT;
    RESTCatalog result = new RESTCatalog();
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, localCatalogUri)
            .put(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
            .put("s3.endpoint", "http://localhost:" + MINIO_PORT)
            .put("s3.access-key-id", AWS_ACCESS_KEY)
            .put("s3.secret-access-key", AWS_SECRET_KEY)
            .put("s3.path-style-access", "true")
            .put("client.region", AWS_REGION)
            .build());
    return result;
  }

  public Map<String, Object> connectorCatalogProperties() {
    return ImmutableMap.<String, Object>builder()
        .put(
            "iceberg.catalog." + CatalogUtil.ICEBERG_CATALOG_TYPE,
            CatalogUtil.ICEBERG_CATALOG_TYPE_REST)
        .put("iceberg.catalog." + CatalogProperties.URI, "http://iceberg:" + CATALOG_PORT)
        .put(
            "iceberg.catalog." + CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.aws.s3.S3FileIO")
        .put("iceberg.catalog.s3.endpoint", "http://minio:" + MINIO_PORT)
        .put("iceberg.catalog.s3.access-key-id", AWS_ACCESS_KEY)
        .put("iceberg.catalog.s3.secret-access-key", AWS_SECRET_KEY)
        .put("iceberg.catalog.s3.path-style-access", true)
        .put("iceberg.catalog.client.region", AWS_REGION)
        .build();
  }

  public KafkaProducer<String, String> initLocalProducer() {
    return new KafkaProducer<>(
        ImmutableMap.of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            BOOTSTRAP_SERVERS,
            ProducerConfig.CLIENT_ID_CONFIG,
            UUID.randomUUID().toString()),
        new StringSerializer(),
        new StringSerializer());
  }

  public Admin initLocalAdmin() {
    return Admin.create(
        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS));
  }
}
