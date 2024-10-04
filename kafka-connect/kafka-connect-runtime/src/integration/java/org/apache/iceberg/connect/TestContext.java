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
import java.io.File;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.ComposeContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class TestContext {

  private static volatile TestContext instance;

  public static final ObjectMapper MAPPER = new ObjectMapper();
  public static final int CONNECT_PORT = 8083;

  private static final int MINIO_PORT = 9000;
  private static final int CATALOG_PORT = 8181;
  private static final String BOOTSTRAP_SERVERS = "localhost:29092";
  private static final String AWS_ACCESS_KEY = "minioadmin";
  private static final String AWS_SECRET_KEY = "minioadmin";
  private static final String AWS_REGION = "us-east-1";

  public static synchronized TestContext instance() {
    if (instance == null) {
      instance = new TestContext();
    }
    return instance;
  }

  private TestContext() {
    ComposeContainer container =
        new ComposeContainer(new File("./docker/docker-compose.yml"))
            .withStartupTimeout(Duration.ofMinutes(2))
            .waitingFor("connect", Wait.forHttp("/connectors"));
    container.start();
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
