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

import static io.tabular.iceberg.connect.TestConstants.AWS_ACCESS_KEY;
import static io.tabular.iceberg.connect.TestConstants.AWS_REGION;
import static io.tabular.iceberg.connect.TestConstants.AWS_SECRET_KEY;
import static io.tabular.iceberg.connect.TestConstants.BUCKET;
import static io.tabular.iceberg.connect.TestContextUtil.MINIO_PORT;
import static io.tabular.iceberg.connect.TestContextUtil.NESSIE_CATALOG_PORT;
import static io.tabular.iceberg.connect.TestContextUtil.REST_CATALOG_PORT;
import static io.tabular.iceberg.connect.TestContextUtil.initLocalS3Client;

import java.util.stream.Stream;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.rest.RESTCatalog;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import software.amazon.awssdk.services.s3.S3Client;

@SuppressWarnings("rawtypes")
public class TestContext {

  public static final TestContext INSTANCE = new TestContext();

  private final Network network;
  private final KafkaContainer kafka;
  private final KafkaConnectContainer kafkaConnect;
  private final GenericContainer restCatalog;
  private final GenericContainer nessieCatalog;
  private final GenericContainer minio;

  private TestContext() {
    network = Network.newNetwork();

    minio = TestContextUtil.minioContainer(network);

    restCatalog = TestContextUtil.restCatalogContainer(network, minio);

    nessieCatalog = TestContextUtil.nessieCatalogContainer(network, minio);

    kafka = TestContextUtil.kafkaContainer(network);

    kafkaConnect =
        TestContextUtil.kafkaConnectContainer(network, restCatalog, nessieCatalog, kafka);

    Startables.deepStart(Stream.of(minio, restCatalog, nessieCatalog, kafka, kafkaConnect)).join();

    try (S3Client s3 = initLocalS3Client(localMinioPort())) {
      s3.createBucket(req -> req.bucket(BUCKET));
    }

    Runtime.getRuntime().addShutdownHook(new Thread(this::shutdown));
  }

  private void shutdown() {
    kafkaConnect.close();
    kafka.close();
    restCatalog.close();
    nessieCatalog.close();
    minio.close();
    network.close();
  }

  protected int localMinioPort() {
    return minio.getMappedPort(MINIO_PORT);
  }

  protected String kafkaBootstrapServers() {
    return kafka.getBootstrapServers();
  }

  protected void startKafkaConnector(KafkaConnectContainer.Config config) {
    kafkaConnect.startConnector(config);
    kafkaConnect.ensureConnectorRunning(config.getName());
  }

  protected void stopKafkaConnector(String name) {
    kafkaConnect.stopConnector(name);
  }

  protected Catalog initRestCatalog() {
    String localCatalogUri = "http://localhost:" + restCatalog.getMappedPort(REST_CATALOG_PORT);
    RESTCatalog result = new RESTCatalog();
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, localCatalogUri)
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put(S3FileIOProperties.ENDPOINT, "http://localhost:" + localMinioPort())
            .put(S3FileIOProperties.ACCESS_KEY_ID, AWS_ACCESS_KEY)
            .put(S3FileIOProperties.SECRET_ACCESS_KEY, AWS_SECRET_KEY)
            .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
            .put(AwsClientProperties.CLIENT_REGION, AWS_REGION)
            .build());
    return result;
  }

  protected Catalog initNessieCatalog() {
    String localCatalogUri =
        "http://localhost:" + nessieCatalog.getMappedPort(NESSIE_CATALOG_PORT) + "/api/v1";
    NessieCatalog result = new NessieCatalog();
    result.initialize(
        "local",
        ImmutableMap.<String, String>builder()
            .put(CatalogProperties.URI, localCatalogUri)
            .put(CatalogProperties.WAREHOUSE_LOCATION, "s3://" + BUCKET + "/warehouse")
            .put(CatalogProperties.FILE_IO_IMPL, S3FileIO.class.getName())
            .put(S3FileIOProperties.ENDPOINT, "http://localhost:" + localMinioPort())
            .put(S3FileIOProperties.ACCESS_KEY_ID, AWS_ACCESS_KEY)
            .put(S3FileIOProperties.SECRET_ACCESS_KEY, AWS_SECRET_KEY)
            .put(S3FileIOProperties.PATH_STYLE_ACCESS, "true")
            .put(AwsClientProperties.CLIENT_REGION, AWS_REGION)
            .build());
    return result;
  }
}
