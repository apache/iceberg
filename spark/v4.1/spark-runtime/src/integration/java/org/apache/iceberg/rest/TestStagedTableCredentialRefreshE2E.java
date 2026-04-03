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
import static org.assertj.core.api.Assumptions.assumeThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.aws.AwsClientProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.credentials.Credential;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.responses.ImmutableLoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadCredentialsResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.model.AssumeRoleRequest;
import software.amazon.awssdk.services.sts.model.AssumeRoleResponse;
import software.amazon.awssdk.services.sts.model.Credentials;

/**
 * Integration test validating Spark CTAS with STS-vended credentials and storage-refresh-token
 * credential refresh.
 *
 * <p>Requires env vars: AWS_TEST_ACCESS_KEY_ID, AWS_TEST_SECRET_ACCESS_KEY, AWS_TEST_ROLE_ARN,
 * S3_TEST_WAREHOUSE, AWS_TEST_REGION. Skipped when absent.
 */
class TestStagedTableCredentialRefreshE2E {

  private static final String ACCESS_KEY = System.getenv("AWS_TEST_ACCESS_KEY_ID");
  private static final String SECRET_KEY = System.getenv("AWS_TEST_SECRET_ACCESS_KEY");
  private static final String ROLE_ARN = System.getenv("AWS_TEST_ROLE_ARN");
  private static final String WAREHOUSE = System.getenv("S3_TEST_WAREHOUSE");
  private static final String REGION = System.getenv().getOrDefault("AWS_TEST_REGION", "us-west-2");

  private static Server httpServer;
  private static CredentialVendingRESTCatalogAdapter adapter;
  private static SparkSession spark;
  private static String serverUri;
  private static S3Client s3Cleanup;

  private static Credentials assumeRoleForCleanup() {
    try (StsClient sts =
        StsClient.builder()
            .region(Region.of(REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
            .build()) {
      return sts.assumeRole(
              AssumeRoleRequest.builder()
                  .roleArn(ROLE_ARN)
                  .roleSessionName("iceberg-test-cleanup")
                  .durationSeconds(900)
                  .build())
          .credentials();
    }
  }

  @BeforeAll
  static void beforeClass() throws Exception {
    assumeThat(ACCESS_KEY != null && SECRET_KEY != null && ROLE_ARN != null && WAREHOUSE != null)
        .isTrue();

    InMemoryCatalog backendCatalog = new InMemoryCatalog();
    backendCatalog.initialize("in-memory", Map.of(CatalogProperties.WAREHOUSE_LOCATION, WAREHOUSE));

    adapter = new CredentialVendingRESTCatalogAdapter(backendCatalog);

    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(new RESTCatalogServlet(adapter)), "/*");
    context.setHandler(new GzipHandler());

    httpServer = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    httpServer.setHandler(context);
    httpServer.start();

    serverUri = httpServer.getURI().toString();
    if (serverUri.endsWith("/")) {
      serverUri = serverUri.substring(0, serverUri.length() - 1);
    }

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.sql.catalog.cred_test", SparkCatalog.class.getName())
            .config("spark.sql.catalog.cred_test.type", "rest")
            .config("spark.sql.catalog.cred_test.uri", serverUri)
            .config(
                "spark.sql.catalog.cred_test." + CatalogProperties.FILE_IO_IMPL,
                "org.apache.iceberg.aws.s3.S3FileIO")
            .config("spark.sql.catalog.cred_test.client.region", REGION)
            .config("spark.sql.catalog.cred_test.credential", "catalog:12345")
            .config("spark.sql.defaultCatalog", "cred_test")
            .getOrCreate();

    spark.sql("CREATE NAMESPACE IF NOT EXISTS default");

    Credentials cleanupCreds = assumeRoleForCleanup();
    s3Cleanup =
        S3Client.builder()
            .region(Region.of(REGION))
            .credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsSessionCredentials.create(
                        cleanupCreds.accessKeyId(),
                        cleanupCreds.secretAccessKey(),
                        cleanupCreds.sessionToken())))
            .build();
  }

  @AfterAll
  static void afterClass() throws Exception {
    if (spark != null) {
      spark.sql("DROP TABLE IF EXISTS cred_test.default.ctas_test");
      spark.sql("DROP TABLE IF EXISTS cred_test.default.refresh_test");
      spark.close();
    }

    if (httpServer != null) {
      httpServer.stop();
    }

    if (s3Cleanup != null) {
      s3Cleanup.close();
    }
  }

  @AfterEach
  void cleanupS3() {
    if (s3Cleanup == null || WAREHOUSE == null) {
      return;
    }

    URI warehouseUri = URI.create(WAREHOUSE);
    String bucket = warehouseUri.getHost();
    String prefix = warehouseUri.getPath();
    if (prefix.startsWith("/")) {
      prefix = prefix.substring(1);
    }

    ListObjectsV2Response listing =
        s3Cleanup.listObjectsV2(
            ListObjectsV2Request.builder().bucket(bucket).prefix(prefix).build());
    for (S3Object obj : listing.contents()) {
      s3Cleanup.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(obj.key()).build());
    }
  }

  @Test
  void ctasWithVendedCredentialsWritesAndReads() {
    spark.sql(
        "CREATE TABLE cred_test.default.ctas_test USING iceberg "
            + "AS SELECT 1 AS id, 'hello' AS data");

    Dataset<Row> result = spark.sql("SELECT * FROM cred_test.default.ctas_test");
    assertThat(result.count()).isEqualTo(1);
    assertThat(result.first().getAs("data").toString()).isEqualTo("hello");

    assertThat(adapter.stagedTables()).isNotEmpty();
  }

  @Test
  void credentialRefreshTriggeredByExpiredCredentials() {
    adapter.enableDelayedResponse(true);

    spark.sql(
        "CREATE TABLE cred_test.default.refresh_test USING iceberg "
            + "AS SELECT 1 AS id, 'refresh' AS data");

    assertThat(adapter.refreshCallCount())
        .as("Credential refresh should have been triggered")
        .isGreaterThanOrEqualTo(1);

    Dataset<Row> result = spark.sql("SELECT * FROM cred_test.default.refresh_test");
    assertThat(result.count()).isEqualTo(1);

    adapter.enableDelayedResponse(false);
  }

  /**
   * Adapter that vends real STS credentials for staged table creation and handles credential
   * refresh via the storageRefreshToken protocol.
   */
  static class CredentialVendingRESTCatalogAdapter extends RESTCatalogAdapter {
    private final ConcurrentMap<String, StagedTableContext> stagedTables = Maps.newConcurrentMap();
    private final AtomicInteger refreshCount = new AtomicInteger(0);
    private volatile boolean delayEnabled = false;

    record StagedTableContext(TableIdentifier ident, String location) {}

    CredentialVendingRESTCatalogAdapter(InMemoryCatalog catalog) {
      super(catalog);
    }

    void enableDelayedResponse(boolean enabled) {
      this.delayEnabled = enabled;
    }

    Map<String, StagedTableContext> stagedTables() {
      return stagedTables;
    }

    int refreshCallCount() {
      return refreshCount.get();
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends RESTResponse> T handleRequest(
        Route route,
        Map<String, String> vars,
        HTTPRequest httpRequest,
        Class<T> responseType,
        Consumer<Map<String, String>> responseHeaders) {
      T response = super.handleRequest(route, vars, httpRequest, responseType, responseHeaders);

      if (route == Route.CREATE_TABLE && response instanceof LoadTableResponse tableResponse) {
        Object body = httpRequest.body();
        CreateTableRequest createReq = (CreateTableRequest) body;
        if (createReq.stageCreate()) {
          if (delayEnabled) {
            try {
              TimeUnit.MILLISECONDS.sleep(1000);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
          }

          String token = UUID.randomUUID().toString();
          String location = tableResponse.tableMetadata().location();
          Namespace ns = RESTUtil.decodeNamespace(vars.get("namespace"), "%2E");
          TableIdentifier ident = TableIdentifier.of(ns, createReq.name());
          stagedTables.put(token, new StagedTableContext(ident, location));

          Credential cred = vendStsCredential(location, token);

          String credEndpoint =
              String.format(
                  "v1/namespaces/%s/tables/%s/credentials",
                  RESTUtil.encodeNamespace(ns), createReq.name());

          return (T)
              LoadTableResponse.builder()
                  .withTableMetadata(tableResponse.tableMetadata())
                  .addAllConfig(tableResponse.config())
                  .addConfig(AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT, credEndpoint)
                  .addConfig(CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.aws.s3.S3FileIO")
                  .addConfig("client.region", REGION)
                  .addCredential(cred)
                  .build();
        }
      }

      return response;
    }

    @Override
    protected LoadCredentialsResponse handleLoadCredentials(Map<String, String> vars) {
      String storageRefreshToken = vars.get("storageRefreshToken");
      if (storageRefreshToken != null) {
        refreshCount.incrementAndGet();
        StagedTableContext ctx = stagedTables.get(storageRefreshToken);
        if (ctx != null) {
          String rotatedToken = UUID.randomUUID().toString();
          stagedTables.remove(storageRefreshToken);
          stagedTables.put(rotatedToken, ctx);
          return ImmutableLoadCredentialsResponse.builder()
              .addCredentials(vendStsCredential(ctx.location(), rotatedToken))
              .build();
        }
      }

      return ImmutableLoadCredentialsResponse.builder().build();
    }

    private Credential vendStsCredential(String prefix, String token) {
      Credentials stsCreds = assumeRole();
      long expiresAtMs;
      if (delayEnabled) {
        expiresAtMs = Instant.now().plus(500, ChronoUnit.MILLIS).toEpochMilli();
      } else {
        expiresAtMs = stsCreds.expiration().toEpochMilli();
      }

      return ImmutableCredential.builder()
          .prefix(prefix)
          .putConfig(S3FileIOProperties.ACCESS_KEY_ID, stsCreds.accessKeyId())
          .putConfig(S3FileIOProperties.SECRET_ACCESS_KEY, stsCreds.secretAccessKey())
          .putConfig(S3FileIOProperties.SESSION_TOKEN, stsCreds.sessionToken())
          .putConfig(S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS, Long.toString(expiresAtMs))
          .storageRefreshToken(token)
          .build();
    }

    private Credentials assumeRole() {
      try (StsClient sts =
          StsClient.builder()
              .region(Region.of(REGION))
              .credentialsProvider(
                  StaticCredentialsProvider.create(
                      AwsBasicCredentials.create(ACCESS_KEY, SECRET_KEY)))
              .build()) {
        AssumeRoleResponse resp =
            sts.assumeRole(
                AssumeRoleRequest.builder()
                    .roleArn(ROLE_ARN)
                    .roleSessionName("iceberg-cred-refresh-test-" + UUID.randomUUID())
                    .durationSeconds(900)
                    .build());
        return resp.credentials();
      }
    }
  }
}
