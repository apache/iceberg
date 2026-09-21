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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignBatchRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignBatchResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.eclipse.jetty.ee10.servlet.ServletContextHandler;
import org.eclipse.jetty.ee10.servlet.ServletHolder;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class TestRemoteSigningClient {

  private static final String SIGN_ENDPOINT = "v1/namespaces/ns/tables/t/sign";
  private static final String LOCATION = "s3://bucket/data/part-0.parquet";

  private static Server server;
  private static SigningServlet servlet;

  @BeforeAll
  public static void startServer() throws Exception {
    servlet = new SigningServlet();
    ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
    context.addServlet(new ServletHolder(servlet), "/*");
    server = new Server(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
    server.setHandler(context);
    server.start();
  }

  @AfterAll
  public static void stopServer() throws Exception {
    server.stop();
  }

  @BeforeEach
  public void resetServlet() {
    servlet.requiredHeader = null;
    servlet.unsupported = false;
    servlet.lastRequest = null;
    servlet.lastBatch = null;
    servlet.lastHeaders = null;
    servlet.failLocation = null;
    servlet.resetSignCounters();
  }

  @Test
  public void testPreSign() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .put("header." + RemoteSigningClient.ACCESS_DELEGATION_HEADER, "vended-credentials")
            .build();

    try (RemoteSigningClient access = RemoteSigningClient.create(properties)) {
      URI url = access.preSign(LOCATION, request()).uri();

      assertThat(url)
          .isEqualTo(
              URI.create("https://bucket.s3.amazonaws.com/data/part-0.parquet?X-Test-Signature=1"));
      assertThat(servlet.lastHeaders.get(RemoteSigningClient.ACCESS_DELEGATION_HEADER))
          .isEqualTo(RemoteSigningClient.PRESIGNED_URLS);
      assertThat(servlet.lastRequest.uri()).isEqualTo(URI.create(LOCATION));
      assertThat(servlet.lastRequest.method()).isEqualTo("GET");
      assertThat(servlet.lastRequest.region()).isEqualTo("us-east-1");
      assertThat(servlet.lastRequest.provider()).isEqualTo("s3");
      assertThat(servlet.lastRequest.headers()).isEmpty();
    }
  }

  @Test
  public void testCredential() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .put(OAuth2Properties.CREDENTIAL, "client:secret")
            .put(
                OAuth2Properties.OAUTH2_SERVER_URI,
                RESTUtil.resolveEndpoint(serverUri(), ResourcePaths.tokens()))
            .build();

    try (RemoteSigningClient access = RemoteSigningClient.create(properties)) {
      access.preSign(LOCATION, request());

      assertThat(servlet.lastHeaders.get("Authorization"))
          .isEqualTo("Bearer client-credentials-token:sub=client");
    }
  }

  @Test
  public void testBatchSigningIsOneRoundTrip() {
    List<String> locations = locations(5);

    try (RemoteSigningClient access = RemoteSigningClient.create(batchProperties(100))) {
      Map<String, RemoteSignResponse> urls =
          access.preSign(locations, TestRemoteSigningClient::request);

      assertThat(urls).containsOnlyKeys(locations.toArray(new String[0]));
      assertThat(servlet.signRoundTrips()).isEqualTo(1);
      assertThat(servlet.signedLocations()).isEqualTo(5);
      assertThat(urls.get(locations.get(3)).uri().toString()).contains("part-3.parquet");
    }
  }

  @Test
  public void testBatchIsChunkedAtTheServerMaximum() {
    try (RemoteSigningClient access = RemoteSigningClient.create(batchProperties(2))) {
      access.preSign(locations(5), TestRemoteSigningClient::request);

      assertThat(servlet.signRoundTrips()).isEqualTo(3);
      assertThat(servlet.signedLocations()).isEqualTo(5);
    }
  }

  @Test
  public void testSigningFallsBackWhenTheBatchEndpointIsAbsent() {
    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      access.preSign(locations(5), TestRemoteSigningClient::request);

      assertThat(servlet.signRoundTrips()).isEqualTo(5);
      assertThat(servlet.signedLocations()).isEqualTo(5);
    }
  }

  @Test
  public void testFailedBatchElementIsRaised() {
    servlet.failLocation = locations(5).get(2);

    try (RemoteSigningClient access = RemoteSigningClient.create(batchProperties(100))) {
      assertThatThrownBy(() -> access.preSign(locations(5), TestRemoteSigningClient::request))
          .isInstanceOf(ForbiddenException.class)
          .hasMessageContaining("not authorized");
    }
  }

  @Test
  public void testRepeatedLocationsAreSignedOnce() {
    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      Map<String, RemoteSignResponse> urls =
          access.preSign(List.of(LOCATION, LOCATION), location -> request());

      assertThat(urls).containsOnlyKeys(LOCATION);
      assertThat(servlet.signedLocations()).isEqualTo(1);
    }
  }

  @Test
  public void testRepeatedLocationsAreOneBatchElement() {
    try (RemoteSigningClient access = RemoteSigningClient.create(batchProperties(100))) {
      Map<String, RemoteSignResponse> urls =
          access.preSign(List.of(LOCATION, LOCATION), location -> request());

      assertThat(urls).containsOnlyKeys(LOCATION);
      assertThat(servlet.signedLocations()).isEqualTo(1);
    }
  }

  @Test
  public void testRemoteSigningConfigIsForwarded() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder()
            .putProperties("tenant", "t1")
            .putHeaders("X-Signing-Tenant", List.of("t1", "t2"))
            .build();
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .put(
                RESTCatalogProperties.REMOTE_SIGNING_CONFIG,
                RemoteSigningConfigParser.toJson(config))
            .build();

    try (RemoteSigningClient access = RemoteSigningClient.create(properties)) {
      access.preSign(LOCATION, request());

      assertThat(servlet.lastRequest.properties()).containsEntry("tenant", "t1");
      assertThat(servlet.lastHeaders.get("X-Signing-Tenant")).isEqualTo("t1, t2");
    }
  }

  @Test
  public void testRemoteSigningConfigIsForwardedOnceForABatch() {
    RemoteSigningConfig config =
        ImmutableRemoteSigningConfig.builder().putProperties("tenant", "t1").build();
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(batchProperties(100))
            .put(
                RESTCatalogProperties.REMOTE_SIGNING_CONFIG,
                RemoteSigningConfigParser.toJson(config))
            .build();

    try (RemoteSigningClient access = RemoteSigningClient.create(properties)) {
      access.preSign(locations(3), TestRemoteSigningClient::request);

      assertThat(servlet.lastBatch.properties()).containsEntry("tenant", "t1");
      assertThat(servlet.lastBatch.requests())
          .allSatisfy(r -> assertThat(r.properties()).isEmpty());
      assertThat(servlet.lastRequest.properties()).containsEntry("tenant", "t1");
    }
  }

  @Test
  public void testRequiredHeadersAreReturned() {
    servlet.requiredHeader = "x-amz-request-payer";

    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      RemoteSignResponse response = access.preSign(LOCATION, request());

      assertThat(response.uri()).isNotNull();
      assertThat(response.headers()).containsKey("x-amz-request-payer");
    }
  }

  @Test
  public void testUnsupportedModeIsReported() {
    servlet.unsupported = true;

    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      assertThatThrownBy(() -> access.preSign(LOCATION, request()))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("does not support presigned-urls");
    }
  }

  @Test
  public void testSigningEndpointIsRequired() {
    assertThatThrownBy(
            () -> RemoteSigningClient.create(ImmutableMap.of(CatalogProperties.URI, serverUri())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the remote signing endpoint");
    assertThatThrownBy(
            () ->
                RemoteSigningClient.create(
                    ImmutableMap.of(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT, SIGN_ENDPOINT)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("requires the REST catalog URI");
  }

  @Test
  public void testResolvingFileIORejectsDelegatesWithoutPreSigning() {
    try (ResolvingFileIO io = new ResolvingFileIO()) {
      io.setConf(new Configuration());
      io.initialize(properties());

      assertThatThrownBy(() -> io.preSign(List.of("file:///tmp/data/part-0.parquet")))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("does not support pre-signing");
    }
  }

  private static Map<String, String> properties() {
    return ImmutableMap.of(
        CatalogProperties.URI,
        serverUri(),
        RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT,
        SIGN_ENDPOINT);
  }

  private static Map<String, String> batchProperties(int maxBatchSize) {
    return ImmutableMap.<String, String>builder()
        .putAll(properties())
        .put(RESTCatalogProperties.REMOTE_SIGNING_BATCH_SUPPORTED, "true")
        .put(RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE, String.valueOf(maxBatchSize))
        .build();
  }

  private static List<String> locations(int count) {
    List<String> locations = Lists.newArrayList();
    for (int i = 0; i < count; i += 1) {
      locations.add("s3://bucket/data/part-" + i + ".parquet");
    }

    return locations;
  }

  private static RemoteSignRequest request(String location) {
    return ImmutableRemoteSignRequest.builder()
        .method("GET")
        .region("us-east-1")
        .uri(URI.create(location))
        .provider("s3")
        .build();
  }

  private static String serverUri() {
    return server.getURI().toString();
  }

  private static RemoteSignRequest request() {
    return ImmutableRemoteSignRequest.builder()
        .method("GET")
        .region("us-east-1")
        .uri(URI.create(LOCATION))
        .provider("s3")
        .build();
  }

  private static class SigningServlet extends RemoteSignerServlet {
    private volatile String requiredHeader = null;
    private volatile boolean unsupported = false;
    private volatile RemoteSignRequest lastRequest = null;
    private volatile RemoteSignBatchRequest lastBatch = null;
    private volatile Map<String, String> lastHeaders = null;
    private volatile String failLocation = null;

    SigningServlet() {
      super(SIGN_ENDPOINT);
    }

    @Override
    protected void execute(HttpServletRequest request, HttpServletResponse response) {
      Map<String, String> headers = Maps.newHashMap();
      for (String name :
          List.of(
              RemoteSigningClient.ACCESS_DELEGATION_HEADER, "X-Signing-Tenant", "Authorization")) {
        headers.put(name, request.getHeader(name));
      }

      this.lastHeaders = headers;
      if (unsupported) {
        response.setStatus(406);
        response.setHeader("Content-Type", "application/json");
        try {
          RESTObjectMapper.mapper()
              .writeValue(
                  response.getWriter(),
                  ErrorResponse.builder()
                      .responseCode(406)
                      .withType("UnsupportedOperationException")
                      .withMessage("presigned-urls is not enabled")
                      .build());
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

        return;
      }

      super.execute(request, response);
    }

    @Override
    protected RemoteSignBatchResponse signBatch(RemoteSignBatchRequest batch) {
      this.lastBatch = batch;
      return super.signBatch(batch);
    }

    @Override
    protected RemoteSignResponse signRequest(RemoteSignRequest request) {
      this.lastRequest = request;
      if (request.uri().toString().equals(failLocation)) {
        throw new ForbiddenException("Caller is not authorized to sign %s", request.uri());
      }

      boolean preSignedUrl =
          RemoteSigningClient.PRESIGNED_URLS.equals(
              lastHeaders.get(RemoteSigningClient.ACCESS_DELEGATION_HEADER));
      if (preSignedUrl) {
        URI location = request.uri();
        ImmutableRemoteSignResponse.Builder response =
            ImmutableRemoteSignResponse.builder()
                .uri(
                    URI.create(
                        "https://"
                            + location.getAuthority()
                            + ".s3.amazonaws.com"
                            + location.getRawPath()
                            + "?X-Test-Signature=1"));
        if (requiredHeader != null) {
          response.putHeaders(requiredHeader, List.of("requester"));
        }

        return response.build();
      }

      return ImmutableRemoteSignResponse.builder()
          .uri(request.uri())
          .putHeaders("Authorization", List.of("AWS4-HMAC-SHA256 Credential=test"))
          .build();
    }
  }
}
