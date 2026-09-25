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
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
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
    servlet.declineSelection = false;
    servlet.unsupported = false;
    servlet.lastRequest = null;
    servlet.lastHeaders = null;
    servlet.requests = 0;
  }

  @Test
  public void testPreSign() {
    Map<String, String> properties =
        ImmutableMap.<String, String>builder()
            .putAll(properties())
            .put("header." + RemoteSigningClient.ACCESS_DELEGATION_HEADER, "vended-credentials")
            .build();

    try (RemoteSigningClient access = RemoteSigningClient.create(properties)) {
      URI url = access.preSign(LOCATION, request());

      assertThat(url)
          .isEqualTo(
              URI.create("https://bucket.s3.amazonaws.com/data/part-0.parquet?X-Test-Signature=1"));
      assertThat(servlet.lastHeaders.get(RemoteSigningClient.ACCESS_DELEGATION_HEADER))
          .isEqualTo(RemoteSigningClient.PRESIGNED_URLS);
      // the request names the location; the signer spells and signs it
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
  public void testRepeatedLocationsAreSignedOnce() {
    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      Map<String, URI> urls = access.preSign(List.of(LOCATION, LOCATION), location -> request());

      assertThat(urls).containsOnlyKeys(LOCATION);
      assertThat(servlet.requests).isEqualTo(1);
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
  public void testSignedHeadersAreRejected() {
    servlet.declineSelection = true;

    try (RemoteSigningClient access = RemoteSigningClient.create(properties())) {
      assertThatThrownBy(() -> access.preSign(LOCATION, request()))
          .isInstanceOf(UnsupportedOperationException.class)
          .hasMessageContaining("signed headers instead of a pre-signed URL");
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
    private volatile boolean declineSelection = false;
    private volatile boolean unsupported = false;
    private volatile RemoteSignRequest lastRequest = null;
    private volatile Map<String, String> lastHeaders = null;
    private volatile int requests = 0;

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
    protected RemoteSignResponse signRequest(RemoteSignRequest request) {
      this.lastRequest = request;
      this.requests += 1;
      boolean preSignedUrl =
          !declineSelection
              && RemoteSigningClient.PRESIGNED_URLS.equals(
                  lastHeaders.get(RemoteSigningClient.ACCESS_DELEGATION_HEADER));
      if (preSignedUrl) {
        URI location = request.uri();
        return ImmutableRemoteSignResponse.builder()
            .uri(
                URI.create(
                    "https://"
                        + location.getAuthority()
                        + ".s3.amazonaws.com"
                        + location.getRawPath()
                        + "?X-Test-Signature=1"))
            .build();
      }

      return ImmutableRemoteSignResponse.builder()
          .uri(request.uri())
          .putHeaders("Authorization", List.of("AWS4-HMAC-SHA256 Credential=test"))
          .build();
    }
  }
}
