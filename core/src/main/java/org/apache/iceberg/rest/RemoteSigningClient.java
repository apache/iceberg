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
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;

/**
 * Client of the catalog's signing endpoint on behalf of a FileIO. {@link #preSign} obtains
 * pre-signed URLs: each request carries {@code X-Iceberg-Access-Delegation: presigned-urls} and
 * names the storage location to sign.
 */
public class RemoteSigningClient implements AutoCloseable {

  public static final String ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  public static final String PRESIGNED_URLS = "presigned-urls";

  private static final String SCOPE = "sign";
  private static final Consumer<ErrorResponse> ERROR_HANDLER =
      error -> {
        if (error.code() == 406) {
          throw new UnsupportedOperationException(
              "Signing endpoint does not support " + PRESIGNED_URLS + ": " + error.message());
        }

        ErrorHandlers.defaultErrorHandler().accept(error);
      };

  private final Map<String, String> properties;
  private final RemoteSigningConfig config;
  private final Map<String, String> configHeaders;
  private final String endpoint;
  private final RESTClient restClient;
  private final AuthManager authManager;

  public static RemoteSigningClient create(Map<String, String> properties) {
    return new RemoteSigningClient(properties);
  }

  private RemoteSigningClient(Map<String, String> properties) {
    String uri = properties.get(CatalogProperties.URI);
    Preconditions.checkArgument(null != uri, "Pre-signing requires the REST catalog URI");
    String endpointPath = properties.get(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT);
    Preconditions.checkArgument(
        null != endpointPath,
        "Pre-signing requires the remote signing endpoint (%s), which the REST catalog sets on table load",
        RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT);

    this.properties = properties;
    String json = properties.get(RESTCatalogProperties.REMOTE_SIGNING_CONFIG);
    this.config =
        null == json ? RemoteSigningConfig.EMPTY : RemoteSigningConfigParser.fromJson(json);
    this.configHeaders = configHeaders(config);
    this.endpoint = RESTUtil.resolveEndpoint(uri, endpointPath);
    this.restClient =
        HTTPClient.builder(properties)
            .uri(uri)
            .withHeaders(RESTUtil.configHeaders(properties))
            .build();
    this.authManager = AuthManagers.loadAuthManager("pre-sign", properties);
  }

  public Map<String, URI> preSign(
      Collection<String> locations, Function<String, RemoteSignRequest> requests) {
    ImmutableMap.Builder<String, URI> urls = ImmutableMap.builder();
    for (String location : Sets.newLinkedHashSet(locations)) {
      urls.put(location, preSign(location, requests.apply(location)));
    }

    return urls.build();
  }

  public URI preSign(String location, RemoteSignRequest request) {
    RemoteSignResponse response =
        restClient
            .withAuthSession(authSession())
            .post(
                endpoint,
                ImmutableRemoteSignRequest.builder()
                    .from(request)
                    .putAllProperties(config.properties())
                    .build(),
                RemoteSignResponse.class,
                headers(PRESIGNED_URLS),
                ERROR_HANDLER);

    if (!response.headers().isEmpty()) {
      throw new UnsupportedOperationException(
          "Signing endpoint returned signed headers instead of a pre-signed URL for " + location);
    }

    return response.uri();
  }

  private static Map<String, String> configHeaders(RemoteSigningConfig config) {
    ImmutableMap.Builder<String, String> headers = ImmutableMap.builder();
    config.headers().forEach((name, values) -> headers.put(name, String.join(", ", values)));
    return headers.build();
  }

  private Map<String, String> headers(String mode) {
    return ImmutableMap.<String, String>builder()
        .putAll(configHeaders)
        .put(ACCESS_DELEGATION_HEADER, mode)
        .buildKeepingLast();
  }

  private AuthSession authSession() {
    ImmutableMap.Builder<String, String> sessionProperties =
        ImmutableMap.<String, String>builder()
            .putAll(properties)
            .put(OAuth2Properties.SCOPE, SCOPE);

    return authManager.tableSession(restClient, sessionProperties.buildKeepingLast());
  }

  @Override
  public void close() {
    try {
      restClient.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } finally {
      authManager.close();
    }
  }
}
