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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthManagers;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.OAuth2Properties;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignBatchRequest;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.ImmutableRemoteSignResponse;
import org.apache.iceberg.rest.responses.RemoteSignBatchResponse;
import org.apache.iceberg.rest.responses.RemoteSignBatchResult;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.iceberg.util.PropertyUtil;

/** Client of the catalog's signing endpoint on behalf of a FileIO. */
public class RemoteSigningClient implements AutoCloseable {

  public static final String ACCESS_DELEGATION_HEADER = "X-Iceberg-Access-Delegation";
  public static final String PRESIGNED_URLS = "presigned-urls";

  private static final String SCOPE = "sign";
  private static final String BATCH = "/batch";
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
  private final String batchEndpoint;
  private final int maxBatchSize;
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
        "Pre-signing requires the remote signing endpoint (%s)",
        RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT);

    this.properties = properties;
    String json = properties.get(RESTCatalogProperties.REMOTE_SIGNING_CONFIG);
    this.config =
        null == json ? RemoteSigningConfig.EMPTY : RemoteSigningConfigParser.fromJson(json);
    this.configHeaders = configHeaders(config);
    this.endpoint = RESTUtil.resolveEndpoint(uri, endpointPath);
    this.batchEndpoint =
        PropertyUtil.propertyAsBoolean(
                properties,
                RESTCatalogProperties.REMOTE_SIGNING_BATCH_SUPPORTED,
                RESTCatalogProperties.REMOTE_SIGNING_BATCH_SUPPORTED_DEFAULT)
            ? RESTUtil.resolveEndpoint(uri, endpointPath + BATCH)
            : null;
    this.maxBatchSize =
        PropertyUtil.propertyAsInt(
            properties,
            RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE,
            RESTCatalogProperties.REMOTE_SIGNING_BATCH_MAX_SIZE_DEFAULT);
    Preconditions.checkArgument(maxBatchSize > 0, "Invalid batch size: %s", maxBatchSize);
    this.restClient =
        HTTPClient.builder(properties)
            .uri(uri)
            .withHeaders(RESTUtil.configHeaders(properties))
            .build();
    this.authManager = AuthManagers.loadAuthManager("pre-sign", properties);
  }

  public Map<String, RemoteSignResponse> preSign(
      Collection<String> locations, Function<String, RemoteSignRequest> requests) {
    List<String> distinct = Lists.newArrayList(Sets.newLinkedHashSet(locations));
    ImmutableMap.Builder<String, RemoteSignResponse> urls = ImmutableMap.builder();
    if (null == batchEndpoint) {
      for (String location : distinct) {
        urls.put(location, preSign(location, requests.apply(location)));
      }

      return urls.build();
    }

    for (int start = 0; start < distinct.size(); start += maxBatchSize) {
      List<String> batch = distinct.subList(start, Math.min(start + maxBatchSize, distinct.size()));
      urls.putAll(preSignBatch(batch, requests));
    }

    return urls.build();
  }

  private Map<String, RemoteSignResponse> preSignBatch(
      List<String> locations, Function<String, RemoteSignRequest> requests) {
    ImmutableRemoteSignBatchRequest.Builder batch =
        ImmutableRemoteSignBatchRequest.builder().putAllProperties(config.properties());
    for (String location : locations) {
      batch.addRequests(requests.apply(location));
    }

    RemoteSignBatchResponse response =
        restClient
            .withAuthSession(authSession())
            .post(
                batchEndpoint,
                batch.build(),
                RemoteSignBatchResponse.class,
                headers(PRESIGNED_URLS),
                ERROR_HANDLER);

    List<RemoteSignBatchResult> results = response.results();
    Preconditions.checkState(
        results.size() == locations.size(),
        "Invalid batch signing response: %s results for %s requests",
        results.size(),
        locations.size());

    ImmutableMap.Builder<String, RemoteSignResponse> urls = ImmutableMap.builder();
    for (int pos = 0; pos < results.size(); pos += 1) {
      urls.put(locations.get(pos), result(locations.get(pos), results.get(pos)));
    }

    return urls.build();
  }

  private RemoteSignResponse result(String location, RemoteSignBatchResult result) {
    switch (result.status()) {
      case COMPLETED:
        return ImmutableRemoteSignResponse.builder()
            .uri(result.uri())
            .headers(result.headers())
            .build();

      case FAILED:
        ERROR_HANDLER.accept(result.error());
        throw new IllegalStateException(
            "Signing failed for " + location + ": " + result.error().message());

      default:
        throw new IllegalStateException("Unknown signing status: " + result.status());
    }
  }

  public RemoteSignResponse preSign(String location, RemoteSignRequest request) {
    return restClient
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
