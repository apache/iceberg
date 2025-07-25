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
package org.apache.iceberg.rest.auth.oauth2.flow;

import com.nimbusds.oauth2.sdk.AbstractConfigurationRequest;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerConfigurationRequest;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerEndpointMetadata;
import com.nimbusds.oauth2.sdk.as.ReadOnlyAuthorizationServerEndpointMetadata;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderConfigurationRequest;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderEndpointMetadata;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Config;
import org.apache.iceberg.rest.auth.oauth2.OAuth2Runtime;
import org.immutables.value.Value;

/**
 * A provider for OAuth2 endpoints.
 *
 * <p>This component centralizes the logic for fetching endpoint metadata from the issuer ("metadata
 * discovery"), and provides a single point of access for all supported OAuth2 endpoints.
 */
@Value.Immutable
public abstract class EndpointProvider {

  public static EndpointProvider of(OAuth2Config config, OAuth2Runtime runtime) {
    return ImmutableEndpointProvider.builder().config(config).runtime(runtime).build();
  }

  /** The OAuth2 configuration. */
  protected abstract OAuth2Config config();

  /** The OAuth2 runtime, required for making HTTP requests. */
  protected abstract OAuth2Runtime runtime();

  /**
   * The resolved token endpoint. If a token endpoint is provided in the configuration, it is used
   * as is, otherwise it is resolved through metadata discovery on first access.
   */
  @Value.Lazy
  public URI resolvedTokenEndpoint() {
    return config()
        .basicConfig()
        .tokenEndpoint()
        .orElseGet(() -> serverMetadata().getTokenEndpointURI());
  }

  @Value.Lazy
  ReadOnlyAuthorizationServerEndpointMetadata serverMetadata() {
    URI issuerUrl =
        config()
            .basicConfig()
            .issuerUrl()
            .orElseThrow(() -> new IllegalStateException("No issuer URL configured"));
    return fetchServerMetadata(issuerUrl);
  }

  private ReadOnlyAuthorizationServerEndpointMetadata fetchServerMetadata(URI issuerUrl) {
    Issuer issuer = new Issuer(issuerUrl);
    List<Exception> failures = null;
    for (MetadataProvider provider :
        List.<MetadataProvider>of(this::oidcProvider, this::oauthProvider)) {
      try {
        return provider.fetchMetadata(issuer);
      } catch (Exception e) {
        if (failures == null) {
          failures = Lists.newArrayListWithCapacity(2);
        }

        failures.add(e);
      }
    }

    RuntimeException error =
        new RuntimeException("Failed to fetch provider metadata", failures.get(0));
    for (int i = 1; i < failures.size(); i++) {
      error.addSuppressed(failures.get(i));
    }

    throw error;
  }

  private ReadOnlyAuthorizationServerEndpointMetadata oidcProvider(Issuer issuer)
      throws IOException, ParseException {
    AbstractConfigurationRequest request = new OIDCProviderConfigurationRequest(issuer);
    HTTPResponse httpResponse = request.toHTTPRequest().send(runtime().httpClient());
    if (httpResponse.indicatesSuccess()) {
      return OIDCProviderEndpointMetadata.parse(httpResponse.getBodyAsJSONObject());
    }

    throw providerFailure("OIDC", httpResponse);
  }

  private ReadOnlyAuthorizationServerEndpointMetadata oauthProvider(Issuer issuer)
      throws IOException, ParseException {
    AbstractConfigurationRequest request = new AuthorizationServerConfigurationRequest(issuer);
    HTTPResponse httpResponse = request.toHTTPRequest().send(runtime().httpClient());
    if (httpResponse.indicatesSuccess()) {
      return AuthorizationServerEndpointMetadata.parse(httpResponse.getBodyAsJSONObject());
    }

    throw providerFailure("OAuth", httpResponse);
  }

  private static RuntimeException providerFailure(String type, HTTPResponse httpResponse) {
    return new RuntimeException(
        String.format(
            Locale.ROOT,
            "Failed to fetch %s provider metadata: server returned code %d with message: %s",
            type,
            httpResponse.getStatusCode(),
            httpResponse.getBody()));
  }

  @FunctionalInterface
  private interface MetadataProvider {
    ReadOnlyAuthorizationServerEndpointMetadata fetchMetadata(Issuer issuer)
        throws IOException, ParseException;
  }
}
