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
package org.apache.iceberg.rest.auth.oauth2;

import com.nimbusds.oauth2.sdk.AbstractConfigurationRequest;
import com.nimbusds.oauth2.sdk.ParseException;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerConfigurationRequest;
import com.nimbusds.oauth2.sdk.as.AuthorizationServerMetadata;
import com.nimbusds.oauth2.sdk.as.ReadOnlyAuthorizationServerEndpointMetadata;
import com.nimbusds.oauth2.sdk.http.HTTPResponse;
import com.nimbusds.oauth2.sdk.id.Issuer;
import com.nimbusds.openid.connect.sdk.op.OIDCProviderConfigurationRequest;
import java.io.IOException;
import java.net.URI;
import java.util.Locale;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Resolves the token endpoint, either from the configuration or through metadata discovery on the
 * issuer URL. OpenID Connect discovery is attempted first, then RFC 8414 authorization server
 * metadata. The resolved endpoint is cached.
 */
final class EndpointProvider {

  private final OAuth2Config config;
  private final OAuth2Runtime runtime;

  private volatile URI tokenEndpoint;

  EndpointProvider(OAuth2Config config, OAuth2Runtime runtime) {
    this.config = config;
    this.runtime = runtime;
    this.tokenEndpoint = config.tokenEndpoint();
  }

  URI tokenEndpoint() {
    if (tokenEndpoint == null) {
      synchronized (this) {
        if (tokenEndpoint == null) {
          tokenEndpoint = fetchServerMetadata().getTokenEndpointURI();
          Preconditions.checkState(
              tokenEndpoint != null,
              "Server metadata for issuer %s does not contain a token endpoint",
              config.issuerUrl());
        }
      }
    }

    return tokenEndpoint;
  }

  private ReadOnlyAuthorizationServerEndpointMetadata fetchServerMetadata() {
    URI issuerUrl = config.issuerUrl();
    Preconditions.checkState(issuerUrl != null, "No issuer URL configured");
    Issuer issuer = new Issuer(issuerUrl);

    Exception oidcFailure;
    try {
      return fetchMetadata(new OIDCProviderConfigurationRequest(issuer), issuer, "OpenID Connect");
    } catch (IOException | ParseException | RuntimeException e) {
      oidcFailure = e;
    }

    try {
      return fetchMetadata(new AuthorizationServerConfigurationRequest(issuer), issuer, "OAuth2");
    } catch (IOException | ParseException | RuntimeException e) {
      RuntimeException error =
          new RuntimeException("Failed to fetch server metadata for issuer " + issuerUrl, e);
      error.addSuppressed(oidcFailure);
      throw error;
    }
  }

  private ReadOnlyAuthorizationServerEndpointMetadata fetchMetadata(
      AbstractConfigurationRequest request, Issuer expected, String type)
      throws IOException, ParseException {
    HTTPResponse httpResponse = request.toHTTPRequest().send(runtime.httpClient());
    if (!httpResponse.indicatesSuccess()) {
      throw new IOException(
          String.format(
              Locale.ROOT,
              "Failed to fetch %s server metadata: server returned status %d with body: %s",
              type,
              httpResponse.getStatusCode(),
              httpResponse.getBody()));
    }

    AuthorizationServerMetadata metadata =
        AuthorizationServerMetadata.parse(httpResponse.getBodyAsJSONObject());

    // RFC 8414 section 3.3: the issuer in the metadata must match the issuer used to build the URL
    Preconditions.checkState(
        expected.equals(metadata.getIssuer()),
        "Invalid %s server metadata: issuer %s does not match expected issuer %s",
        type,
        metadata.getIssuer(),
        expected);

    return metadata;
  }
}
