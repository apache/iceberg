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
package org.apache.iceberg.gcp;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.auth.oauth2.OAuth2CredentialsWithRefresh;
import java.util.Optional;
import org.apache.iceberg.gcp.gcs.OAuth2RefreshCredentialsHandler;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public final class GCPAuthUtils {

  private GCPAuthUtils() {}

  public static OAuth2Credentials oauth2CredentialsFromGcpProperties(
      GCPProperties gcpProperties, CloseableGroup closeableGroup) {

    Optional<String> optionalToken = gcpProperties.oauth2Token();
    Preconditions.checkState(
        optionalToken.isPresent(), "OAuth2 token must be set to produce OAuth2 Credentials");

    // Explicitly configure an OAuth token
    AccessToken accessToken =
        new AccessToken(optionalToken.get(), gcpProperties.oauth2TokenExpiresAt().orElse(null));

    if (gcpProperties.oauth2RefreshCredentialsEnabled()
        && gcpProperties.oauth2RefreshCredentialsEndpoint().isPresent()) {

      Preconditions.checkNotNull(
          closeableGroup,
          "Must specify a closeable group that handles closure of refresh handler.");
      OAuth2RefreshCredentialsHandler oAuthRefreshHandler =
          OAuth2RefreshCredentialsHandler.create(gcpProperties.properties());
      closeableGroup.addCloseable(oAuthRefreshHandler);

      return OAuth2CredentialsWithRefresh.newBuilder()
          .setAccessToken(accessToken)
          .setRefreshHandler(oAuthRefreshHandler)
          .build();
    } else {
      return OAuth2Credentials.create(accessToken);
    }
  }
}
