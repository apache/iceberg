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
package org.apache.iceberg.rest.auth;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.iceberg.rest.ResourcePaths;
import org.apache.iceberg.util.PropertyUtil;
import org.immutables.value.Value;

/**
 * The purpose of this class is to hold OAuth configuration options for {@link
 * OAuth2Util.AuthSession}.
 */
@Value.Style(redactedMask = "****")
@Value.Immutable
@SuppressWarnings({"ImmutablesStyle", "SafeLoggingPropagation"})
public interface AuthConfig {
  @Nullable
  @Value.Redacted
  String token();

  @Nullable
  String tokenType();

  @Nullable
  @Value.Redacted
  String credential();

  @Value.Default
  default String scope() {
    return OAuth2Properties.CATALOG_SCOPE;
  }

  @Value.Default
  @Nullable
  default Long expiresAtMillis() {
    return OAuth2Util.expiresAtMillis(token());
  }

  @Value.Default
  default boolean keepRefreshed() {
    return OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT;
  }

  @Value.Default
  default boolean exchangeEnabled() {
    return OAuth2Properties.TOKEN_EXCHANGE_ENABLED_DEFAULT;
  }

  @Nullable
  @Value.Default
  default String oauth2ServerUri() {
    return ResourcePaths.tokens();
  }

  Map<String, String> optionalOAuthParams();

  static ImmutableAuthConfig.Builder builder() {
    return ImmutableAuthConfig.builder();
  }

  static AuthConfig fromProperties(Map<String, String> properties) {
    return builder()
        .credential(properties.get(OAuth2Properties.CREDENTIAL))
        .token(properties.get(OAuth2Properties.TOKEN))
        .scope(properties.getOrDefault(OAuth2Properties.SCOPE, OAuth2Properties.CATALOG_SCOPE))
        .oauth2ServerUri(
            properties.getOrDefault(OAuth2Properties.OAUTH2_SERVER_URI, ResourcePaths.tokens()))
        .optionalOAuthParams(OAuth2Util.buildOptionalParam(properties))
        .keepRefreshed(
            PropertyUtil.propertyAsBoolean(
                properties,
                OAuth2Properties.TOKEN_REFRESH_ENABLED,
                OAuth2Properties.TOKEN_REFRESH_ENABLED_DEFAULT))
        .exchangeEnabled(
            PropertyUtil.propertyAsBoolean(
                properties,
                OAuth2Properties.TOKEN_EXCHANGE_ENABLED,
                OAuth2Properties.TOKEN_EXCHANGE_ENABLED_DEFAULT))
        .expiresAtMillis(expiresAtMillis(properties))
        .build();
  }

  private static Long expiresAtMillis(Map<String, String> props) {
    Long expiresAtMillis = null;

    if (props.containsKey(OAuth2Properties.TOKEN)) {
      expiresAtMillis = OAuth2Util.expiresAtMillis(props.get(OAuth2Properties.TOKEN));
    }

    if (expiresAtMillis == null && props.containsKey(OAuth2Properties.TOKEN_EXPIRES_IN_MS)) {
      long millis =
          PropertyUtil.propertyAsLong(
              props,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS,
              OAuth2Properties.TOKEN_EXPIRES_IN_MS_DEFAULT);
      expiresAtMillis = System.currentTimeMillis() + millis;
    }

    return expiresAtMillis;
  }
}
