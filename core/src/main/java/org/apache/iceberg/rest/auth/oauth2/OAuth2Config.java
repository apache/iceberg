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

import com.nimbusds.oauth2.sdk.GrantType;
import java.util.Map;
import org.immutables.value.Value;

/** The configuration for the OAuth2 AuthManager. */
@Value.Immutable
interface OAuth2Config {

  String PREFIX = "rest.auth.oauth2.";

  /** The basic configuration, including token endpoint, grant type, client id and client secret. */
  BasicConfig basicConfig();

  /** The token refresh configuration. */
  @Value.Default
  default TokenRefreshConfig tokenRefreshConfig() {
    return ImmutableTokenRefreshConfig.builder().build();
  }

  /** The token exchange grant configuration. */
  @Value.Default
  default TokenExchangeConfig tokenExchangeConfig() {
    return ImmutableTokenExchangeConfig.builder().build();
  }

  @Value.Check
  default void validate() {
    // At this level, we only need to validate constraints that span multiple
    // configuration classes; individual configuration classes are validated
    // internally in their respective validate() methods.
    ConfigValidator validator = new ConfigValidator();

    if (basicConfig().grantType().equals(GrantType.TOKEN_EXCHANGE)) {
      validator.check(
          tokenExchangeConfig().subjectTokenString().isPresent(),
          TokenExchangeConfig.SUBJECT_TOKEN,
          "subject token must be set if grant type is '%s'",
          GrantType.TOKEN_EXCHANGE.getValue());
    }

    validator.validate();
  }

  /** Creates an {@link OAuth2Config} from the given properties map. */
  static OAuth2Config from(Map<String, String> properties) {
    return ImmutableOAuth2Config.builder()
        .basicConfig(BasicConfig.from(properties).build())
        .tokenRefreshConfig(TokenRefreshConfig.from(properties).build())
        .tokenExchangeConfig(TokenExchangeConfig.from(properties).build())
        .build();
  }
}
