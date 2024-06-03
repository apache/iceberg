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
import org.immutables.value.Value;

/**
 * The purpose of this class is to hold configuration options for {@link
 * org.apache.iceberg.rest.auth.OAuth2Util.AuthSession}.
 */
@Value.Style(redactedMask = "****")
@SuppressWarnings("ImmutablesStyle")
@Value.Immutable
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

  @Value.Lazy
  @Nullable
  default Long expiresAtMillis() {
    return OAuth2Util.expiresAtMillis(token());
  }

  @Value.Default
  default boolean keepRefreshed() {
    return true;
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
}
