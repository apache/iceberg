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
package org.apache.iceberg.rest.responses;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.auth.OAuth2Util;

public class OAuthTokenResponse implements RESTResponse {
  private final String accessToken;
  private final String issuedTokenType;
  private final String tokenType;
  private final Integer expiresIn;
  private final String scope;

  private OAuthTokenResponse(
      String accessToken,
      String issuedTokenType,
      String tokenType,
      Integer expiresIn,
      String scope) {
    this.accessToken = accessToken;
    this.issuedTokenType = issuedTokenType;
    this.tokenType = tokenType;
    this.expiresIn = expiresIn;
    this.scope = scope;
  }

  @Override
  public void validate() {
    Preconditions.checkNotNull(accessToken, "Invalid access token: null");
    Preconditions.checkArgument(
        "bearer".equalsIgnoreCase(tokenType) || "N_A".equalsIgnoreCase(tokenType),
        "Unsupported token type: %s (must be \"bearer\" or \"N_A\")",
        tokenType);
  }

  public String token() {
    return accessToken;
  }

  public String issuedTokenType() {
    return issuedTokenType;
  }

  public String tokenType() {
    return tokenType;
  }

  public Integer expiresInSeconds() {
    return expiresIn;
  }

  public List<String> scopes() {
    return scope != null ? OAuth2Util.parseScope(scope) : ImmutableList.of();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {
    private String accessToken;
    private String issuedTokenType;
    private String tokenType;
    private Integer expiresInSeconds;
    private final List<String> scopes = Lists.newArrayList();

    private Builder() {}

    public Builder withToken(String token) {
      this.accessToken = token;
      return this;
    }

    public Builder withIssuedTokenType(String issuedType) {
      this.issuedTokenType = issuedType;
      return this;
    }

    public Builder withTokenType(String type) {
      this.tokenType = type;
      return this;
    }

    public Builder setExpirationInSeconds(int durationInSeconds) {
      this.expiresInSeconds = durationInSeconds;
      return this;
    }

    public Builder addScope(String scope) {
      Preconditions.checkArgument(OAuth2Util.isValidScopeToken(scope), "Invalid scope: %s");
      this.scopes.add(scope);
      return this;
    }

    public Builder addScopes(List<String> scope) {
      scope.forEach(this::addScope);
      return this;
    }

    public OAuthTokenResponse build() {
      String scope = scopes.isEmpty() ? null : OAuth2Util.toScope(scopes);
      return new OAuthTokenResponse(
          accessToken, issuedTokenType, tokenType, expiresInSeconds, scope);
    }
  }
}
