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

import com.nimbusds.oauth2.sdk.token.AccessToken;
import java.util.function.Supplier;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;

/** An {@link AuthSession} that adds a bearer token obtained by an {@link OAuth2Client}. */
class OAuth2Session implements AuthSession {

  private final OAuth2Config config;
  private final OAuth2Client client;

  OAuth2Session(OAuth2Config config, Supplier<RESTClient> restClientSupplier) {
    this.config = config;
    this.client = new OAuth2Client(config, new OAuth2Runtime(restClientSupplier));
  }

  OAuth2Config config() {
    return config;
  }

  @Override
  public HTTPRequest authenticate(HTTPRequest request) {
    AccessToken accessToken = client.authenticate();
    HTTPHeader authorization = HTTPHeader.of("Authorization", "Bearer " + accessToken.getValue());
    HTTPHeaders newHeaders = request.headers().putIfAbsent(HTTPHeaders.of(authorization));
    return newHeaders.equals(request.headers())
        ? request
        : ImmutableHTTPRequest.builder().from(request).headers(newHeaders).build();
  }

  @Override
  public void close() {
    client.close();
  }
}
