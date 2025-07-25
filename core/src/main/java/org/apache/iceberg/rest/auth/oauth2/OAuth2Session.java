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
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.HTTPHeaders.HTTPHeader;
import org.apache.iceberg.rest.HTTPRequest;
import org.apache.iceberg.rest.ImmutableHTTPRequest;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthSession;
import org.apache.iceberg.rest.auth.oauth2.client.OAuth2Client;

public class OAuth2Session implements AuthSession {

  private final OAuth2Config config;
  private final OAuth2Client client;

  public OAuth2Session(
      OAuth2Config config,
      Supplier<RESTClient> restClientSupplier,
      ScheduledExecutorService executor) {
    this.config = config;
    this.client = new OAuth2Client(config, OAuth2Runtime.of(restClientSupplier, executor, null));
  }

  public OAuth2Session(
      OAuth2Session parent,
      OAuth2Config config,
      Supplier<RESTClient> restClientSupplier,
      ScheduledExecutorService executor) {
    this.config = config;
    this.client =
        new OAuth2Client(config, OAuth2Runtime.of(restClientSupplier, executor, parent.client));
  }

  private OAuth2Session(OAuth2Session toCopy) {
    this.config = toCopy.config;
    this.client = toCopy.client.copy();
  }

  public OAuth2Config config() {
    return config;
  }

  /**
   * Copies this session and the underlying client. This is only needed when reusing an init session
   * as a catalog session.
   */
  public OAuth2Session copy() {
    return new OAuth2Session(this);
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
