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
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.HTTPHeaders;
import org.apache.iceberg.rest.RESTClient;

/** An auth manager that adds static BASIC authentication data to outgoing HTTP requests. */
public final class BasicAuthManager implements AuthManager {

  public BasicAuthManager(String ignored) {
    // no-op
  }

  @Override
  public AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    Preconditions.checkArgument(
        properties.containsKey(AuthProperties.BASIC_USERNAME),
        "Invalid username: missing required property %s",
        AuthProperties.BASIC_USERNAME);
    Preconditions.checkArgument(
        properties.containsKey(AuthProperties.BASIC_PASSWORD),
        "Invalid password: missing required property %s",
        AuthProperties.BASIC_PASSWORD);
    String username = properties.get(AuthProperties.BASIC_USERNAME);
    String password = properties.get(AuthProperties.BASIC_PASSWORD);
    String credentials = username + ":" + password;
    return DefaultAuthSession.of(HTTPHeaders.of(OAuth2Util.basicAuthHeaders(credentials)));
  }

  @Override
  public void close() {
    // no resources to close
  }
}
