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

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog.SessionContext;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;

public class OAuth2Manager implements AuthManager {

  public OAuth2Manager(String name) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AuthSession initSession(RESTClient initClient, Map<String, String> initProperties) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> catalogProperties) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AuthSession contextualSession(SessionContext context, AuthSession parent) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() {}
}
