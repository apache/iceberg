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
package org.apache.iceberg.aws;

import java.util.Map;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthSession;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/**
 * An AuthManager that authenticates requests with SigV4.
 *
 * <p>It takes a delegate AuthManager to handle double authentication cases, e.g. on top of OAuth2.
 */
@SuppressWarnings("unused") // loaded by reflection
public class RESTSigV4AuthManager implements AuthManager {

  private final Aws4Signer signer = Aws4Signer.create();
  private final AuthManager delegate;

  private Map<String, String> catalogProperties = Map.of();

  public RESTSigV4AuthManager(String name, AuthManager delegate) {
    this.delegate = Preconditions.checkNotNull(delegate, "Invalid delegate: null");
  }

  @Override
  public RESTSigV4AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return new RESTSigV4AuthSession(
        signer, delegate.initSession(initClient, properties), new AwsProperties(properties));
  }

  @Override
  public RESTSigV4AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> properties) {
    this.catalogProperties = properties;
    AwsProperties awsProperties = new AwsProperties(catalogProperties);
    return new RESTSigV4AuthSession(
        signer, delegate.catalogSession(sharedClient, catalogProperties), awsProperties);
  }

  @Override
  public AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    AwsProperties contextProperties =
        new AwsProperties(RESTUtil.merge(catalogProperties, context.properties()));
    return new RESTSigV4AuthSession(
        signer, delegate.contextualSession(context, parent), contextProperties);
  }

  @Override
  public AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    AwsProperties tableProperties =
        new AwsProperties(RESTUtil.merge(catalogProperties, properties));
    return new RESTSigV4AuthSession(
        signer, delegate.tableSession(table, properties, parent), tableProperties);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
