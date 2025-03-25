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
import java.util.Optional;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTUtil;
import org.apache.iceberg.rest.auth.AuthManager;
import org.apache.iceberg.rest.auth.AuthScope;
import org.apache.iceberg.rest.auth.AuthScopes;
import org.apache.iceberg.rest.auth.AuthSession;
import software.amazon.awssdk.auth.signer.Aws4Signer;

/**
 * An AuthManager that authenticates requests with SigV4.
 *
 * <p>It takes a delegate AuthManager to handle double authentication cases, e.g. on top of OAuth2.
 */
public class RESTSigV4AuthManager implements AuthManager {

  private final Aws4Signer signer;
  private final AuthManager delegate;

  private Map<String, String> catalogProperties = Map.of();

  public RESTSigV4AuthManager(String ignored, AuthManager delegate) {
    this.delegate = Preconditions.checkNotNull(delegate, "Invalid delegate: null");
    signer = Aws4Signer.create();
  }

  private RESTSigV4AuthManager(AuthManager delegate, Aws4Signer signer) {
    this.delegate = Preconditions.checkNotNull(delegate, "Invalid delegate: null");
    this.signer = signer;
  }

  @Override
  public AuthManager withClient(RESTClient client) {
    return new RESTSigV4AuthManager(delegate.withClient(client), signer);
  }

  @Override
  public AuthSession authSession(AuthScope scope) {
    AwsProperties awsProperties;
    if (scope instanceof AuthScopes.Catalog) {
      this.catalogProperties = scope.properties();
      awsProperties = new AwsProperties(catalogProperties);
    } else {
      awsProperties = new AwsProperties(RESTUtil.merge(catalogProperties, scope.properties()));
    }

    AuthSession parent = scope.parent();
    Preconditions.checkState(
        parent == null || parent instanceof RESTSigV4AuthSession,
        "Parent session is not SigV4: %s",
        parent);
    AuthScope delegateScope =
        parent == null ? scope : scope.withParent(((RESTSigV4AuthSession) parent).delegate());
    return new RESTSigV4AuthSession(signer, delegate.authSession(delegateScope), awsProperties);
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Override
  @Deprecated
  public RESTSigV4AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return new RESTSigV4AuthSession(
        signer, delegate.initSession(initClient, properties), new AwsProperties(properties));
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Override
  @Deprecated
  public RESTSigV4AuthSession catalogSession(
      RESTClient sharedClient, Map<String, String> properties) {
    this.catalogProperties = properties;
    AwsProperties awsProperties = new AwsProperties(catalogProperties);
    return new RESTSigV4AuthSession(
        signer, delegate.catalogSession(sharedClient, catalogProperties), awsProperties);
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Override
  @Deprecated
  public RESTSigV4AuthSession contextualSession(
      SessionCatalog.SessionContext context, AuthSession parent) {
    Preconditions.checkState(
        parent instanceof RESTSigV4AuthSession, "Parent session is not SigV4: %s", parent);
    AwsProperties contextProperties =
        new AwsProperties(
            RESTUtil.merge(
                catalogProperties,
                // Use both context properties and credentials to create the AwsProperties instance
                RESTUtil.merge(
                    Optional.ofNullable(context.properties()).orElseGet(Map::of),
                    Optional.ofNullable(context.credentials()).orElseGet(Map::of))));
    RESTSigV4AuthSession sigV4Parent = (RESTSigV4AuthSession) parent;
    return new RESTSigV4AuthSession(
        signer, delegate.contextualSession(context, sigV4Parent.delegate()), contextProperties);
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Override
  @Deprecated
  public RESTSigV4AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    Preconditions.checkState(
        parent instanceof RESTSigV4AuthSession, "Parent session is not SigV4: %s", parent);
    AwsProperties tableProperties =
        new AwsProperties(RESTUtil.merge(catalogProperties, properties));
    RESTSigV4AuthSession sigV4Parent = (RESTSigV4AuthSession) parent;
    return new RESTSigV4AuthSession(
        signer, delegate.tableSession(table, properties, sigV4Parent.delegate()), tableProperties);
  }

  @Override
  public void close() {
    delegate.close();
  }
}
