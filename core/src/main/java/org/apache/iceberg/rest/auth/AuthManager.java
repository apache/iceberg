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
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTClient;

/**
 * Manager for authentication sessions. This interface is used to create sessions for the catalog,
 * the tables/views, and any other context that requires authentication.
 *
 * <p>Managers are usually stateful and may require initialization and cleanup. The manager is
 * created by the catalog and is closed when the catalog is closed.
 */
public interface AuthManager extends AutoCloseable {

  /**
   * Returns a manager that uses the given REST client for its internal authentication requests,
   * such as for fetching tokens or refreshing credentials.
   *
   * <p>By default, this method ignores the REST client and returns {@code this}. Implementations
   * that require a client should override this method.
   */
  default AuthManager withClient(RESTClient client) {
    return this;
  }

  /** Returns an authentication session for the given scope. */
  default AuthSession authSession(AuthScope scope) {
    throw new UnsupportedOperationException("AuthManager.authSession is not implemented");
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Deprecated
  default AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return catalogSession(initClient, properties);
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Deprecated
  default AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties) {
    return withClient(sharedClient).authSession(AuthScopes.Catalog.of(properties));
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Deprecated
  default AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    return catalogSession(sharedClient, properties);
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Deprecated
  default AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    return parent;
  }

  /**
   * @deprecated since 1.9.0, will be removed in 1.10.0; use {@link #authSession(AuthScope)}
   *     instead.
   */
  @Deprecated
  default AuthSession tableSession(
      TableIdentifier table, Map<String, String> properties, AuthSession parent) {
    return parent;
  }

  /**
   * Closes the manager and releases any resources.
   *
   * <p>This method is called when the owning catalog is closed.
   */
  @Override
  void close();
}
