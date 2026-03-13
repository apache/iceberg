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
   * Returns a temporary session to use for contacting the configuration endpoint only. Note that
   * the returned session will be closed after the configuration endpoint is contacted, and should
   * not be cached.
   *
   * <p>The provided REST client is a short-lived client; it should only be used to fetch initial
   * credentials, if required, and must be discarded after that.
   *
   * <p>This method cannot return null. By default, it returns the catalog session.
   */
  default AuthSession initSession(RESTClient initClient, Map<String, String> properties) {
    return catalogSession(initClient, properties);
  }

  /**
   * Returns a long-lived session whose lifetime is tied to the owning catalog. This session serves
   * as the parent session for all other sessions (contextual and table-specific). It is closed when
   * the owning catalog is closed.
   *
   * <p>The provided REST client is a long-lived, shared client; if required, implementors may store
   * it and reuse it for all subsequent requests to the authorization server, e.g. for renewing or
   * refreshing credentials. It is not necessary to close it when {@link #close()} is called.
   *
   * <p>This method cannot return null.
   *
   * <p>It is not required to cache the returned session internally, as the catalog will keep it
   * alive for the lifetime of the catalog.
   */
  AuthSession catalogSession(RESTClient sharedClient, Map<String, String> properties);

  /**
   * Returns a new session targeting a table or view. This method is intended for components other
   * that the catalog that need to access tables or views, such as request signer clients.
   *
   * <p>This method cannot return null. By default, it returns the catalog session.
   *
   * <p>Implementors should cache table sessions internally, as the owning component will not cache
   * them. Also, the owning component never closes table sessions; implementations should manage
   * their lifecycle themselves and close them when they are no longer needed.
   */
  default AuthSession tableSession(RESTClient sharedClient, Map<String, String> properties) {
    return catalogSession(sharedClient, properties);
  }

  /**
   * Returns a session for a specific context.
   *
   * <p>If the context requires a specific {@link AuthSession}, this method should return a new
   * {@link AuthSession} instance, otherwise it should return the parent session.
   *
   * <p>This method cannot return null. By default, it returns the parent session.
   *
   * <p>Implementors should cache contextual sessions internally, as the catalog will not cache
   * them. Also, the owning catalog never closes contextual sessions; implementations should manage
   * their lifecycle themselves and close them when they are no longer needed.
   */
  default AuthSession contextualSession(SessionCatalog.SessionContext context, AuthSession parent) {
    return parent;
  }

  /**
   * Returns a new session targeting a specific table or view. The properties are the ones returned
   * by the table/view endpoint.
   *
   * <p>If the table or view requires a specific {@link AuthSession}, this method should return a
   * new {@link AuthSession} instance, otherwise it should return the parent session.
   *
   * <p>This method cannot return null. By default, it returns the parent session.
   *
   * <p>Implementors should cache table sessions internally, as the catalog will not cache them.
   * Also, the owning catalog never closes table sessions; implementations should manage their
   * lifecycle themselves and close them when they are no longer needed.
   */
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
