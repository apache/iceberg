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
   * Initializes the manager with the owner of the catalog, the REST client, and the properties
   * provided in the catalog configuration.
   *
   * <p>This method is intended to be called many times, typically once before contacting the config
   * endpoint, and once after contacting the config endpoint. The properties provided in the catalog
   * configuration are passed in the first call, and the properties returned by the config endpoint
   * are passed in the second call, merged with the catalog ones.
   *
   * <p>This method cannot return null.
   */
  void initialize(String owner, RESTClient client, Map<String, String> properties);

  /**
   * Returns a session for the whole catalog.
   *
   * <p>This method is intended to be called many times, typically once before contacting the config
   * endpoint, and once after contacting the config endpoint.
   *
   * <p>This method cannot return null.
   */
  AuthSession catalogSession();

  /**
   * Returns a session for a specific context.
   *
   * <p>If the context requires a specific {@link AuthSession}, this method should return a new
   * {@link AuthSession} instance, otherwise it should return the parent session.
   *
   * <p>This method cannot return null. By default, it returns the parent session.
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
  default void close() {
    // Do nothing
  }
}
