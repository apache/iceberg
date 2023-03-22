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
package org.apache.iceberg.catalog;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** A Catalog API for table and namespace operations that includes session context. */
public interface SessionCatalog {
  /** Context for a session. */
  final class SessionContext {
    private final String sessionId;
    private final String identity;
    private final Map<String, String> credentials;
    private final Map<String, String> properties;
    private final Object wrappedIdentity;

    public static SessionContext createEmpty() {
      return new SessionContext(UUID.randomUUID().toString(), null, null, ImmutableMap.of());
    }

    public SessionContext(
        String sessionId,
        String identity,
        Map<String, String> credentials,
        Map<String, String> properties) {
      this(sessionId, identity, credentials, properties, null);
    }

    public SessionContext(
        String sessionId,
        String identity,
        Map<String, String> credentials,
        Map<String, String> properties,
        Object wrappedIdentity) {
      this.sessionId = sessionId;
      this.identity = identity;
      this.credentials = credentials;
      this.properties = properties;
      this.wrappedIdentity = wrappedIdentity;
    }

    /**
     * Returns a string that identifies this session.
     *
     * <p>This can be used for caching state within a session.
     *
     * @return a string that identifies this session
     */
    public String sessionId() {
      return sessionId;
    }

    /**
     * Returns a string that identifies the current user or principal.
     *
     * <p>This identity cannot change for a given session ID.
     *
     * @return a user or principal identity string
     */
    public String identity() {
      return identity;
    }

    /**
     * Returns the session's credential map.
     *
     * <p>This cannot change for a given session ID.
     *
     * @return a credential string
     */
    public Map<String, String> credentials() {
      return credentials;
    }

    /**
     * Returns a map of properties currently set for the session.
     *
     * @return a map of properties
     */
    public Map<String, String> properties() {
      return properties;
    }

    /**
     * Returns the opaque wrapped identity object.
     *
     * @return the wrapped identity
     */
    public Object wrappedIdentity() {
      return wrappedIdentity;
    }
  }

  /**
   * Initialize given a custom name and a map of catalog properties.
   *
   * @param name a custom name for the catalog
   * @param properties catalog properties
   */
  void initialize(String name, Map<String, String> properties);

  /**
   * Return the name for this catalog.
   *
   * @return this catalog's name
   */
  String name();

  /**
   * Return the properties for this catalog.
   *
   * @return this catalog's config properties
   */
  Map<String, String> properties();

  /**
   * Return all the identifiers under this namespace.
   *
   * @param context session context
   * @param namespace a namespace
   * @return a list of identifiers for tables
   * @throws NoSuchNamespaceException if the namespace does not exist
   */
  List<TableIdentifier> listTables(SessionContext context, Namespace namespace);

  /**
   * Create a builder to create a table or start a create/replace transaction.
   *
   * @param context session context
   * @param ident a table identifier
   * @param schema a schema
   * @return the builder to create a table or start a create/replace transaction
   */
  Catalog.TableBuilder buildTable(SessionContext context, TableIdentifier ident, Schema schema);

  /**
   * Register a table if it does not exist.
   *
   * @param context session context
   * @param ident a table identifier
   * @param metadataFileLocation the location of a metadata file
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists in the catalog.
   */
  Table registerTable(SessionContext context, TableIdentifier ident, String metadataFileLocation);

  /**
   * Check whether table exists.
   *
   * @param context session context
   * @param ident a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(SessionContext context, TableIdentifier ident) {
    try {
      loadTable(context, ident);
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Load a table.
   *
   * @param context session context
   * @param ident a table identifier
   * @return instance of {@link Table} implementation referred by {@code tableIdentifier}
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(SessionContext context, TableIdentifier ident);

  /**
   * Drop a table, without requesting that files are immediately deleted.
   *
   * <p>Data and metadata files should be deleted according to the catalog's policy.
   *
   * @param context session context
   * @param ident a table identifier
   * @return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(SessionContext context, TableIdentifier ident);

  /**
   * Drop a table and request that files are immediately deleted.
   *
   * @param context session context
   * @param ident a table identifier
   * @return true if the table was dropped and purged, false if the table did not exist
   * @throws UnsupportedOperationException if immediate delete is not supported
   */
  boolean purgeTable(SessionContext context, TableIdentifier ident);

  /**
   * Rename a table.
   *
   * @param context session context
   * @param from identifier of the table to rename
   * @param to new table name
   * @throws NoSuchTableException if the from table does not exist
   * @throws AlreadyExistsException if the to table already exists
   */
  void renameTable(SessionContext context, TableIdentifier from, TableIdentifier to);

  /**
   * Invalidate cached table metadata from current catalog.
   *
   * <p>If the table is already loaded or cached, drop cached data. If the table does not exist or
   * is not cached, do nothing.
   *
   * @param context session context
   * @param ident a table identifier
   */
  void invalidateTable(SessionContext context, TableIdentifier ident);

  /**
   * Create a namespace in the catalog.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @throws AlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  default void createNamespace(SessionContext context, Namespace namespace) {
    createNamespace(context, namespace, ImmutableMap.of());
  }

  /**
   * Create a namespace in the catalog.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @param metadata a string Map of properties for the given namespace
   * @throws AlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  void createNamespace(SessionContext context, Namespace namespace, Map<String, String> metadata);

  /**
   * List top-level namespaces from the catalog.
   *
   * <p>If an object such as a table, view, or function exists, its parent namespaces must also
   * exist and must be returned by this discovery method. For example, if table a.b.t exists, this
   * method must return ["a"] in the result array.
   *
   * @param context session context
   * @return an List of namespace {@link Namespace} names
   */
  default List<Namespace> listNamespaces(SessionContext context) {
    return listNamespaces(context, Namespace.empty());
  }

  /**
   * List namespaces from the namespace.
   *
   * <p>For example, if table a.b.t exists, use 'SELECT NAMESPACE IN a' this method must return
   * Namepace.of("a","b") {@link Namespace}.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @return a List of namespace {@link Namespace} names
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  List<Namespace> listNamespaces(SessionContext context, Namespace namespace);

  /**
   * Load metadata properties for a namespace.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  Map<String, String> loadNamespaceMetadata(SessionContext context, Namespace namespace);

  /**
   * Drop a namespace. If the namespace exists and was dropped, this will return true.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @return true if the namespace was dropped, false otherwise.
   * @throws NamespaceNotEmptyException If the namespace is not empty
   */
  boolean dropNamespace(SessionContext context, Namespace namespace);

  /**
   * Set a collection of properties on a namespace in the catalog.
   *
   * <p>Properties that are not in the given map are not modified or removed by this method.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @param updates properties to set for the namespace
   * @param removals properties to remove from the namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  boolean updateNamespaceMetadata(
      SessionContext context,
      Namespace namespace,
      Map<String, String> updates,
      Set<String> removals);

  /**
   * Checks whether the Namespace exists.
   *
   * @param context session context
   * @param namespace a {@link Namespace namespace}
   * @return true if the Namespace exists, false otherwise
   */
  default boolean namespaceExists(SessionContext context, Namespace namespace) {
    try {
      loadNamespaceMetadata(context, namespace);
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }
}
