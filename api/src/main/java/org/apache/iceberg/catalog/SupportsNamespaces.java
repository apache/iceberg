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
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Catalog methods for working with namespaces.
 *
 * <p>If an object such as a table, view, or function exists, its parent namespaces must also exist
 * and must be returned by the discovery methods {@link #listNamespaces()} and {@link
 * #listNamespaces(Namespace namespace)}.
 *
 * <p>Catalog implementations are not required to maintain the existence of namespaces independent
 * of objects in a namespace. For example, a function catalog that loads functions using reflection
 * and uses Java packages as namespaces is not required to support the methods to create, alter, or
 * drop a namespace. Implementations are allowed to discover the existence of objects or namespaces
 * without throwing {@link NoSuchNamespaceException} when no namespace is found.
 */
public interface SupportsNamespaces {
  /**
   * Create a namespace in the catalog.
   *
   * @param namespace a namespace. {@link Namespace}.
   * @throws AlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  default void createNamespace(Namespace namespace) {
    createNamespace(namespace, ImmutableMap.of());
  }

  /**
   * Create a namespace in the catalog.
   *
   * @param namespace a multi-part namespace
   * @param metadata a string Map of properties for the given namespace
   * @throws AlreadyExistsException If the namespace already exists
   * @throws UnsupportedOperationException If create is not a supported operation
   */
  void createNamespace(Namespace namespace, Map<String, String> metadata);

  /**
   * List top-level namespaces from the catalog.
   *
   * <p>If an object such as a table, view, or function exists, its parent namespaces must also
   * exist and must be returned by this discovery method. For example, if table a.b.t exists, this
   * method must return ["a"] in the result array.
   *
   * @return an List of namespace {@link Namespace} names
   */
  default List<Namespace> listNamespaces() {
    return listNamespaces(Namespace.empty());
  }

  /**
   * List namespaces from the namespace.
   *
   * <p>For example, if table a.b.t exists, use 'SELECT NAMESPACE IN a' this method must return
   * Namepace.of("a","b") {@link Namespace}.
   *
   * @return a List of namespace {@link Namespace} names
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException;

  /**
   * Load metadata properties for a namespace.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   */
  Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException;

  /**
   * Drop a namespace. If the namespace exists and was dropped, this will return true.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return true if the namespace was dropped, false otherwise.
   * @throws NamespaceNotEmptyException If the namespace is not empty
   */
  boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException;

  /**
   * Set a collection of properties on a namespace in the catalog.
   *
   * <p>Properties that are not in the given map are not modified or removed by this method.
   *
   * @param namespace a namespace. {@link Namespace}
   * @param properties a collection of metadata to apply to the namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException;

  /**
   * Remove a set of property keys from a namespace in the catalog.
   *
   * <p>Properties that are not in the given set are not modified or removed by this method.
   *
   * @param namespace a namespace. {@link Namespace}
   * @param properties a collection of metadata to apply to the namespace
   * @throws NoSuchNamespaceException If the namespace does not exist (optional)
   * @throws UnsupportedOperationException If namespace properties are not supported
   */
  boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException;

  /**
   * Checks whether the Namespace exists.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return true if the Namespace exists, false otherwise
   */
  default boolean namespaceExists(Namespace namespace) {
    try {
      loadNamespaceMetadata(namespace);
      return true;
    } catch (NoSuchNamespaceException e) {
      return false;
    }
  }
}
