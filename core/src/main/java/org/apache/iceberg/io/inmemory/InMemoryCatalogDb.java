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

package org.apache.iceberg.io.inmemory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

final class InMemoryCatalogDb implements AutoCloseable {

  private final ConcurrentMap<Namespace, Map<String, String>> namespaceDb;
  private final ConcurrentMap<TableIdentifier, String> tableDb;

  InMemoryCatalogDb() {
    this.namespaceDb = new ConcurrentHashMap<>();
    this.tableDb = new ConcurrentHashMap<>();
  }

  /**
   * Put the namespace in the namespace db if it does not already exist.
   */
  void putNamespaceEntryIfAbsent(Namespace namespace, Map<String, String> properties) {
    namespaceDb.computeIfAbsent(namespace, k -> new HashMap<>(properties));
  }

  /**
   * Get a copy of the namespace properties.
   * This method returns {@code null} if the namespace does not exist.
   */
  ImmutableMap<String, String> getNamespaceProperties(Namespace namespace) {
    Map<String, String> properties = namespaceDb.get(namespace);
    return properties != null ? ImmutableMap.copyOf(properties) : null;
  }

  /**
   * Put properties in existing namespace, overwriting previous key-values.
   */
  void putNamespaceProperties(Namespace namespace, Map<String, String> properties) {
    namespaceDb.compute(namespace, (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("Namespace does not exist: " + namespace);
      } else {
        v.putAll(properties);
        return v;
      }
    });
  }

  /**
   * Remove properties from existing namespace.
   */
  void removeNamespaceProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    namespaceDb.compute(namespace, (k, v) -> {
      if (v == null) {
        throw new IllegalStateException("Namespace does not exist: " + namespace);
      } else {
        Map<String, String> newProperties = new HashMap<>(v);
        properties.forEach(newProperties::remove);
        return newProperties;
      }
    });
  }

  /**
   * Remove the namespace from the namespace db.
   * This method returns {@code true} if the namespace was successfully removed,
   * and {@code false} otherwise.
   */
  boolean removeNamespace(Namespace namespace) {
    return namespaceDb.remove(namespace) != null;
  }

  /**
   * Get the list of namespaces in the namespace db.
   */
  ImmutableSet<Namespace> getNamespaceList() {
    return ImmutableSet.copyOf(namespaceDb.keySet());
  }

  /**
   * Create the table entry if it does not already exist.
   * Otherwise, if the existing location matches the {@code expectedOldLocation},
   * update the entry with {@code newLocation}.
   * If the namespace does not exist,
   * put the namespace entry with empty properties as well.
   */
  void createOrUpdateTableEntry(TableIdentifier tableIdentifier, String expectedOldLocation, String newLocation) {
    putNamespaceEntryIfAbsent(tableIdentifier.namespace(), ImmutableMap.of());
    tableDb.compute(tableIdentifier, (k, existingLocation) -> {
      if (!Objects.equal(existingLocation, expectedOldLocation)) {
        throw new CommitFailedException("Table: %s cannot be updated from '%s' to '%s'" +
          " because it has been concurrently modified to '%s'",
          tableIdentifier, expectedOldLocation, newLocation, existingLocation);
      }
      return newLocation;
    });
  }

  /**
   * Get table location.
   * This method returns {@code null} if the table does not exist.
   */
  String getTableLocation(TableIdentifier tableIdentifier) {
    return tableDb.get(tableIdentifier);
  }

  /**
   * Remove table from the table db.
   * This method returns {@code true} if the table was successfully removed,
   * and {@code false} otherwise.
   */
  boolean removeTable(TableIdentifier tableIdentifier) {
    return tableDb.remove(tableIdentifier) != null;
  }

  /**
   * Get the set of table identifiers.
   */
  ImmutableSet<TableIdentifier> getTableList() {
    return ImmutableSet.copyOf(tableDb.keySet());
  }

  /**
   * Move table entry from source ('from') to destination ('to').
   */
  void moveTableEntry(TableIdentifier from, TableIdentifier to) {
    String fromLocation = tableDb.remove(from);
    if (fromLocation != null) {
      putNamespaceEntryIfAbsent(to.namespace(), ImmutableMap.of());
      tableDb.put(to, fromLocation);
    } else {
      throw new IllegalStateException(
        String.format("Failed to rename 'from' table: %s to 'to' table: %s. The 'from' location is null!", from, to));
    }
  }

  @Override
  public void close() {
    namespaceDb.clear();
    tableDb.clear();
  }
}
