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
   * Put the namespace and properties in the namespace db.
   */
  void putNamespaceEntry(Namespace namespace, Map<String, String> properties) {
    namespaceDb.put(namespace, ImmutableMap.copyOf(properties));
  }

  /**
   * Get a copy of the namespace properties.
   * This method returns {@code null} if the namespace does not exist.
   */
  ImmutableMap<String, String> namespaceProperties(Namespace namespace) {
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
        Map<String, String> newProperties = new HashMap<>(v);
        newProperties.putAll(properties);
        return newProperties;
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
  ImmutableSet<Namespace> listNamespaces() {
    return ImmutableSet.copyOf(namespaceDb.keySet());
  }

  /**
   * Create the table entry if it does not already exist.
   * Otherwise, if the existing location matches the {@code expectedOldLocation},
   * update the entry with {@code newLocation}.
   */
  void createOrUpdateTableEntry(TableIdentifier identifier, String expectedOldLocation, String newLocation) {
    tableDb.compute(identifier, (k, existingLocation) -> {
      if (!Objects.equal(existingLocation, expectedOldLocation)) {
        throw new CommitFailedException("Cannot create/update table %s metadata location from %s to %s" +
          " because it has been concurrently modified to %s",
          identifier, expectedOldLocation, newLocation, existingLocation);
      }
      return newLocation;
    });
  }

  /**
   * Get current metadata location for the table.
   * This method returns {@code null} if the table does not exist.
   */
  String currentMetadataLocation(TableIdentifier identifier) {
    return tableDb.get(identifier);
  }

  /**
   * Remove table from the table db.
   * This method returns {@code true} if the table was successfully removed,
   * and {@code false} otherwise.
   */
  boolean removeTable(TableIdentifier identifier) {
    return tableDb.remove(identifier) != null;
  }

  /**
   * Get the set of table identifiers.
   */
  ImmutableSet<TableIdentifier> listTables() {
    return ImmutableSet.copyOf(tableDb.keySet());
  }

  /**
   * Move table entry from {@code fromIdentifier} to {@code toIdentifier}.
   */
  void moveTableEntry(TableIdentifier fromIdentifier, TableIdentifier toIdentifier) {
    String fromLocation = tableDb.remove(fromIdentifier);
    if (fromLocation != null) {
      tableDb.put(toIdentifier, fromLocation);
    } else {
      throw new IllegalStateException(
        String.format("Cannot rename from %s to %s because source table does not exist", fromIdentifier, toIdentifier));
    }
  }

  @Override
  public void close() {
    namespaceDb.clear();
    tableDb.clear();
  }
}
