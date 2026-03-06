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
package org.apache.iceberg.view;

import java.util.Objects;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Identifies the storage table backing a materialized view.
 *
 * <p>A storage table holds the precomputed results of a materialized view query. It is a regular
 * Iceberg table referenced from {@link ViewVersion} metadata via the {@code storage-table} field.
 *
 * <p>The catalog field is optional. When absent, the storage table resides in the same catalog as
 * the materialized view itself.
 */
public class StorageTableIdentifier {

  private final Namespace namespace;
  private final String name;
  private final String catalog;

  /**
   * Creates a StorageTableIdentifier within the same catalog as the materialized view.
   *
   * @param namespace the namespace of the storage table
   * @param name the name of the storage table
   * @return a StorageTableIdentifier
   */
  public static StorageTableIdentifier of(Namespace namespace, String name) {
    return new StorageTableIdentifier(namespace, name, null);
  }

  /**
   * Creates a StorageTableIdentifier in an explicit catalog.
   *
   * @param catalog the catalog containing the storage table
   * @param namespace the namespace of the storage table
   * @param name the name of the storage table
   * @return a StorageTableIdentifier
   */
  public static StorageTableIdentifier of(String catalog, Namespace namespace, String name) {
    return new StorageTableIdentifier(namespace, name, catalog);
  }

  /**
   * Creates a StorageTableIdentifier from an existing {@link TableIdentifier}, without a catalog.
   *
   * @param identifier the table identifier
   * @return a StorageTableIdentifier
   */
  public static StorageTableIdentifier from(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier != null, "Cannot create StorageTableIdentifier from null TableIdentifier");
    return new StorageTableIdentifier(identifier.namespace(), identifier.name(), null);
  }

  private StorageTableIdentifier(Namespace namespace, String name, String catalog) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid storage table name: null or empty");
    Preconditions.checkArgument(namespace != null, "Invalid namespace: null");
    this.namespace = namespace;
    this.name = name;
    this.catalog = catalog;
  }

  /** Returns the namespace of the storage table. */
  public Namespace namespace() {
    return namespace;
  }

  /** Returns the name of the storage table. */
  public String name() {
    return name;
  }

  /**
   * Returns the catalog of the storage table, or null if it resides in the same catalog as the
   * materialized view.
   */
  public String catalog() {
    return catalog;
  }

  /** Returns true if an explicit catalog is set. */
  public boolean hasCatalog() {
    return catalog != null && !catalog.isEmpty();
  }

  /** Returns a {@link TableIdentifier} view of this identifier (catalog is dropped). */
  public TableIdentifier toTableIdentifier() {
    return TableIdentifier.of(namespace, name);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }
    StorageTableIdentifier that = (StorageTableIdentifier) other;
    return namespace.equals(that.namespace)
        && name.equals(that.name)
        && Objects.equals(catalog, that.catalog);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name, catalog);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (hasCatalog()) {
      sb.append(catalog).append('.');
    }
    if (!namespace.isEmpty()) {
      sb.append(namespace).append('.');
    }
    sb.append(name);
    return sb.toString();
  }
}
