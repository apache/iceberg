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

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A generic identifier for any catalog object, such as a table, view, function, or namespace.
 *
 * <p>Structurally identical to {@link TableIdentifier}, consisting of a {@link Namespace} and a
 * name. For namespace-typed objects, the parent namespace goes into {@link #namespace()} and the
 * leaf level goes into {@link #name()}.
 *
 * <p>This class can be used wherever a type-neutral catalog object reference is needed, such as
 * event payloads and query filters. When the object type matters, pair this with {@link
 * CatalogObjectType}.
 */
public class CatalogObjectIdentifier {
  private final Namespace namespace;
  private final String name;

  /**
   * Create a catalog object identifier from a namespace and name.
   *
   * @param namespace the namespace (must not be null)
   * @param name the object name (must not be null or empty)
   * @return a new CatalogObjectIdentifier
   */
  public static CatalogObjectIdentifier of(Namespace namespace, String name) {
    return new CatalogObjectIdentifier(namespace, name);
  }

  /**
   * Create a catalog object identifier from a {@link TableIdentifier}.
   *
   * @param tableIdentifier the table identifier to convert
   * @return a new CatalogObjectIdentifier with the same namespace and name
   */
  public static CatalogObjectIdentifier of(TableIdentifier tableIdentifier) {
    Preconditions.checkArgument(
        tableIdentifier != null, "Cannot create CatalogObjectIdentifier from null identifier");
    return new CatalogObjectIdentifier(tableIdentifier.namespace(), tableIdentifier.name());
  }

  private CatalogObjectIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(namespace != null, "Invalid namespace: null");
    Preconditions.checkArgument(
        name != null && !name.isEmpty(), "Invalid object name: null or empty");
    this.namespace = namespace;
    this.name = name;
  }

  /** Returns the namespace of this identifier. */
  public Namespace namespace() {
    return namespace;
  }

  /** Returns the object name. */
  public String name() {
    return name;
  }

  /** Returns whether the namespace is non-empty. */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * Convert this identifier to a {@link TableIdentifier}.
   *
   * @return a TableIdentifier with the same namespace and name
   */
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

    CatalogObjectIdentifier that = (CatalogObjectIdentifier) other;
    return namespace.equals(that.namespace) && name.equals(that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, name);
  }

  @Override
  public String toString() {
    if (hasNamespace()) {
      return namespace.toString() + "." + name;
    } else {
      return name;
    }
  }
}
