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

import java.util.Arrays;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;

/**
 * Identifies a table in iceberg catalog.
 */
public class TableIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;
  private final String name;

  /**
   * Creates a {@link TableIdentifier} from the given array of name.
   *
   * @param names The names to create the {@link TableIdentifier} from.
   * @return A {@link TableIdentifier} instance from the given array of names.
   * @deprecated Please use {@link TableIdentifier#of(Namespace)} and {@link TableIdentifier#of(Namespace, String)}.
   */
  @Deprecated
  public static TableIdentifier of(String... names) {
    Preconditions.checkArgument(names != null, "Cannot create table identifier from null array");
    Preconditions.checkArgument(names.length > 0, "Cannot create table identifier without a table name");
    return new TableIdentifier(Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  /**
   * Creates a {@link TableIdentifier} from the given {@link Namespace} and simple table name.
   *
   * @param namespace The {@link Namespace} to use for the {@link TableIdentifier}.
   * @param name      The simple table name.
   * @return A {@link TableIdentifier} from the given {@link Namespace} and simple table name.
   */
  public static TableIdentifier of(Namespace namespace, String name) {
    return new TableIdentifier(namespace, name);
  }

  /**
   * Creates a {@link TableIdentifier} with the same name as the given {@link Namespace} instance. This is mostly
   * being used when one wants to create a base {@link TableIdentifier}, such as
   * <code>TableIdentifier baseTableIdentifier = TableIdentifier.of(identifier.namespace())</code>
   *
   * @param namespace The {@link Namespace} to create the base {@link TableIdentifier} from.
   * @return A {@link TableIdentifier} with the same name as the given {@link Namespace} instance
   */
  public static TableIdentifier of(Namespace namespace) {
    Preconditions.checkArgument(namespace != null, "Invalid Namespace: null");
    Preconditions.checkArgument(!namespace.isEmpty(), "Cannot create table identifier from empty namespace");
    String[] levels = namespace.levels();
    return new TableIdentifier(Namespace.of(Arrays.copyOf(levels, levels.length - 1)), levels[levels.length - 1]);
  }

  /**
   * Parses the given identifier into a {@link TableIdentifier} instance.
   *
   * @param identifier The table identifier to parse to a {@link TableIdentifier}
   * @return A {@link TableIdentifier} instance based on the given string.
   * @deprecated Please use {@link TableIdentifier#of(Namespace)} and {@link TableIdentifier#of(Namespace, String)}.
   */
  @Deprecated
  public static TableIdentifier parse(String identifier) {
    Preconditions.checkArgument(identifier != null, "Cannot parse table identifier: null");
    Iterable<String> parts = DOT.split(identifier);
    return TableIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private TableIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid table name: null or empty");
    Preconditions.checkArgument(namespace != null, "Invalid Namespace: null");
    this.namespace = namespace;
    this.name = name;
  }

  /**
   * Whether the namespace is empty.
   * @return true if the namespace is not empty, false otherwise
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * Returns the identifier namespace.
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * Returns the identifier name.
   */
  public String name() {
    return name;
  }

  public TableIdentifier toLowerCase() {
    String[] newLevels = Arrays.stream(namespace().levels())
        .map(String::toLowerCase)
        .toArray(String[]::new);
    String newName = name().toLowerCase();
    return TableIdentifier.of(Namespace.of(newLevels), newName);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    TableIdentifier that = (TableIdentifier) other;
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
