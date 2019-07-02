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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import java.util.Arrays;

/**
 * Identifies a table in iceberg catalog.
 */
public class TableIdentifier {

  private static final Splitter DOT = Splitter.on('.');

  private final Namespace namespace;
  private final String name;

  public static TableIdentifier of(String... names) {
    Preconditions.checkArgument(names.length > 0, "Cannot create table identifier without a table name");
    return new TableIdentifier(Namespace.of(Arrays.copyOf(names, names.length - 1)), names[names.length - 1]);
  }

  public static TableIdentifier of(Namespace namespace, String name) {
    return new TableIdentifier(namespace, name);
  }

  public static TableIdentifier parse(String identifier) {
    Iterable<String> parts = DOT.split(identifier);
    return TableIdentifier.of(Iterables.toArray(parts, String.class));
  }

  private TableIdentifier(Namespace namespace, String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Invalid table name %s", name);
    this.namespace = namespace;
    this.name = name;
  }

  /**
   * Whether the namespace is empty.
   * @return true if the namespace is empty, false otherwise
   */
  public boolean hasNamespace() {
    return !namespace.isEmpty();
  }

  /**
   * @return the identifier namespace
   */
  public Namespace namespace() {
    return namespace;
  }

  /**
   * @return the identifier name
   */
  public String name() {
    return name;
  }

  public String toString() {
    return namespace.toString() + "." + name;
  }
}
