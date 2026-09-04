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
package org.apache.iceberg.expressions;

import java.io.Serializable;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Represents a function reference in an expression. A function reference identifies a function by
 * name, optionally qualified with a catalog.
 *
 * <p>Use {@link Expressions#function(String...)} and its overloads to create instances.
 */
public class FunctionReference implements Serializable {
  private final String catalog;
  private final List<String> identifier;

  FunctionReference(String catalog, List<String> identifier) {
    Preconditions.checkArgument(
        identifier != null && !identifier.isEmpty(), "Invalid function identifier: %s", identifier);
    Preconditions.checkArgument(
        identifier.stream().noneMatch(part -> part == null || part.isEmpty()),
        "Invalid function identifier (empty or null part): %s",
        identifier);
    this.catalog = catalog;
    // not an immutable list so that Kryo can deserialize this class
    this.identifier = Lists.newArrayList(identifier);
  }

  public String catalog() {
    return catalog;
  }

  public List<String> identifier() {
    return identifier;
  }

  /** Returns the name of the function, without the catalog or namespace. */
  public String name() {
    return identifier.get(identifier.size() - 1);
  }

  @Override
  public String toString() {
    if (catalog != null) {
      return catalog + "." + String.join(".", identifier);
    }
    return String.join(".", identifier);
  }
}
