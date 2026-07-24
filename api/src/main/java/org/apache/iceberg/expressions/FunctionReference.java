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
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * Represents a function reference in an expression. A function reference identifies a function by
 * name, optionally qualified with a catalog.
 */
public class FunctionReference implements Serializable {
  private final String catalog;
  private final List<String> identifier;

  private FunctionReference(String catalog, List<String> identifier) {
    Preconditions.checkArgument(
        identifier != null && !identifier.isEmpty(), "Function identifier cannot be null or empty");
    this.catalog = catalog;
    this.identifier = ImmutableList.copyOf(identifier);
  }

  public static FunctionReference of(String name) {
    Preconditions.checkArgument(name != null && !name.isEmpty(), "Function name cannot be empty");
    return new FunctionReference(null, ImmutableList.of(name));
  }

  public static FunctionReference of(List<String> identifier) {
    return new FunctionReference(null, identifier);
  }

  public static FunctionReference of(String catalog, List<String> identifier) {
    return new FunctionReference(catalog, identifier);
  }

  public String catalog() {
    return catalog;
  }

  public List<String> identifier() {
    return identifier;
  }

  /** Returns the last element of the identifier (the function name). */
  public String name() {
    return identifier.get(identifier.size() - 1);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FunctionReference)) {
      return false;
    }
    FunctionReference that = (FunctionReference) o;
    return Objects.equals(catalog, that.catalog) && Objects.equals(identifier, that.identifier);
  }

  @Override
  public int hashCode() {
    return Objects.hash(catalog, identifier);
  }

  @Override
  public String toString() {
    if (catalog != null) {
      return catalog + "." + String.join(".", identifier);
    }
    return String.join(".", identifier);
  }
}
