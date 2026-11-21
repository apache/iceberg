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
import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.SerializableFunction;

/**
 * Represents a specific function implementation that returns a specific type. What arguments the
 * function accepts is not directly exposed as implementations may cast or promote as necessary to
 * apply the function.
 *
 * @param <T> the Java type returned by the function
 */
public class BoundFunctionReference<T> implements Serializable {
  private final String catalog;
  private final CatalogObjectIdentifier identifier;
  private final Type type;
  private final SerializableFunction<List<Object>, T> implementation;

  /**
   * Creates a reference to a resolved function implementation.
   *
   * @param catalog a catalog name or a reserved name, like {@code iceberg_functions}; null refers
   *     to the catalog of the object that stores the expression
   * @param identifier zero or more namespace levels followed by the function name
   * @param type the type returned by the function
   * @param <T> the Java type returned by the function
   * @return a bound function reference
   * @throws IllegalArgumentException if the identifier is null or has no function name, or if the
   *     type is null
   */
  public static <T> BoundFunctionReference<T> of(
      String catalog, CatalogObjectIdentifier identifier, Type type) {
    return new BoundFunctionReference<>(catalog, identifier, type, null);
  }

  /**
   * Creates a reference to a resolved function implementation that can be evaluated.
   *
   * <p>The implementation is called with the evaluated arguments in order. Predicate arguments are
   * passed as {@link Boolean} values, and any argument may be null.
   *
   * @param catalog a catalog name or a reserved name, like {@code iceberg_functions}; null refers
   *     to the catalog of the object that stores the expression
   * @param identifier zero or more namespace levels followed by the function name
   * @param type the type returned by the function
   * @param implementation a function that produces the result from the argument values
   * @param <T> the Java type returned by the function
   * @return a bound function reference
   * @throws IllegalArgumentException if the identifier is null or has no function name, or if the
   *     type or implementation is null
   */
  public static <T> BoundFunctionReference<T> of(
      String catalog,
      CatalogObjectIdentifier identifier,
      Type type,
      SerializableFunction<List<Object>, T> implementation) {
    Preconditions.checkArgument(implementation != null, "Invalid implementation: null");
    return new BoundFunctionReference<>(catalog, identifier, type, implementation);
  }

  BoundFunctionReference(String catalog, CatalogObjectIdentifier identifier, Type type) {
    this(catalog, identifier, type, null);
  }

  private BoundFunctionReference(
      String catalog,
      CatalogObjectIdentifier identifier,
      Type type,
      SerializableFunction<List<Object>, T> implementation) {
    Preconditions.checkArgument(identifier != null, "Invalid function identifier: null");
    Preconditions.checkArgument(
        identifier.length() > 0, "Invalid function identifier: missing function name");
    Preconditions.checkArgument(type != null, "Invalid return type: null");
    this.catalog = catalog;
    this.identifier = identifier;
    this.type = type;
    this.implementation = implementation;
  }

  public String catalog() {
    return catalog;
  }

  public CatalogObjectIdentifier identifier() {
    return identifier;
  }

  public String name() {
    return identifier.level(identifier.length() - 1);
  }

  /**
   * The return type of applying this function.
   *
   * @return return type
   */
  public Type type() {
    return type;
  }

  /**
   * The implementation used to evaluate this function, or null if the function was resolved without
   * one.
   *
   * @return the function implementation, or null
   */
  public SerializableFunction<List<Object>, T> implementation() {
    return implementation;
  }

  @Override
  public String toString() {
    return catalog != null ? catalog + "." + identifier : identifier.toString();
  }
}
