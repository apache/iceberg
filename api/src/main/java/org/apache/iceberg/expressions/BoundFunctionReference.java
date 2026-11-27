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

import org.apache.iceberg.catalog.CatalogObjectIdentifier;
import org.apache.iceberg.types.Type;

public class BoundFunctionReference<T> {
  private final String catalog;
  private final CatalogObjectIdentifier identifier;
  private final Type type;

  /**
   * Represents a specific function implementation that returns a specific type. What arguments the
   * function accepts is not directly exposed as implementations my cast or promote as necessary to
   * apply the function.
   *
   * @param catalog catalog name
   * @param identifier qualified identifier
   * @param type return type of the function
   */
  public BoundFunctionReference(String catalog, CatalogObjectIdentifier identifier, Type type) {
    this.catalog = catalog;
    this.identifier = identifier;
    this.type = type;
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
}
