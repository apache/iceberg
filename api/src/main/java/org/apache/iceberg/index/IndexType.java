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
package org.apache.iceberg.index;

import java.util.Locale;

/**
 * Enum representing the supported index types.
 *
 * <p>An index type defines the algorithm and the underlying data structure that governs the
 * behavior of the index.
 */
public enum IndexType {
  /**
   * Bloom Filter index for probabilistic file testing.
   *
   * <p>Efficient for file skipping on low cardinality columns with equality or in predicates.
   */
  BLOOM("bloom"),

  /**
   * B-Tree index for ordered data access.
   *
   * <p>Efficient for range queries and point lookups on orderable columns.
   */
  BTREE("btree"),

  /**
   * Term index for full-text search capabilities.
   *
   * <p>Efficient for text matching, tokenized search, and keyword lookups on string columns.
   */
  TERM("term"),

  /**
   * IVF (Inverted File) index for vector similarity search.
   *
   * <p>Efficient for approximate nearest neighbor queries on high-dimensional vector columns.
   */
  IVF("ivf");

  private final String name;

  IndexType(String name) {
    this.name = name;
  }

  public String typeName() {
    return name;
  }

  public static IndexType fromString(String typeName) {
    for (IndexType type : IndexType.values()) {
      if (type.name.equalsIgnoreCase(typeName)) {
        return type;
      }
    }

    throw new IllegalArgumentException(
        String.format("Unknown index type: %s", typeName.toLowerCase(Locale.ROOT)));
  }

  @Override
  public String toString() {
    return name;
  }
}
