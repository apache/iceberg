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
package org.apache.iceberg.udf;

import java.util.Map;
import java.util.Objects;

/**
 * Represents a UDF data type as defined in the UDF spec. UDF types are based on Iceberg types but
 * intentionally omit field IDs and element nullability. Primitive and semi-structured types (e.g.,
 * "int", "string", "decimal(9,2)", "variant") are represented as type strings. Nested types
 * (struct, list, map) are represented as structured JSON objects.
 */
public class UdfType {
  private final String primitiveType;
  private final Map<String, Object> nestedType;

  private UdfType(String primitiveType, Map<String, Object> nestedType) {
    this.primitiveType = primitiveType;
    this.nestedType = nestedType;
  }

  /** Creates a UdfType for a primitive or semi-structured type (e.g., "int", "decimal(9,2)"). */
  public static UdfType primitive(String type) {
    if (type == null) {
      throw new IllegalArgumentException("Primitive type string must not be null");
    }

    return new UdfType(type, null);
  }

  /** Creates a UdfType for a nested type (struct, list, or map). */
  public static UdfType nested(Map<String, Object> type) {
    if (type == null) {
      throw new IllegalArgumentException("Nested type map must not be null");
    }

    return new UdfType(null, type);
  }

  /** Returns true if this is a primitive or semi-structured type. */
  public boolean isPrimitive() {
    return primitiveType != null;
  }

  /** Returns the primitive type string, or throws if this is a nested type. */
  public String asPrimitive() {
    if (primitiveType == null) {
      throw new IllegalStateException("Not a primitive type: " + nestedType);
    }

    return primitiveType;
  }

  /** Returns the nested type structure, or throws if this is a primitive type. */
  public Map<String, Object> asNested() {
    if (nestedType == null) {
      throw new IllegalStateException("Not a nested type: " + primitiveType);
    }

    return nestedType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfType)) {
      return false;
    }

    UdfType that = (UdfType) o;
    return Objects.equals(primitiveType, that.primitiveType)
        && Objects.equals(nestedType, that.nestedType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(primitiveType, nestedType);
  }

  @Override
  public String toString() {
    return isPrimitive() ? primitiveType : nestedType.toString();
  }
}
