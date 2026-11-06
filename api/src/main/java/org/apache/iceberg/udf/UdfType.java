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

/**
 * Represents a UDF data type as defined in the UDF spec. UDF types are based on Iceberg types but
 * intentionally omit field IDs and element nullability. Concrete implementations live as static
 * nested classes on {@link UdfTypes}: {@link UdfTypes.PrimitiveType} for primitive and
 * semi-structured types, and the nested types {@link UdfTypes.ListType}, {@link UdfTypes.MapType},
 * and {@link UdfTypes.StructType}.
 */
public interface UdfType {

  enum TypeID {
    PRIMITIVE,
    LIST,
    MAP,
    STRUCT
  }

  TypeID typeId();

  default boolean isPrimitiveType() {
    return typeId() == TypeID.PRIMITIVE;
  }

  default boolean isListType() {
    return typeId() == TypeID.LIST;
  }

  default boolean isMapType() {
    return typeId() == TypeID.MAP;
  }

  default boolean isStructType() {
    return typeId() == TypeID.STRUCT;
  }

  default UdfTypes.PrimitiveType asPrimitiveType() {
    throw new IllegalArgumentException("Not a primitive type: " + this);
  }

  default UdfTypes.ListType asListType() {
    throw new IllegalArgumentException("Not a list type: " + this);
  }

  default UdfTypes.MapType asMapType() {
    throw new IllegalArgumentException("Not a map type: " + this);
  }

  default UdfTypes.StructType asStructType() {
    throw new IllegalArgumentException("Not a struct type: " + this);
  }
}
