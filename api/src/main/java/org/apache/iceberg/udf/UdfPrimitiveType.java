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

import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * A UDF primitive or semi-structured type, encoded as a type string (e.g., {@code int}, {@code
 * string}, {@code decimal(9,2)}, {@code variant}).
 */
public final class UdfPrimitiveType implements UdfType {

  private final String typeString;

  public static UdfPrimitiveType of(String typeString) {
    Preconditions.checkArgument(typeString != null, "Invalid primitive type: null");
    return new UdfPrimitiveType(typeString);
  }

  private UdfPrimitiveType(String typeString) {
    this.typeString = typeString;
  }

  @Override
  public TypeId typeId() {
    return TypeId.PRIMITIVE;
  }

  @Override
  public UdfPrimitiveType asPrimitive() {
    return this;
  }

  /** The primitive or semi-structured type string. */
  public String typeString() {
    return typeString;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfPrimitiveType)) {
      return false;
    }

    return Objects.equals(typeString, ((UdfPrimitiveType) o).typeString);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(typeString);
  }

  @Override
  public String toString() {
    return typeString;
  }
}
