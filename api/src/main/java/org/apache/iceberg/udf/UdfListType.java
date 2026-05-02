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

/** A UDF list type with an element type. */
public final class UdfListType implements UdfType {

  private final UdfType elementType;

  public static UdfListType of(UdfType elementType) {
    Preconditions.checkArgument(elementType != null, "Invalid element type: null");
    return new UdfListType(elementType);
  }

  private UdfListType(UdfType elementType) {
    this.elementType = elementType;
  }

  @Override
  public TypeId typeId() {
    return TypeId.LIST;
  }

  @Override
  public UdfListType asListType() {
    return this;
  }

  public UdfType elementType() {
    return elementType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfListType)) {
      return false;
    }

    return Objects.equals(elementType, ((UdfListType) o).elementType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(UdfListType.class, elementType);
  }

  @Override
  public String toString() {
    return String.format("list<%s>", elementType);
  }
}
