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

/** A UDF map type with key and value types. */
public final class UdfMapType implements UdfType {

  private final UdfType keyType;
  private final UdfType valueType;

  public static UdfMapType of(UdfType keyType, UdfType valueType) {
    Preconditions.checkArgument(keyType != null, "Invalid key type: null");
    Preconditions.checkArgument(valueType != null, "Invalid value type: null");
    return new UdfMapType(keyType, valueType);
  }

  private UdfMapType(UdfType keyType, UdfType valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  @Override
  public TypeId typeId() {
    return TypeId.MAP;
  }

  @Override
  public UdfMapType asMapType() {
    return this;
  }

  public UdfType keyType() {
    return keyType;
  }

  public UdfType valueType() {
    return valueType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfMapType)) {
      return false;
    }

    UdfMapType that = (UdfMapType) o;
    return Objects.equals(keyType, that.keyType) && Objects.equals(valueType, that.valueType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(UdfMapType.class, keyType, valueType);
  }

  @Override
  public String toString() {
    return String.format("map<%s,%s>", keyType, valueType);
  }
}
