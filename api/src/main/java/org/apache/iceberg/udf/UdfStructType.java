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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;

/**
 * A UDF struct type with an ordered list of named fields. Unlike Iceberg struct types, UDF struct
 * fields do not have field IDs.
 */
public final class UdfStructType implements UdfType {

  private final List<UdfFieldType> fields;

  public static UdfStructType of(UdfFieldType... fields) {
    Preconditions.checkArgument(fields != null, "Invalid fields: null");
    return of(Arrays.asList(fields));
  }

  public static UdfStructType of(List<UdfFieldType> fields) {
    Preconditions.checkArgument(fields != null, "Invalid fields: null");
    return new UdfStructType(ImmutableList.copyOf(fields));
  }

  private UdfStructType(List<UdfFieldType> fields) {
    this.fields = fields;
  }

  @Override
  public TypeId typeId() {
    return TypeId.STRUCT;
  }

  @Override
  public UdfStructType asStructType() {
    return this;
  }

  public List<UdfFieldType> fields() {
    return fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }

    if (!(o instanceof UdfStructType)) {
      return false;
    }

    return Objects.equals(fields, ((UdfStructType) o).fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(UdfStructType.class, fields);
  }

  @Override
  public String toString() {
    return fields.stream()
        .map(UdfFieldType::toString)
        .collect(Collectors.joining(",", "struct<", ">"));
  }
}
