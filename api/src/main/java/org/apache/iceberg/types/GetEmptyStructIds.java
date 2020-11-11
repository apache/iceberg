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

package org.apache.iceberg.types;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;

class GetEmptyStructIds extends TypeUtil.SchemaVisitor<Set<Integer>> {
  private final Set<Integer> fieldIds = Sets.newHashSet();

  @Override
  public Set<Integer> schema(Schema schema, Set<Integer> structResult) {
    return fieldIds;
  }

  @Override
  public Set<Integer> struct(Types.StructType struct, List<Set<Integer>> fieldResults) {
    return fieldIds;
  }

  private boolean isEmptyStruct(Types.NestedField field) {
    return field.type().isStructType() && ((Types.StructType) field.type()).fields().isEmpty();
  }

  /**
   * We don't know at this point whether the fields contained within a struct are Metadata (and don't exist in the file)
   * or have been projected in as optional and don't exist in the file. Here we check if all the fields are a combination
   * of those two states.
   * @param field a nested field which may be an empty struct
   * @return true if we need to preserve this struct as it may be needed for its child fields
   */
  private boolean isPotentiallyEmpty(Types.NestedField field) {
    if (!field.type().isStructType()) {
      return false;
    }
    List<Types.NestedField> fields = ((Types.StructType) field.type()).fields();
    return fields.stream().allMatch(f -> f.fieldId() > Integer.MAX_VALUE - 201 || f.isOptional());
  }

  @Override
  public Set<Integer> field(Types.NestedField field, Set<Integer> fieldResult) {
    if (isEmptyStruct(field) || isPotentiallyEmpty(field)) {
      fieldIds.add(field.fieldId());
    }
    return fieldIds;
  }

  @Override
  public Set<Integer> list(Types.ListType list, Set<Integer> elementResult) {
    return fieldIds;
  }

  @Override
  public Set<Integer> map(Types.MapType map, Set<Integer> keyResult, Set<Integer> valueResult) {
    return fieldIds;
  }
}
