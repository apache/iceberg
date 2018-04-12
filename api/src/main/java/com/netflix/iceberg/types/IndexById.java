/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.types;

import com.google.common.collect.Maps;
import com.netflix.iceberg.Schema;
import java.util.List;
import java.util.Map;

class IndexById extends TypeUtil.SchemaVisitor<Map<Integer, Types.NestedField>> {
  private final Map<Integer, Types.NestedField> index = Maps.newHashMap();

  @Override
  public Map<Integer, Types.NestedField> schema(Schema schema, Map<Integer, Types.NestedField> structResult) {
    return index;
  }

  @Override
  public Map<Integer, Types.NestedField> struct(Types.StructType struct, List<Map<Integer, Types.NestedField>> fieldResults) {
    return index;
  }

  @Override
  public Map<Integer, Types.NestedField> field(Types.NestedField field, Map<Integer, Types.NestedField> fieldResult) {
    index.put(field.fieldId(), field);
    return null;
  }

  @Override
  public Map<Integer, Types.NestedField> list(Types.ListType list, Map<Integer, Types.NestedField> elementResult) {
    for (Types.NestedField field : list.fields()) {
      index.put(field.fieldId(), field);
    }
    return null;
  }

  @Override
  public Map<Integer, Types.NestedField> map(Types.MapType map, Map<Integer, Types.NestedField> keyResult, Map<Integer, Types.NestedField> valueResult) {
    for (Types.NestedField field : map.fields()) {
      index.put(field.fieldId(), field);
    }
    return null;
  }
}
