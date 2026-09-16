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
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class ReplaceTypeById extends TypeUtil.SchemaVisitor<Type> {
  private final Map<Integer, Type> replacementsById;

  ReplaceTypeById(Map<Integer, Type> replacementsById) {
    this.replacementsById = replacementsById;
  }

  @Override
  public Type schema(Schema schema, Type structResult) {
    return structResult;
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    List<Types.NestedField> fields = struct.fields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean hasChanged = false;

    for (int i = 0; i < fields.size(); i += 1) {
      Type fieldReplacement = fieldResults.get(i);
      Types.NestedField field = fields.get(i);
      if (field.type() != fieldReplacement) {
        hasChanged = true;
        newFields.add(Types.NestedField.from(field).ofType(fieldReplacement).build());
      } else {
        newFields.add(field);
      }
    }

    if (hasChanged) {
      return Types.StructType.of(newFields);
    }

    return struct;
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return replacementsById.getOrDefault(field.fieldId(), fieldResult);
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    Type elementReplacement = replacementsById.getOrDefault(list.elementId(), elementResult);
    if (list.elementType() != elementReplacement) {
      if (list.isElementRequired()) {
        return Types.ListType.ofRequired(list.elementId(), elementReplacement);
      } else {
        return Types.ListType.ofOptional(list.elementId(), elementReplacement);
      }
    }

    return list;
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    Type keyReplacement = replacementsById.getOrDefault(map.keyId(), keyResult);
    Type valueReplacement = replacementsById.getOrDefault(map.valueId(), valueResult);
    if (map.keyType() != keyReplacement || map.valueType() != valueReplacement) {
      if (map.isValueRequired()) {
        return Types.MapType.ofRequired(
            map.keyId(), map.valueId(), keyReplacement, valueReplacement);
      } else {
        return Types.MapType.ofOptional(
            map.keyId(), map.valueId(), keyReplacement, valueReplacement);
      }
    }

    return map;
  }

  @Override
  public Type variant(Types.VariantType variant) {
    return variant;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive;
  }
}
