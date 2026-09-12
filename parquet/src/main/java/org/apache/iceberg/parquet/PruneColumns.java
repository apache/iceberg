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
package org.apache.iceberg.parquet;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

class PruneColumns extends TypeWithSchemaVisitor<Type> {
  private final Set<Integer> selectedIds;

  PruneColumns(Set<Integer> selectedIds) {
    Preconditions.checkNotNull(selectedIds, "Selected field ids cannot be null");
    this.selectedIds = selectedIds;
  }

  @Override
  public Type message(StructType expected, MessageType message, List<Type> fields) {
    Types.MessageTypeBuilder builder = Types.buildMessage();

    boolean hasChange = false;
    int fieldCount = 0;
    for (int i = 0; i < fields.size(); i += 1) {
      Type originalField = message.getType(i);
      Type field = fields.get(i);
      Integer fieldId = getId(originalField);
      if (fieldId != null && selectedIds.contains(fieldId)) {
        if (field != null) {
          hasChange = true;
          builder.addField(field);
        } else {
          if (isStruct(originalField, expected.field(fieldId))) {
            hasChange = true;
            builder.addField(originalField.asGroupType().withNewFields(Collections.emptyList()));
          } else {
            builder.addField(originalField);
          }
        }
        fieldCount += 1;
      } else if (field != null) {
        hasChange = true;
        builder.addField(field);
        fieldCount += 1;
      }
    }

    if (hasChange) {
      return builder.named(message.getName());
    } else if (message.getFieldCount() == fieldCount) {
      return message;
    }

    return builder.named(message.getName());
  }

  @Override
  public Type struct(StructType expected, GroupType struct, List<Type> fields) {
    boolean hasChange = false;
    List<Type> filteredFields = Lists.newArrayListWithExpectedSize(fields.size());
    for (int i = 0; i < fields.size(); i += 1) {
      Type originalField = struct.getType(i);
      Type field = fields.get(i);
      Integer fieldId = getId(originalField);
      if (fieldId != null && selectedIds.contains(fieldId)) {
        filteredFields.add(originalField);
      } else if (field != null) {
        filteredFields.add(originalField);
        hasChange = true;
      }
    }

    if (hasChange) {
      return struct.withNewFields(filteredFields);
    } else if (struct.getFieldCount() == filteredFields.size()) {
      return struct;
    } else if (!filteredFields.isEmpty()) {
      return struct.withNewFields(filteredFields);
    }

    return null;
  }

  @Override
  public Type list(ListType expected, GroupType list, Type element) {
    Type repeated = list.getType(0);
    Type originalElement = ParquetSchemaUtil.determineListElementType(list);
    Integer elementId = getId(originalElement);

    if (elementId != null && selectedIds.contains(elementId)) {
      return list;
    } else if (element != null) {
      if (!Objects.equal(element, originalElement)) {
        if (originalElement.isRepetition(Type.Repetition.REPEATED)) {
          return list.withNewFields(element);
        } else {
          return list.withNewFields(repeated.asGroupType().withNewFields(element));
        }
      }
      return list;
    }

    return null;
  }

  @Override
  public Type map(MapType expected, GroupType map, Type key, Type value) {
    GroupType repeated = map.getType(0).asGroupType();
    Type originalKey = repeated.getType(0);
    Type originalValue = repeated.getType(1);

    Integer keyId = getId(originalKey);
    Integer valueId = getId(originalValue);

    if ((keyId != null && selectedIds.contains(keyId))
        || (valueId != null && selectedIds.contains(valueId))) {
      return map;
    } else if (value != null) {
      if (!Objects.equal(value, originalValue)) {
        return map.withNewFields(repeated.withNewFields(originalKey, value));
      }
      return map;
    }

    return null;
  }

  @Override
  public Type variant(
      org.apache.iceberg.types.Types.VariantType expected, GroupType variantGroup, Type variant) {
    return variant;
  }

  @Override
  public Type primitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive) {
    validatePrimitive(expected, primitive, String.join(".", currentPath()));
    return null;
  }

  static void validatePrimitive(
      org.apache.iceberg.types.Type.PrimitiveType expected, PrimitiveType primitive, String path) {
    if (expected != null
        && (expected.typeId() == TypeID.GEOMETRY || expected.typeId() == TypeID.GEOGRAPHY)) {
      Preconditions.checkArgument(
          TypeUtil.isPromotionAllowed(MessageTypeToType.convertPrimitive(primitive), expected),
          "Cannot read Parquet type %s as Iceberg type %s for field %s",
          primitive,
          expected,
          path);
    }
  }

  static void validateFallbackType(org.apache.iceberg.types.Type expected, Type type, String path) {
    if (expected == null) {
      return;
    }

    if (expected.isPrimitiveType() || type.isPrimitive()) {
      if (expected.isPrimitiveType() && type.isPrimitive()) {
        validatePrimitive(expected.asPrimitiveType(), type.asPrimitiveType(), path);
      }

      return;
    }

    GroupType group = type.asGroupType();
    if (expected.isStructType()) {
      validateFallbackStruct(expected.asStructType(), group, path);
    } else if (expected.isListType()) {
      validateFallbackList(expected.asListType(), group, path);
    } else if (expected.isMapType()) {
      validateFallbackMap(expected.asMapType(), group, path);
    }
  }

  private static void validateFallbackStruct(StructType expected, GroupType struct, String path) {
    List<NestedField> expectedFields = expected.fields();
    int fieldCount = Math.min(expectedFields.size(), struct.getFieldCount());
    for (int i = 0; i < fieldCount; i += 1) {
      Type field = struct.getType(i);
      validateFallbackType(expectedFields.get(i).type(), field, path + "." + field.getName());
    }
  }

  private static void validateFallbackList(ListType expected, GroupType list, String path) {
    Type element = ParquetSchemaUtil.determineListElementType(list);
    String elementPath = path;
    if (!element.isRepetition(Type.Repetition.REPEATED)) {
      elementPath += "." + list.getFieldName(0);
    }

    validateFallbackType(expected.elementType(), element, elementPath + "." + element.getName());
  }

  private static void validateFallbackMap(MapType expected, GroupType map, String path) {
    GroupType repeated = map.getType(0).asGroupType();
    String repeatedPath = path + "." + repeated.getName();
    if (repeated.getFieldCount() == 2) {
      Type key = repeated.getType(0);
      Type value = repeated.getType(1);
      validateFallbackType(expected.keyType(), key, repeatedPath + "." + key.getName());
      validateFallbackType(expected.valueType(), value, repeatedPath + "." + value.getName());

    } else if (repeated.getFieldCount() == 1) {
      Type keyOrValue = repeated.getType(0);
      org.apache.iceberg.types.Type expectedKeyOrValue =
          keyOrValue.getName().equalsIgnoreCase("key") ? expected.keyType() : expected.valueType();
      validateFallbackType(
          expectedKeyOrValue, keyOrValue, repeatedPath + "." + keyOrValue.getName());
    }
  }

  private Integer getId(Type type) {
    return type.getId() == null ? null : type.getId().intValue();
  }

  private boolean isStruct(Type field, NestedField expected) {
    if (field.isPrimitive() || expected.type().isVariantType()) {
      return false;
    } else {
      GroupType groupType = field.asGroupType();
      LogicalTypeAnnotation logicalTypeAnnotation = groupType.getLogicalTypeAnnotation();
      return !LogicalTypeAnnotation.mapType().equals(logicalTypeAnnotation)
          && !LogicalTypeAnnotation.listType().equals(logicalTypeAnnotation);
    }
  }
}
