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

package org.apache.iceberg.spark;

import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

/**
 * This is used to fix primitive types to match a table schema. Some types, like binary and fixed,
 * are converted to the same Spark type. Conversion back can produce only one, which may not be
 * correct. This uses a reference schema to override types that were lost in round-trip conversion.
 */
class FixupTypes extends TypeUtil.CustomOrderSchemaVisitor<Type> {
  private final Schema referenceSchema;
  private Type sourceType;

  static Schema fixup(Schema schema, Schema referenceSchema) {
    return new Schema(TypeUtil.visit(schema,
        new FixupTypes(referenceSchema)).asStructType().fields());
  }

  private FixupTypes(Schema referenceSchema) {
    this.referenceSchema = referenceSchema;
    this.sourceType = referenceSchema.asStruct();
  }

  @Override
  public Type schema(Schema schema, Supplier<Type> future) {
    this.sourceType = referenceSchema.asStruct();
    return future.get();
  }

  @Override
  public Type struct(Types.StructType struct, Iterable<Type> fieldTypes) {
    Preconditions.checkArgument(sourceType.isStructType(), "Not a struct: %s", sourceType);

    List<Types.NestedField> fields = struct.fields();
    int length = fields.size();

    List<Type> types = Lists.newArrayList(fieldTypes);
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(length);
    boolean hasChange = false;
    for (int i = 0; i < length; i += 1) {
      Types.NestedField field = fields.get(i);
      Type resultType = types.get(i);

      if (field.type() == resultType) {
        newFields.add(field);

      } else if (field.isRequired()) {
        hasChange = true;
        newFields.add(Types.NestedField.required(field.fieldId(), field.name(), resultType));

      } else {
        hasChange = true;
        newFields.add(Types.NestedField.optional(field.fieldId(), field.name(), resultType));
      }
    }

    if (hasChange) {
      return Types.StructType.of(newFields);
    }

    return struct;
  }

  @Override
  public Type field(Types.NestedField field, Supplier<Type> future) {
    Preconditions.checkArgument(sourceType.isStructType(), "Not a struct: %s", sourceType);

    Types.StructType sourceStruct = sourceType.asStructType();
    this.sourceType = sourceStruct.field(field.fieldId()).type();
    try {
      return future.get();
    } finally {
      sourceType = sourceStruct;
    }
  }

  @Override
  public Type list(Types.ListType list, Supplier<Type> elementTypeFuture) {
    Preconditions.checkArgument(sourceType.isListType(), "Not a list: %s", sourceType);

    Types.ListType sourceList = sourceType.asListType();
    this.sourceType = sourceList.elementType();
    try {
      Type elementType = elementTypeFuture.get();
      if (list.elementType() == elementType) {
        return list;
      }

      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(list.elementId(), elementType);
      } else {
        return Types.ListType.ofRequired(list.elementId(), elementType);
      }

    } finally {
      this.sourceType = sourceList;
    }
  }

  @Override
  public Type map(Types.MapType map, Supplier<Type> keyTypeFuture, Supplier<Type> valueTypeFuture) {
    Preconditions.checkArgument(sourceType.isMapType(), "Not a map: %s", sourceType);

    Types.MapType sourceMap = sourceType.asMapType();
    try {
      this.sourceType = sourceMap.keyType();
      Type keyType = keyTypeFuture.get();

      this.sourceType = sourceMap.valueType();
      Type valueType = valueTypeFuture.get();

      if (map.keyType() == keyType && map.valueType() == valueType) {
        return map;
      }

      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(map.keyId(), map.valueId(), keyType, valueType);
      } else {
        return Types.MapType.ofRequired(map.keyId(), map.valueId(), keyType, valueType);
      }

    } finally {
      this.sourceType = sourceMap;
    }
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    if (sourceType.equals(primitive)) {
      return primitive; // already correct
    }

    switch (primitive.typeId()) {
      case STRING:
        if (sourceType.typeId() == Type.TypeID.UUID) {
          return sourceType;
        }
        break;
      case BINARY:
        if (sourceType.typeId() == Type.TypeID.FIXED) {
          return sourceType;
        }
        break;
      default:
    }
    // nothing to fix up, let validation catch promotion errors
    return primitive;
  }
}
