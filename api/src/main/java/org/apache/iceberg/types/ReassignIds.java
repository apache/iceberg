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
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

class ReassignIds extends TypeUtil.CustomOrderSchemaVisitor<Type> {
  private final Schema sourceSchema;
  private final TypeUtil.NextID assignId;
  private Type sourceType;
  private boolean caseSensitive = true;

  ReassignIds(Schema sourceSchema, TypeUtil.NextID nextID) {
    this.sourceSchema = sourceSchema;
    this.assignId = nextID;
  }

  ReassignIds(Schema sourceSchema, TypeUtil.NextID nextID, boolean caseSensitive) {
    this(sourceSchema, nextID);
    this.caseSensitive = caseSensitive;
  }

  @Override
  public Type schema(Schema schema, Supplier<Type> future) {
    this.sourceType = sourceSchema.asStruct();
    try {
      return future.get();
    } finally {
      this.sourceType = null;
    }
  }

  private int id(Types.StructType sourceStruct, String name) {
    Types.NestedField sourceField =
        caseSensitive ? sourceStruct.field(name) : sourceStruct.caseInsensitiveField(name);

    if (sourceField != null) {
      return sourceField.fieldId();
    }

    if (assignId != null) {
      return assignId.get();
    }

    throw new IllegalArgumentException("Field " + name + " not found in source schema");
  }

  @Override
  public Type struct(Types.StructType struct, Iterable<Type> fieldTypes) {
    Preconditions.checkNotNull(sourceType, "Evaluation must start with a schema.");
    Preconditions.checkArgument(sourceType.isStructType(), "Not a struct: %s", sourceType);

    Types.StructType sourceStruct = sourceType.asStructType();
    List<Types.NestedField> fields = struct.fields();
    int length = fields.size();

    List<Type> types = Lists.newArrayList(fieldTypes);
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(length);
    for (int i = 0; i < length; i += 1) {
      Types.NestedField field = fields.get(i);
      int fieldId = id(sourceStruct, field.name());
      if (field.isRequired()) {
        newFields.add(Types.NestedField.required(fieldId, field.name(), types.get(i), field.doc()));
      } else {
        newFields.add(Types.NestedField.optional(fieldId, field.name(), types.get(i), field.doc()));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(Types.NestedField field, Supplier<Type> future) {
    Preconditions.checkArgument(sourceType.isStructType(), "Not a struct: %s", sourceType);

    Types.StructType sourceStruct = sourceType.asStructType();
    Types.NestedField sourceField =
        caseSensitive
            ? sourceStruct.field(field.name())
            : sourceStruct.caseInsensitiveField(field.name());
    if (sourceField != null) {
      this.sourceType = sourceField.type();
      try {
        return future.get();
      } finally {
        sourceType = sourceStruct;
      }

    } else if (assignId != null) {
      // there is no corresponding field in the id source schema, assign fresh IDs for the type
      return TypeUtil.assignFreshIds(field.type(), assignId);

    } else {
      throw new IllegalArgumentException("Field " + field.name() + " not found in source schema");
    }
  }

  @Override
  public Type list(Types.ListType list, Supplier<Type> elementTypeFuture) {
    Preconditions.checkArgument(sourceType.isListType(), "Not a list: %s", sourceType);

    Types.ListType sourceList = sourceType.asListType();
    int sourceElementId = sourceList.elementId();

    this.sourceType = sourceList.elementType();
    try {
      if (list.isElementOptional()) {
        return Types.ListType.ofOptional(sourceElementId, elementTypeFuture.get());
      } else {
        return Types.ListType.ofRequired(sourceElementId, elementTypeFuture.get());
      }

    } finally {
      this.sourceType = sourceList;
    }
  }

  @Override
  public Type map(Types.MapType map, Supplier<Type> keyTypeFuture, Supplier<Type> valueTypeFuture) {
    Preconditions.checkArgument(sourceType.isMapType(), "Not a map: %s", sourceType);

    Types.MapType sourceMap = sourceType.asMapType();
    int sourceKeyId = sourceMap.keyId();
    int sourceValueId = sourceMap.valueId();

    try {
      this.sourceType = sourceMap.keyType();
      Type keyType = keyTypeFuture.get();

      this.sourceType = sourceMap.valueType();
      Type valueType = valueTypeFuture.get();

      if (map.isValueOptional()) {
        return Types.MapType.ofOptional(sourceKeyId, sourceValueId, keyType, valueType);
      } else {
        return Types.MapType.ofRequired(sourceKeyId, sourceValueId, keyType, valueType);
      }

    } finally {
      this.sourceType = sourceMap;
    }
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive; // nothing to reassign
  }
}
