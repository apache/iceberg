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
  private Type sourceType;

  ReassignIds(Schema sourceSchema) {
    this.sourceSchema = sourceSchema;
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
      int sourceFieldId = sourceStruct.field(field.name()).fieldId();
      if (field.isRequired()) {
        newFields.add(Types.NestedField.required(sourceFieldId, field.name(), types.get(i), field.doc()));
      } else {
        newFields.add(Types.NestedField.optional(sourceFieldId, field.name(), types.get(i), field.doc()));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(Types.NestedField field, Supplier<Type> future) {
    Preconditions.checkArgument(sourceType.isStructType(), "Not a struct: %s", sourceType);

    Types.StructType sourceStruct = sourceType.asStructType();
    Types.NestedField sourceField = sourceStruct.field(field.name());
    if (sourceField == null) {
      throw new IllegalArgumentException("Field " + field.name() + " not found in source schema");
    }

    this.sourceType = sourceField.type();
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
