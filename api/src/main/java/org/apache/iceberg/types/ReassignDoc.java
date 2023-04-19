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

class ReassignDoc extends TypeUtil.CustomOrderSchemaVisitor<Type> {
  private final Schema docSourceSchema;

  ReassignDoc(Schema docSourceSchema) {
    this.docSourceSchema = docSourceSchema;
  }

  @Override
  public Type schema(Schema schema, Supplier<Type> future) {
    return future.get();
  }

  @Override
  public Type struct(Types.StructType struct, Iterable<Type> fieldTypes) {
    List<Types.NestedField> fields = struct.fields();
    int length = fields.size();

    List<Type> types = Lists.newArrayList(fieldTypes);
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(length);
    for (int i = 0; i < length; i += 1) {
      Types.NestedField field = fields.get(i);
      int fieldId = field.fieldId();
      Types.NestedField docField = docSourceSchema.findField(fieldId);

      Preconditions.checkNotNull(docField, "Field " + fieldId + " not found in source schema");

      if (field.isRequired()) {
        newFields.add(
            Types.NestedField.required(fieldId, field.name(), types.get(i), docField.doc()));
      } else {
        newFields.add(
            Types.NestedField.optional(fieldId, field.name(), types.get(i), docField.doc()));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(Types.NestedField field, Supplier<Type> future) {
    return future.get();
  }

  @Override
  public Type list(Types.ListType list, Supplier<Type> elementTypeFuture) {
    Type elementType = elementTypeFuture.get();
    if (list.elementType() == elementType) {
      return list;
    }

    if (list.isElementOptional()) {
      return Types.ListType.ofOptional(list.elementId(), elementType);
    } else {
      return Types.ListType.ofRequired(list.elementId(), elementType);
    }
  }

  @Override
  public Type map(Types.MapType map, Supplier<Type> keyTypeFuture, Supplier<Type> valueTypeFuture) {
    int keyId = map.keyId();
    int valueId = map.valueId();

    Type keyType = keyTypeFuture.get();
    Type valueType = valueTypeFuture.get();

    if (map.isValueOptional()) {
      return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
    } else {
      return Types.MapType.ofRequired(keyId, valueId, keyType, valueType);
    }
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive;
  }
}
