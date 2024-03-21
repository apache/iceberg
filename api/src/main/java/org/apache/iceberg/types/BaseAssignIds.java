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

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

abstract class BaseAssignIds extends TypeUtil.CustomOrderSchemaVisitor<Type> {
  private final Schema visitingSchema;

  BaseAssignIds() {
    this.visitingSchema = null;
  }

  /**
   * Replaces the ids in a schema with ids from a base schema, or uses nextId to assign a fresh ids.
   *
   * @param visitingSchema current schema that will have ids replaced (for id to name lookup)
   */
  BaseAssignIds(Schema visitingSchema) {
    this.visitingSchema = visitingSchema;
  }

  /**
   * Return an id for qualified field name
   *
   * @param fullName qualified field name
   * @return id to assign on field
   */
  protected abstract int idFor(String fullName);

  private String name(int id) {
    if (visitingSchema != null) {
      return visitingSchema.findColumnName(id);
    }

    return null;
  }

  @Override
  public Type schema(Schema schema, Supplier<Type> future) {
    return future.get();
  }

  @Override
  public Type struct(Types.StructType struct, Iterable<Type> futures) {
    List<Types.NestedField> fields = struct.fields();
    int length = struct.fields().size();

    // assign IDs for this struct's fields first
    List<Integer> newIds = Lists.newArrayListWithExpectedSize(length);
    for (int i = 0; i < length; i += 1) {
      newIds.add(idFor(name(fields.get(i).fieldId())));
    }

    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(length);
    Iterator<Type> types = futures.iterator();
    for (int i = 0; i < length; i += 1) {
      Types.NestedField field = fields.get(i);
      Type type = types.next();
      if (field.isOptional()) {
        newFields.add(Types.NestedField.optional(newIds.get(i), field.name(), type, field.doc()));
      } else {
        newFields.add(Types.NestedField.required(newIds.get(i), field.name(), type, field.doc()));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(Types.NestedField field, Supplier<Type> future) {
    return future.get();
  }

  @Override
  public Type list(Types.ListType list, Supplier<Type> future) {
    int newId = idFor(name(list.elementId()));
    if (list.isElementOptional()) {
      return Types.ListType.ofOptional(newId, future.get());
    } else {
      return Types.ListType.ofRequired(newId, future.get());
    }
  }

  @Override
  public Type map(Types.MapType map, Supplier<Type> keyFuture, Supplier<Type> valueFuture) {
    int newKeyId = idFor(name(map.keyId()));
    int newValueId = idFor(name(map.valueId()));
    if (map.isValueOptional()) {
      return Types.MapType.ofOptional(newKeyId, newValueId, keyFuture.get(), valueFuture.get());
    } else {
      return Types.MapType.ofRequired(newKeyId, newValueId, keyFuture.get(), valueFuture.get());
    }
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    return primitive;
  }
}
