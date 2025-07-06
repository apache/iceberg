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
import java.util.function.Predicate;
import org.apache.iceberg.Schema;

class FindTypeVisitor extends TypeUtil.SchemaVisitor<Type> {
  private final Predicate<Type> predicate;

  FindTypeVisitor(Predicate<Type> predicate) {
    this.predicate = predicate;
  }

  @Override
  public Type schema(Schema schema, Type structResult) {
    return structResult;
  }

  @Override
  public Type struct(Types.StructType struct, List<Type> fieldResults) {
    if (predicate.test(struct)) {
      return struct;
    }

    for (Type fieldType : fieldResults) {
      if (fieldType != null) {
        return fieldType;
      }
    }

    return null;
  }

  @Override
  public Type field(Types.NestedField field, Type fieldResult) {
    return fieldResult;
  }

  @Override
  public Type list(Types.ListType list, Type elementResult) {
    if (predicate.test(list)) {
      return list;
    }

    return elementResult;
  }

  @Override
  public Type map(Types.MapType map, Type keyResult, Type valueResult) {
    if (predicate.test(map)) {
      return map;
    }

    if (keyResult != null) {
      return keyResult;
    }

    return valueResult;
  }

  @Override
  public Type variant(Types.VariantType variant) {
    if (predicate.test(variant)) {
      return variant;
    }

    return null;
  }

  @Override
  public Type primitive(Type.PrimitiveType primitive) {
    if (predicate.test(primitive)) {
      return primitive;
    }

    return null;
  }
}
