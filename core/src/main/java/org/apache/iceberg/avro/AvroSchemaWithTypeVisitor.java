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

package org.apache.iceberg.avro;

import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/**
 * Avro {@link Schema} with expected Iceberg schema visitor. See {@link #schemaEvolution}, this class is for schema
 * evolution reading. The avro schema can evolve into the expected Iceberg type.
 */
public abstract class AvroSchemaWithTypeVisitor<T> extends AvroWithPartnerSchemaVisitor<Type, T> {
  public static <T> T visit(org.apache.iceberg.Schema iSchema, Schema schema, AvroSchemaWithTypeVisitor<T> visitor) {
    return visit(iSchema.asStruct(), schema, visitor);
  }

  @Override
  public boolean schemaEvolution() {
    return true;
  }

  @Override
  public boolean isMapType(Type type) {
    return type != null && type.isMapType();
  }

  @Override
  public boolean isValidMapKey(Type type) {
    return type == null || type instanceof Types.StringType;
  }

  @Override
  public Type arrayElementType(Type arrayType) {
    return arrayType == null ? null : arrayType.asListType().elementType();
  }

  @Override
  public Type mapKeyType(Type mapType) {
    return mapType == null ? null : mapType.asMapType().keyType();
  }

  @Override
  public Type mapValueType(Type mapType) {
    return mapType == null ? null : mapType.asMapType().valueType();
  }

  @Override
  public Type structFieldTypeById(Type structType, int id) {
    Types.NestedField field = structType == null ? null : structType.asStructType().field(id);
    return field == null ? null : field.type();
  }

  @Override
  public Type nullType() {
    return null;
  }
}
