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
package org.apache.iceberg.delta;

import io.delta.standalone.types.ArrayType;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DataType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.MapType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class DeltaLakeTypeToType extends DeltaLakeDataTypeVisitor<Type> {
  private final StructType root;
  private int nextId = 0;

  DeltaLakeTypeToType() {
    this.root = null;
  }

  DeltaLakeTypeToType(StructType root) {
    this.root = root;
    this.nextId = root.getFields().length;
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  public Type struct(StructType struct, List<Type> types) {
    StructField[] fields = struct.getFields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.length);
    boolean isRoot = root == struct;
    for (int i = 0; i < fields.length; i += 1) {
      StructField field = fields[i];
      Type type = types.get(i);

      int id;
      if (isRoot) {
        // for new conversions, use ordinals for ids in the root struct
        id = i;
      } else {
        id = getNextId();
      }

      String doc =
          field.getMetadata().contains("comment")
              ? field.getMetadata().get("comment").toString()
              : null;

      if (field.isNullable()) {
        newFields.add(Types.NestedField.optional(id, field.getName(), type, doc));
      } else {
        newFields.add(Types.NestedField.required(id, field.getName(), type, doc));
      }
    }

    return Types.StructType.of(newFields);
  }

  @Override
  public Type field(StructField field, Type typeResult) {
    return typeResult;
  }

  @Override
  public Type array(ArrayType array, Type elementType) {
    if (array.containsNull()) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(MapType map, Type keyType, Type valueType) {
    if (map.valueContainsNull()) {
      return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
    } else {
      return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public Type atomic(DataType atomic) {
    if (atomic instanceof BooleanType) {
      return Types.BooleanType.get();

    } else if (atomic instanceof IntegerType
        || atomic instanceof ShortType
        || atomic instanceof ByteType) {
      return Types.IntegerType.get();

    } else if (atomic instanceof LongType) {
      return Types.LongType.get();

    } else if (atomic instanceof FloatType) {
      return Types.FloatType.get();

    } else if (atomic instanceof DoubleType) {
      return Types.DoubleType.get();

    } else if (atomic instanceof StringType) {
      return Types.StringType.get();

    } else if (atomic instanceof DateType) {
      return Types.DateType.get();

    } else if (atomic instanceof TimestampType) {
      return Types.TimestampType.withZone();

    } else if (atomic instanceof DecimalType) {
      return Types.DecimalType.of(
          ((DecimalType) atomic).getPrecision(), ((DecimalType) atomic).getScale());
    } else if (atomic instanceof BinaryType) {
      return Types.BinaryType.get();
    }

    throw new ValidationException("Not a supported type: %s", atomic.getCatalogString());
  }
}
