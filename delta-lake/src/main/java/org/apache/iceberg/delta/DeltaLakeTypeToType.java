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

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.ByteType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.ShortType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
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
    this.nextId = root.fields().size();
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  public Type struct(StructType struct, List<Type> types) {
    List<StructField> fields = struct.fields();
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(fields.size());
    boolean isRoot = root == struct;
    for (int i = 0; i < fields.size(); i += 1) {
      StructField field = fields.get(i);
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
    if (map.isValueContainsNull()) {
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

    } else if (atomic instanceof TimestampNTZType) {
      return Types.TimestampType.withoutZone();

    } else if (atomic instanceof DecimalType) {
      return Types.DecimalType.of(
          ((DecimalType) atomic).getPrecision(), ((DecimalType) atomic).getScale());
    } else if (atomic instanceof BinaryType) {
      return Types.BinaryType.get();
    }

    throw new ValidationException("Not a supported type: %s", atomic.toString());
  }
}
