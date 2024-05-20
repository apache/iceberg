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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.CharType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.sql.types.VarcharType;

class SparkTypeToType extends SparkTypeVisitor<Type> {
  private final StructType root;
  private int nextId = 0;

  private boolean isAllNullableField = SparkSQLProperties.SET_FIELD_NULLABLE_DEFAULT;

  SparkTypeToType() {
    this.root = null;
  }

  SparkTypeToType(StructType root) {
    this.root = root;
    // the root struct's fields use the first ids
    this.nextId = root.fields().length;

    String nullableFieldStr =
        SparkSession.active()
            .conf()
            .get(SparkSQLProperties.SET_FIELD_NULLABLE, String.valueOf(isAllNullableField));
    this.isAllNullableField = Boolean.parseBoolean(nullableFieldStr);
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  @Override
  @SuppressWarnings("ReferenceEquality")
  public Type struct(StructType struct, List<Type> types) {
    StructField[] fields = struct.fields();
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

      String doc = field.getComment().isDefined() ? field.getComment().get() : null;

      if (field.nullable() || isAllNullableField) {
        newFields.add(Types.NestedField.optional(id, field.name(), type, doc));
      } else {
        newFields.add(Types.NestedField.required(id, field.name(), type, doc));
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
    if (array.containsNull() || isAllNullableField) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(MapType map, Type keyType, Type valueType) {
    if (map.valueContainsNull() || isAllNullableField) {
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

    } else if (atomic instanceof StringType
        || atomic instanceof CharType
        || atomic instanceof VarcharType) {
      return Types.StringType.get();

    } else if (atomic instanceof DateType) {
      return Types.DateType.get();

    } else if (atomic instanceof TimestampType) {
      return Types.TimestampType.withZone();

    } else if (atomic instanceof TimestampNTZType) {
      return Types.TimestampType.withoutZone();

    } else if (atomic instanceof DecimalType) {
      return Types.DecimalType.of(
          ((DecimalType) atomic).precision(), ((DecimalType) atomic).scale());
    } else if (atomic instanceof BinaryType) {
      return Types.BinaryType.get();
    }

    throw new UnsupportedOperationException("Not a supported type: " + atomic.catalogString());
  }
}
