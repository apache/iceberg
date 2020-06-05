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

package org.apache.iceberg.flink;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class FlinkTypeToType extends FlinkTypeVisitor<Type> {
  private final FieldsDataType root;
  private int nextId = 0;

  FlinkTypeToType(FieldsDataType root) {
    this.root = root;
    // the root struct's fields use the first ids
    this.nextId = root.getFieldDataTypes().size();
  }

  private int getNextId() {
    int next = nextId;
    nextId += 1;
    return next;
  }

  @Override
  public Type fields(FieldsDataType dataType, Map<String, Tuple2<String, Type>> types) {
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(types.size());
    boolean isRoot = root == dataType;

    Map<String, DataType> fieldsMap = dataType.getFieldDataTypes();
    int index = 0;
    for (String name : types.keySet()) {
      assert fieldsMap.containsKey(name);
      DataType field = fieldsMap.get(name);
      Tuple2<String, Type> tuple2 = types.get(name);

      int id = isRoot ? index : getNextId();
      if (field.getLogicalType().isNullable()) {
        newFields.add(Types.NestedField.optional(id, name, tuple2.f1, tuple2.f0));
      } else {
        newFields.add(Types.NestedField.required(id, name, tuple2.f1, tuple2.f0));
      }
      index++;
    }
    return Types.StructType.of(newFields);
  }

  @Override
  public Type collection(CollectionDataType collection, Type elementType) {
    if (collection.getElementDataType().getLogicalType().isNullable()) {
      return Types.ListType.ofOptional(getNextId(), elementType);
    } else {
      return Types.ListType.ofRequired(getNextId(), elementType);
    }
  }

  @Override
  public Type map(KeyValueDataType map, Type keyType, Type valueType) {
    if (map.getValueDataType().getLogicalType().isNullable()) {
      return Types.MapType.ofOptional(getNextId(), getNextId(), keyType, valueType);
    } else {
      return Types.MapType.ofRequired(getNextId(), getNextId(), keyType, valueType);
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public Type atomic(AtomicDataType type) {
    LogicalType inner = type.getLogicalType();
    if (inner instanceof VarCharType ||
        inner instanceof CharType) {
      return Types.StringType.get();
    } else if (inner instanceof BooleanType) {
      return Types.BooleanType.get();
    } else if (inner instanceof IntType ||
        inner instanceof SmallIntType ||
        inner instanceof TinyIntType) {
      return Types.IntegerType.get();
    } else if (inner instanceof BigIntType) {
      return Types.LongType.get();
    } else if (inner instanceof VarBinaryType ||
        inner instanceof BinaryType) {
      return Types.BinaryType.get();
    } else if (inner instanceof FloatType) {
      return Types.FloatType.get();
    } else if (inner instanceof DoubleType) {
      return Types.DoubleType.get();
    } else if (inner instanceof DateType) {
      return Types.DateType.get();
    } else if (inner instanceof TimeType) {
      return Types.TimeType.get();
    } else if (inner instanceof TimestampType) {
      return Types.TimestampType.withZone();
    } else if (inner instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) inner;
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    } else {
      throw new UnsupportedOperationException("Not a supported type: " + type.toString());
    }
  }
}
