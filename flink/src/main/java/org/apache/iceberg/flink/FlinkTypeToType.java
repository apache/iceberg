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
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
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
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
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
  public Type fields(FieldsDataType fields, List<Type> types) {
    List<Types.NestedField> newFields = Lists.newArrayListWithExpectedSize(types.size());
    boolean isRoot = root == fields;

    List<RowType.RowField> rowFields = ((RowType) fields.getLogicalType()).getFields();
    Preconditions.checkArgument(rowFields.size() == types.size(), "fields list and types list should have same size.");

    for (int i = 0; i < rowFields.size(); i++) {
      int id = isRoot ? i : getNextId();

      RowType.RowField field = rowFields.get(i);
      String name = field.getName();
      String comment = field.getDescription().orElse(null);

      if (field.getType().isNullable()) {
        newFields.add(Types.NestedField.optional(id, name, types.get(i), comment));
      } else {
        newFields.add(Types.NestedField.required(id, name, types.get(i), comment));
      }
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
    // keys in map are not allowed to be null.
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
    } else if (inner instanceof VarBinaryType) {
      return Types.BinaryType.get();
    } else if (inner instanceof BinaryType) {
      BinaryType binaryType = (BinaryType) inner;
      return Types.FixedType.ofLength(binaryType.getLength());
    } else if (inner instanceof FloatType) {
      return Types.FloatType.get();
    } else if (inner instanceof DoubleType) {
      return Types.DoubleType.get();
    } else if (inner instanceof DateType) {
      return Types.DateType.get();
    } else if (inner instanceof TimeType) {
      return Types.TimeType.get();
    } else if (inner instanceof TimestampType) {
      return Types.TimestampType.withoutZone();
    } else if (inner instanceof LocalZonedTimestampType) {
      return Types.TimestampType.withZone();
    } else if (inner instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) inner;
      return Types.DecimalType.of(decimalType.getPrecision(), decimalType.getScale());
    } else {
      throw new UnsupportedOperationException("Not a supported type: " + type.toString());
    }
  }
}
