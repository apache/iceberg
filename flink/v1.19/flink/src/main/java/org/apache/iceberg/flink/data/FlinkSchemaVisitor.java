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
package org.apache.iceberg.flink.data;

import java.util.List;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

abstract class FlinkSchemaVisitor<T> {

  static <T> T visit(RowType flinkType, Schema schema, FlinkSchemaVisitor<T> visitor) {
    return visit(flinkType, schema.asStruct(), visitor);
  }

  private static <T> T visit(LogicalType flinkType, Type iType, FlinkSchemaVisitor<T> visitor) {
    switch (iType.typeId()) {
      case STRUCT:
        return visitRecord(flinkType, iType.asStructType(), visitor);

      case MAP:
        MapType mapType = (MapType) flinkType;
        Types.MapType iMapType = iType.asMapType();
        T key;
        T value;

        Types.NestedField keyField = iMapType.field(iMapType.keyId());
        visitor.beforeMapKey(keyField);
        try {
          key = visit(mapType.getKeyType(), iMapType.keyType(), visitor);
        } finally {
          visitor.afterMapKey(keyField);
        }

        Types.NestedField valueField = iMapType.field(iMapType.valueId());
        visitor.beforeMapValue(valueField);
        try {
          value = visit(mapType.getValueType(), iMapType.valueType(), visitor);
        } finally {
          visitor.afterMapValue(valueField);
        }

        return visitor.map(iMapType, key, value, mapType.getKeyType(), mapType.getValueType());

      case LIST:
        ArrayType listType = (ArrayType) flinkType;
        Types.ListType iListType = iType.asListType();
        T element;

        Types.NestedField elementField = iListType.field(iListType.elementId());
        visitor.beforeListElement(elementField);
        try {
          element = visit(listType.getElementType(), iListType.elementType(), visitor);
        } finally {
          visitor.afterListElement(elementField);
        }

        return visitor.list(iListType, element, listType.getElementType());

      default:
        return visitor.primitive(iType.asPrimitiveType(), flinkType);
    }
  }

  private static <T> T visitRecord(
      LogicalType flinkType, Types.StructType struct, FlinkSchemaVisitor<T> visitor) {
    Preconditions.checkArgument(flinkType instanceof RowType, "%s is not a RowType.", flinkType);
    RowType rowType = (RowType) flinkType;

    int fieldSize = struct.fields().size();
    List<T> results = Lists.newArrayListWithExpectedSize(fieldSize);
    List<LogicalType> fieldTypes = Lists.newArrayListWithExpectedSize(fieldSize);
    List<Types.NestedField> nestedFields = struct.fields();

    for (int i = 0; i < fieldSize; i++) {
      Types.NestedField iField = nestedFields.get(i);
      int fieldIndex = rowType.getFieldIndex(iField.name());
      Preconditions.checkArgument(
          fieldIndex >= 0, "NestedField: %s is not found in flink RowType: %s", iField, rowType);

      LogicalType fieldFlinkType = rowType.getTypeAt(fieldIndex);

      fieldTypes.add(fieldFlinkType);

      visitor.beforeField(iField);
      try {
        results.add(visit(fieldFlinkType, iField.type(), visitor));
      } finally {
        visitor.afterField(iField);
      }
    }

    return visitor.record(struct, results, fieldTypes);
  }

  public T record(Types.StructType iStruct, List<T> results, List<LogicalType> fieldTypes) {
    return null;
  }

  public T list(Types.ListType iList, T element, LogicalType elementType) {
    return null;
  }

  public T map(Types.MapType iMap, T key, T value, LogicalType keyType, LogicalType valueType) {
    return null;
  }

  public T primitive(Type.PrimitiveType iPrimitive, LogicalType flinkPrimitive) {
    return null;
  }

  public void beforeField(Types.NestedField field) {}

  public void afterField(Types.NestedField field) {}

  public void beforeListElement(Types.NestedField elementField) {
    beforeField(elementField);
  }

  public void afterListElement(Types.NestedField elementField) {
    afterField(elementField);
  }

  public void beforeMapKey(Types.NestedField keyField) {
    beforeField(keyField);
  }

  public void afterMapKey(Types.NestedField keyField) {
    afterField(keyField);
  }

  public void beforeMapValue(Types.NestedField valueField) {
    beforeField(valueField);
  }

  public void afterMapValue(Types.NestedField valueField) {
    afterField(valueField);
  }
}
