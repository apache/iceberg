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
package org.apache.iceberg.flink.sink.dynamic;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * A RowDataEvolver is responsible to change the input RowData to make it compatible with the target
 * schema. This is done when
 *
 * <ol>
 *   <li>The input schema has fewer fields than the target schema.
 *   <li>The table types are wider than the input type.
 *   <li>The field order differs for source and target schema.
 * </ol>
 *
 * <p>The resolution is as follows:
 *
 * <ol>
 *   <li>In the first case, we would add a null values for the missing field (if the field is
 *       optional).
 *   <li>In the second case, we would convert the data for the input field to a wider type, e.g. int
 *       (input type) => long (table type).
 *   <li>In the third case, we would rearrange the input data to match the target table.
 * </ol>
 */
class RowDataEvolver {
  private RowDataEvolver() {}

  public static RowData convert(RowData sourceData, Schema sourceSchema, Schema targetSchema) {
    return convertStruct(
        sourceData, FlinkSchemaUtil.convert(sourceSchema), FlinkSchemaUtil.convert(targetSchema));
  }

  private static Object convert(Object object, LogicalType sourceType, LogicalType targetType) {
    if (object == null) {
      return null;
    }

    switch (targetType.getTypeRoot()) {
      case BOOLEAN:
      case INTEGER:
      case FLOAT:
      case VARCHAR:
      case DATE:
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
      case BINARY:
      case VARBINARY:
        return object;
      case DOUBLE:
        if (object instanceof Float) {
          return ((Float) object).doubleValue();
        } else {
          return object;
        }
      case BIGINT:
        if (object instanceof Integer) {
          return ((Integer) object).longValue();
        } else {
          return object;
        }
      case DECIMAL:
        DecimalType toDecimalType = (DecimalType) targetType;
        DecimalData decimalData = (DecimalData) object;
        if (((DecimalType) sourceType).getPrecision() == toDecimalType.getPrecision()) {
          return object;
        } else {
          return DecimalData.fromBigDecimal(
              decimalData.toBigDecimal(), toDecimalType.getPrecision(), toDecimalType.getScale());
        }
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        if (object instanceof Integer) {
          LocalDateTime dateTime =
              LocalDateTime.of(LocalDate.ofEpochDay((Integer) object), LocalTime.MIN);
          return TimestampData.fromLocalDateTime(dateTime);
        } else {
          return object;
        }
      case ROW:
        return convertStruct((RowData) object, (RowType) sourceType, (RowType) targetType);
      case ARRAY:
        return convertArray((ArrayData) object, (ArrayType) sourceType, (ArrayType) targetType);
      case MAP:
        return convertMap((MapData) object, (MapType) sourceType, (MapType) targetType);
      default:
        throw new UnsupportedOperationException("Not a supported type: " + targetType);
    }
  }

  private static RowData convertStruct(RowData sourceData, RowType sourceType, RowType targetType) {
    GenericRowData targetData = new GenericRowData(targetType.getFields().size());
    List<RowType.RowField> targetFields = targetType.getFields();
    for (int i = 0; i < targetFields.size(); i++) {
      RowType.RowField targetField = targetFields.get(i);

      int sourceFieldId = sourceType.getFieldIndex(targetField.getName());
      if (sourceFieldId == -1) {
        if (targetField.getType().isNullable()) {
          targetData.setField(i, null);
        } else {
          throw new IllegalArgumentException(
              String.format(
                  "Field %s in target schema %s is non-nullable but does not exist in source schema.",
                  i + 1, targetType));
        }
      } else {
        RowData.FieldGetter getter =
            RowData.createFieldGetter(sourceType.getTypeAt(sourceFieldId), sourceFieldId);
        targetData.setField(
            i,
            convert(
                getter.getFieldOrNull(sourceData),
                sourceType.getFields().get(sourceFieldId).getType(),
                targetField.getType()));
      }
    }

    return targetData;
  }

  private static ArrayData convertArray(
      ArrayData sourceData, ArrayType sourceType, ArrayType targetType) {
    LogicalType fromElementType = sourceType.getElementType();
    LogicalType toElementType = targetType.getElementType();
    ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(fromElementType);
    Object[] convertedArray = new Object[sourceData.size()];
    for (int i = 0; i < convertedArray.length; i++) {
      convertedArray[i] =
          convert(elementGetter.getElementOrNull(sourceData, i), fromElementType, toElementType);
    }
    return new GenericArrayData(convertedArray);
  }

  private static MapData convertMap(MapData sourceData, MapType sourceType, MapType targetType) {
    LogicalType fromMapKeyType = sourceType.getKeyType();
    LogicalType fromMapValueType = sourceType.getValueType();
    LogicalType toMapKeyType = targetType.getKeyType();
    LogicalType toMapValueType = targetType.getValueType();
    ArrayData keyArray = sourceData.keyArray();
    ArrayData valueArray = sourceData.valueArray();
    ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(fromMapKeyType);
    ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(fromMapValueType);
    Map<Object, Object> convertedMap = Maps.newLinkedHashMap();
    for (int i = 0; i < keyArray.size(); ++i) {
      convertedMap.put(
          convert(keyGetter.getElementOrNull(keyArray, i), fromMapKeyType, toMapKeyType),
          convert(valueGetter.getElementOrNull(valueArray, i), fromMapValueType, toMapValueType));
    }

    return new GenericMapData(convertedMap);
  }
}
