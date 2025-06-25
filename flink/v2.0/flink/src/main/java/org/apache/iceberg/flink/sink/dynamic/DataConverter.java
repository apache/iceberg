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
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

/**
 * {@link org.apache.iceberg.flink.sink.dynamic.DataConverter} is responsible to change the input
 * data to make it compatible with the target schema. This is done when
 *
 * <ul>
 *   <li>The input schema has fewer fields than the target schema.
 *   <li>The table types are wider than the input type.
 *   <li>The field order differs for source and target schema.
 * </ul>
 *
 * <p>The resolution is as follows:
 *
 * <ul>
 *   <li>In the first case, we would add a null values for the missing field (if the field is
 *       optional).
 *   <li>In the second case, we would convert the data for the input field to a wider type, e.g. int
 *       (input type) => long (table type).
 *   <li>In the third case, we would rearrange the input data to match the target table.
 * </ul>
 */
interface DataConverter {
  Object convert(Object object);

  static DataConverter identity() {
    return object -> object;
  }

  static DataConverter getNullable(LogicalType sourceType, LogicalType targetType) {
    return nullable(get(sourceType, targetType));
  }

  static DataConverter get(LogicalType sourceType, LogicalType targetType) {
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
        return object -> object;
      case DOUBLE:
        return object -> {
          if (object instanceof Float) {
            return ((Float) object).doubleValue();
          } else {
            return object;
          }
        };
      case BIGINT:
        return object -> {
          if (object instanceof Integer) {
            return ((Integer) object).longValue();
          } else {
            return object;
          }
        };
      case DECIMAL:
        return object -> {
          DecimalType toDecimalType = (DecimalType) targetType;
          DecimalData decimalData = (DecimalData) object;
          if (((DecimalType) sourceType).getPrecision() == toDecimalType.getPrecision()) {
            return object;
          } else {
            return DecimalData.fromBigDecimal(
                decimalData.toBigDecimal(), toDecimalType.getPrecision(), toDecimalType.getScale());
          }
        };
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        return object -> {
          if (object instanceof Integer) {
            LocalDateTime dateTime =
                LocalDateTime.of(LocalDate.ofEpochDay((Integer) object), LocalTime.MIN);
            return TimestampData.fromLocalDateTime(dateTime);
          } else {
            return object;
          }
        };
      case ROW:
        return new RowDataConverter((RowType) sourceType, (RowType) targetType);
      case ARRAY:
        return new ArrayConverter((ArrayType) sourceType, (ArrayType) targetType);
      case MAP:
        return new MapConverter((MapType) sourceType, (MapType) targetType);
      default:
        throw new UnsupportedOperationException("Not a supported type: " + targetType);
    }
  }

  static DataConverter nullable(DataConverter converter) {
    return value -> value == null ? null : converter.convert(value);
  }

  class RowDataConverter implements DataConverter {
    private final RowData.FieldGetter[] fieldGetters;
    private final DataConverter[] dataConverters;

    RowDataConverter(RowType sourceType, RowType targetType) {
      this.fieldGetters = new RowData.FieldGetter[targetType.getFields().size()];
      this.dataConverters = new DataConverter[targetType.getFields().size()];

      for (int i = 0; i < targetType.getFields().size(); i++) {
        RowData.FieldGetter fieldGetter;
        DataConverter dataConverter;
        RowType.RowField targetField = targetType.getFields().get(i);
        int sourceFieldIndex = sourceType.getFieldIndex(targetField.getName());
        if (sourceFieldIndex == -1) {
          if (targetField.getType().isNullable()) {
            fieldGetter = row -> null;
            dataConverter = value -> null;
          } else {
            throw new IllegalArgumentException(
                String.format(
                    "Field %s in target schema %s is non-nullable but does not exist in source schema.",
                    i + 1, targetType));
          }
        } else {
          RowType.RowField sourceField = sourceType.getFields().get(sourceFieldIndex);
          fieldGetter = RowData.createFieldGetter(sourceField.getType(), sourceFieldIndex);
          dataConverter = DataConverter.getNullable(sourceField.getType(), targetField.getType());
        }

        this.fieldGetters[i] = fieldGetter;
        this.dataConverters[i] = dataConverter;
      }
    }

    @Override
    public RowData convert(Object object) {
      RowData sourceData = (RowData) object;
      GenericRowData targetData = new GenericRowData(fieldGetters.length);
      for (int i = 0; i < fieldGetters.length; i++) {
        Object value = fieldGetters[i].getFieldOrNull(sourceData);
        targetData.setField(i, dataConverters[i].convert(value));
      }

      return targetData;
    }
  }

  class ArrayConverter implements DataConverter {
    private final ArrayData.ElementGetter elementGetter;
    private final DataConverter elementConverter;

    ArrayConverter(ArrayType sourceType, ArrayType targetType) {
      this.elementGetter = ArrayData.createElementGetter(sourceType.getElementType());
      this.elementConverter =
          DataConverter.getNullable(sourceType.getElementType(), targetType.getElementType());
    }

    @Override
    public ArrayData convert(Object object) {
      ArrayData arrayData = (ArrayData) object;
      Object[] convertedArray = new Object[arrayData.size()];
      for (int i = 0; i < convertedArray.length; i++) {
        Object element = elementGetter.getElementOrNull(arrayData, i);
        convertedArray[i] = elementConverter.convert(element);
      }

      return new GenericArrayData(convertedArray);
    }
  }

  class MapConverter implements DataConverter {
    private final ArrayData.ElementGetter keyGetter;
    private final ArrayData.ElementGetter valueGetter;
    private final DataConverter keyConverter;
    private final DataConverter valueConverter;

    MapConverter(MapType sourceType, MapType targetType) {
      this.keyGetter = ArrayData.createElementGetter(sourceType.getKeyType());
      this.valueGetter = ArrayData.createElementGetter(sourceType.getValueType());
      this.keyConverter =
          DataConverter.getNullable(sourceType.getKeyType(), targetType.getKeyType());
      this.valueConverter =
          DataConverter.getNullable(sourceType.getValueType(), targetType.getValueType());
    }

    @Override
    public MapData convert(Object object) {
      MapData sourceData = (MapData) object;
      ArrayData keyArray = sourceData.keyArray();
      ArrayData valueArray = sourceData.valueArray();
      Map<Object, Object> convertedMap = Maps.newLinkedHashMap();
      for (int i = 0; i < keyArray.size(); ++i) {
        convertedMap.put(
            keyConverter.convert(keyGetter.getElementOrNull(keyArray, i)),
            valueConverter.convert(valueGetter.getElementOrNull(valueArray, i)));
      }

      return new GenericMapData(convertedMap);
    }
  }
}
