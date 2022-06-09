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

package org.apache.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.JsonUtil;

public class DefaultValueParser {
  private DefaultValueParser() {
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static Object fromJson(Type type, JsonNode defaultValue) {

    if (defaultValue == null || defaultValue.isNull()) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        Preconditions.checkArgument(defaultValue.isBoolean(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.booleanValue();
      case INTEGER:
        Preconditions.checkArgument(defaultValue.isIntegralNumber() && defaultValue.canConvertToInt(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.intValue();
      case LONG:
        Preconditions.checkArgument(defaultValue.isIntegralNumber() && defaultValue.canConvertToLong(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.longValue();
      case FLOAT:
        Preconditions.checkArgument(defaultValue.isFloatingPointNumber(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.floatValue();
      case DOUBLE:
        Preconditions.checkArgument(defaultValue.isNumber(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.doubleValue();
      case DECIMAL:
        Preconditions.checkArgument(defaultValue.isNumber(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        BigDecimal retDecimal = defaultValue.decimalValue();
        Preconditions.checkArgument(
            retDecimal.scale() == ((Types.DecimalType) type).scale(), "Cannot parse default as a %s value: %s",
            type,
            defaultValue);
        return retDecimal;
      case STRING:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return defaultValue.textValue();
      case UUID:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        try {
          UUID.fromString(defaultValue.textValue());
        } catch (IllegalArgumentException e) {
          Preconditions.checkArgument(false, "Cannot parse default as a %s value: %s", type, defaultValue);
        }
        return UUID.fromString(defaultValue.textValue());
      case DATE:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return DateTimeUtil.daysFromISODateString(defaultValue.textValue());
      case TIME:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        return DateTimeUtil.microsFromISOTimeString(defaultValue.textValue());
      case TIMESTAMP:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        if (((Types.TimestampType) type).shouldAdjustToUTC()) {
          return DateTimeUtil.microsFromISOOffsetTsString(defaultValue.textValue());
        } else {
          return DateTimeUtil.microsFromISOTsString(defaultValue.textValue());
        }
      case FIXED:
        Preconditions.checkArgument(
            defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        int defaultLength = defaultValue.textValue().length();
        int fixedLength = ((Types.FixedType) type).length();
        Preconditions.checkArgument(defaultLength == fixedLength * 2,
            "Default value %s is not compatible with the expected fixed type, the fixed type is expected to store " +
                "exactly %s bytes, which means the default value should be of exactly 2 * %s length hex string",
            defaultValue, fixedLength, fixedLength);
        byte[] fixedBytes = BaseEncoding.base16().decode(defaultValue.textValue().toUpperCase(Locale.ROOT));
        return ByteBuffer.wrap(fixedBytes);
      case BINARY:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        byte[] binaryBytes = BaseEncoding.base16().decode(defaultValue.textValue().toUpperCase(Locale.ROOT));
        return ByteBuffer.wrap(binaryBytes);
      case LIST:
        Preconditions.checkArgument(defaultValue.isArray(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        Type elementType = type.asListType().elementType();
        return Lists.newArrayList(Iterables.transform(defaultValue, e -> fromJson(elementType, e)));
      case MAP:
        Preconditions.checkArgument(
            defaultValue.isObject() && defaultValue.has("keys") && defaultValue.has("values") &&
                defaultValue.get("keys").isArray() && defaultValue.get("values").isArray(),
            "Cannot parse %s to a %s value",
            defaultValue, type);
        JsonNode keys = defaultValue.get("keys");
        JsonNode values = defaultValue.get("values");
        Preconditions.checkArgument(keys.size() == values.size(), "Cannot parse default as a %s value: %s", type,
            defaultValue);

        ImmutableMap.Builder<Object, Object> mapBuilder = ImmutableMap.builder();

        Iterator<JsonNode> keyIter = keys.iterator();
        Type keyType = type.asMapType().keyType();
        Iterator<JsonNode> valueIter = values.iterator();
        Type valueType = type.asMapType().valueType();

        while (keyIter.hasNext()) {
          mapBuilder.put(fromJson(keyType, keyIter.next()), fromJson(valueType, valueIter.next()));
        }

        return mapBuilder.build();
      case STRUCT:
        Preconditions.checkArgument(defaultValue.isObject(),
            "Cannot parse default as a %s value: %s", type, defaultValue);
        Types.StructType struct = type.asStructType();
        StructLike defaultRecord = GenericRecord.create(struct);

        List<Types.NestedField> fields = struct.fields();
        for (int pos = 0; pos < fields.size(); pos += 1) {
          Types.NestedField field = fields.get(pos);
          String idString = String.valueOf(field.fieldId());
          if (defaultValue.has(idString)) {
            defaultRecord.set(pos, fromJson(field.type(), defaultValue.get(idString)));
          }
        }
        return defaultRecord;
      default:
        throw new IllegalArgumentException(String.format("Type: %s is not supported", type));
    }
  }

  public static Object fromJson(Type type, String defaultValue) {
    try {
      JsonNode defaultValueJN = JsonUtil.mapper().readTree(defaultValue);
      return fromJson(type, defaultValueJN);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to parse: " + defaultValue + "; reason: " + e.getMessage(), e);
    }
  }

  public static String toJson(Type type, Object javaDefaultValue) throws IOException {
    return JsonUtil.mapper().writeValueAsString(DefaultValueParser.convertJavaDefaultForSerialization(
        type,
        javaDefaultValue));
  }

  private static Object convertJavaDefaultForSerialization(Type type, Object javaDefaultValue) {
    switch (type.typeId()) {
      case DATE:
        return DateTimeUtil.formatEpochDays((int) javaDefaultValue);
      case TIME:
        return DateTimeUtil.formatTimeOfDayMicros((long) javaDefaultValue);
      case TIMESTAMP:
        return DateTimeUtil.formatEpochTimeMicros(
            (long) javaDefaultValue,
            ((Types.TimestampType) type).shouldAdjustToUTC());
      case FIXED:
      case BINARY:
        return BaseEncoding.base16().encode(ByteBuffers.toByteArray(((ByteBuffer) javaDefaultValue)));
      case LIST:
        List<Object> defaultList = (List<Object>) javaDefaultValue;
        List<Object> convertedList = Lists.newArrayListWithExpectedSize(defaultList.size());
        for (Object element : defaultList) {
          convertedList.add(convertJavaDefaultForSerialization(type.asListType().elementType(), element));
        }
        return convertedList;
      case MAP:
        Map<Object, Object> defaultMap = (Map<Object, Object>) javaDefaultValue;
        Map<String, List<Object>> convertedDefault = Maps.newHashMapWithExpectedSize(2);
        List<Object> keyList = Lists.newArrayListWithExpectedSize(defaultMap.size());
        List<Object> valueList = Lists.newArrayListWithExpectedSize(defaultMap.size());
        for (Map.Entry<Object, Object> entry : defaultMap.entrySet()) {
          keyList.add(convertJavaDefaultForSerialization(type.asMapType().keyType(), entry.getKey()));
          valueList.add(convertJavaDefaultForSerialization(type.asMapType().valueType(), entry.getValue()));
        }
        convertedDefault.put("keys", keyList);
        convertedDefault.put("values", valueList);
        return convertedDefault;
      case STRUCT:
        Types.StructType structType = type.asStructType();
        List<Types.NestedField> fields = structType.fields();
        StructLike defaultStruct = (StructLike) javaDefaultValue;
        Map<Integer, Object> convertedStruct = Maps.newHashMap();

        for (int i = 0; i < defaultStruct.size(); i++) {
          Types.NestedField field = fields.get(i);
          int fieldId = field.fieldId();
          Object fieldJavaDefaultValue = defaultStruct.get(i, Object.class);
          if (fieldJavaDefaultValue != null) {
            convertedStruct.put(fieldId, convertJavaDefaultForSerialization(field.type(), fieldJavaDefaultValue));
          }
        }
        return convertedStruct;
      default:
        return javaDefaultValue;
    }
  }
}
