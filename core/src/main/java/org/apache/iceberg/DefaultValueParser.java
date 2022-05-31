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
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.io.BaseEncoding;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;

public class DefaultValueParser {
  private DefaultValueParser() {
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static Object parseDefaultFromJson(Type type, JsonNode defaultValue) {

    if (defaultValue == null || defaultValue.isNull()) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
        Preconditions.checkArgument(defaultValue.isBoolean(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.booleanValue();
      case INTEGER:
        Preconditions.checkArgument(defaultValue.isIntegralNumber() && defaultValue.canConvertToInt(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.intValue();
      case LONG:
        Preconditions.checkArgument(defaultValue.isIntegralNumber() && defaultValue.canConvertToLong(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.longValue();
      case FLOAT:
        Preconditions.checkArgument(defaultValue.isNumber(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.floatValue();
      case DOUBLE:
        Preconditions.checkArgument(defaultValue.isNumber(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.doubleValue();
      case DECIMAL:
        Preconditions.checkArgument(defaultValue.isNumber(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.decimalValue();
      case STRING:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return defaultValue.textValue();
      case UUID:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return UUID.fromString(defaultValue.textValue());
      case DATE:
      case TIME:
      case TIMESTAMP:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse %s to a %s value", defaultValue, type);
        return Literal.of(defaultValue.textValue()).to(type).value();
      case FIXED:
        Preconditions.checkArgument(
            defaultValue.isTextual() && defaultValue.textValue().length() == ((Types.FixedType) type).length() * 2,
            "Cannot parse %s to a %s value",
            defaultValue,
            type);
        byte[] fixedBytes = BaseEncoding.base16().decode(defaultValue.textValue().toUpperCase(Locale.ROOT));
        return ByteBuffer.allocate(((Types.FixedType) type).length()).put(fixedBytes);
      case BINARY:
        Preconditions.checkArgument(defaultValue.isTextual(),
            "Cannot parse %s to a %s value", defaultValue, type);
        byte[] binaryBytes = BaseEncoding.base16().decode(defaultValue.textValue().toUpperCase(Locale.ROOT));
        return ByteBuffer.wrap(binaryBytes);
      case LIST:
        Preconditions.checkArgument(defaultValue.isArray(),
            "Cannot parse %s to a %s value", defaultValue, type);
        List<Object> defaultList = Lists.newArrayList();
        for (JsonNode element : defaultValue) {
          defaultList.add(parseDefaultFromJson(type.asListType().elementType(), element));
        }
        return defaultList;
      case MAP:
        Preconditions.checkArgument(
            defaultValue.isObject() && defaultValue.has("keys") && defaultValue.has("values") &&
                defaultValue.get("keys").isArray() && defaultValue.get("values").isArray(),
            "Cannot parse %s to a %s value",
            defaultValue,
            type);
        Map<Object, Object> defaultMap = Maps.newHashMap();
        JsonNode keys = defaultValue.get("keys");
        JsonNode values = defaultValue.get("values");
        List<JsonNode> keyList = Lists.newArrayList(keys.iterator());
        List<JsonNode> valueList = Lists.newArrayList(values.iterator());

        for (int i = 0; i < keyList.size(); i++) {
          defaultMap.put(
              parseDefaultFromJson(type.asMapType().keyType(), keyList.get(i)),
              parseDefaultFromJson(type.asMapType().valueType(), valueList.get(i)));
        }
        return defaultMap;
      case STRUCT:
        Preconditions.checkArgument(defaultValue.isObject(),
            "Cannot parse %s to a %s value", defaultValue, type);
        Map<Integer, Object> defaultStruct = Maps.newHashMap();
        for (Types.NestedField subField : type.asStructType().fields()) {
          String fieldIdAsString = String.valueOf(subField.fieldId());
          Object value = defaultValue.has(fieldIdAsString) ? parseDefaultFromJson(
              subField.type(),
              defaultValue.get(fieldIdAsString)) : null;
          if (value != null) {
            defaultStruct.put(subField.fieldId(), value);
          }
        }
        return defaultStruct;
      default:
        return null;
    }
  }

  public static Object convertJavaDefaultForSerialization(Type type, Object value) {
    switch (type.typeId()) {
      case DATE:
        return DateTimeUtil.formatEpochDays((int) value);
      case TIME:
        return DateTimeUtil.formatTimeOfDayMicros((long) value);
      case TIMESTAMP:
        return DateTimeUtil.formatEpochTimeMicros((long) value, ((Types.TimestampType) type).shouldAdjustToUTC());
      case FIXED:
      case BINARY:
        return BaseEncoding.base16().encode(((ByteBuffer) value).array());
      case LIST:
        List<Object> defaultList = (List<Object>) value;
        List<Object> convertedList = Lists.newArrayListWithExpectedSize(defaultList.size());
        for (Object element : defaultList) {
          convertedList.add(convertJavaDefaultForSerialization(type.asListType().elementType(), element));
        }
        return convertedList;
      case MAP:
        Map<Object, Object> defaultMap = (Map<Object, Object>) value;
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
        Map<Integer, Object> defaultStruct = (Map<Integer, Object>) value;
        Map<Integer, Object> convertedStruct = Maps.newHashMap();
        for (Map.Entry<Integer, Object> entry : defaultStruct.entrySet()) {
          int fieldId = entry.getKey();
          convertedStruct.put(fieldId, convertJavaDefaultForSerialization(
              type.asStructType().field(fieldId).type(),
              entry.getValue()));
        }
        return convertedStruct;
      default:
        return value;
    }
  }
}
