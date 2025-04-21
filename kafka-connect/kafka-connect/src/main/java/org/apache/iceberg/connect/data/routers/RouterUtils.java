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
package org.apache.iceberg.connect.data.routers;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

class RouterUtils {
  private RouterUtils() {}

  static String extractRouteValue(Object recordValue, String routeField) {
    if (recordValue == null) {
      return null;
    }
    Object routeValue = extractFromRecordValue(recordValue, routeField);
    return routeValue == null ? null : routeValue.toString();
  }

  @SuppressWarnings("unchecked")
  static Object extractFromRecordValue(Object recordValue, String fieldName) {
    List<String> fields = Splitter.on('.').splitToList(fieldName);
    if (recordValue instanceof Struct) {
      return valueFromStruct((Struct) recordValue, fields);
    } else if (recordValue instanceof Map) {
      return valueFromMap((Map<String, ?>) recordValue, fields);
    } else {
      throw new UnsupportedOperationException(
          "Cannot extract value from type: " + recordValue.getClass().getName());
    }
  }

  private static Object valueFromStruct(Struct parent, List<String> fields) {
    Struct struct = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = fieldValueFromStruct(struct, fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Struct, "Expected a struct type");
      struct = (Struct) value;
    }
    return fieldValueFromStruct(struct, fields.get(fields.size() - 1));
  }

  private static Object fieldValueFromStruct(Struct struct, String fieldName) {
    Field structField = struct.schema().field(fieldName);
    if (structField == null) {
      return null;
    }
    return struct.get(structField);
  }

  @SuppressWarnings("unchecked")
  private static Object valueFromMap(Map<String, ?> parent, List<String> fields) {
    Map<String, ?> map = parent;
    for (int idx = 0; idx < fields.size() - 1; idx++) {
      Object value = map.get(fields.get(idx));
      if (value == null) {
        return null;
      }
      Preconditions.checkState(value instanceof Map, "Expected a map type");
      map = (Map<String, ?>) value;
    }
    return map.get(fields.get(fields.size() - 1));
  }
}
