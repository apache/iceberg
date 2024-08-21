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
package org.apache.iceberg.avro;

import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

/** Convert Iceberg data to Avro GenericRecord. */
public class IcebergDataToGenericRecord {
  private IcebergDataToGenericRecord() {}

  /**
   * Convert Iceberg data to Avro GenericRecord.
   *
   * @param type Iceberg type
   * @param data Iceberg data
   * @return Avro GenericRecord
   */
  public static Object toGenericRecord(Type type, Object data) {
    switch (type.typeId()) {
      case STRUCT:
        Types.StructType structType = (Types.StructType) type;
        GenericData.Record genericRecord = new GenericData.Record(AvroSchemaUtil.convert(type));
        int index = 0;
        for (Types.NestedField field : structType.fields()) {
          genericRecord.put(
              field.name(), toGenericRecord(field.type(), ((GenericRecord) data).get(index)));
          index++;
        }
        return genericRecord;
      case LIST:
        Types.ListType listType = (Types.ListType) type;
        List<Object> genericList = Lists.newArrayListWithExpectedSize(((List<Object>) data).size());
        for (Object element : (List<Object>) data) {
          genericList.add(toGenericRecord(listType.elementType(), element));
        }
        return genericList;
      case MAP:
        Types.MapType mapType = (Types.MapType) type;
        Map<Object, Object> genericMap =
            Maps.newHashMapWithExpectedSize(((Map<Object, Object>) data).size());
        for (Map.Entry<Object, Object> entry : ((Map<Object, Object>) data).entrySet()) {
          genericMap.put(
              toGenericRecord(mapType.keyType(), entry.getKey()),
              toGenericRecord(mapType.valueType(), entry.getValue()));
        }
        return genericMap;
      default:
        return data;
    }
  }
}
