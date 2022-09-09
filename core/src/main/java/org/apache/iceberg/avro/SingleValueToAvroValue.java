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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

public class SingleValueToAvroValue {

  private SingleValueToAvroValue() {}

  public static Object convert(Type type, Object value) {
    switch (type.typeId()) {
      case STRUCT:
        Types.StructType structType = type.asNestedType().asStructType();
        StructLike structValue = (StructLike) value;
        GenericData.Record rec = new GenericData.Record(AvroSchemaUtil.convert(type));

        for (int i = 0; i < structValue.size(); i += 1) {
          Type fieldType = structType.fields().get(i).type();
          rec.put(i, convert(fieldType, structValue.get(i, fieldType.typeId().javaClass())));
        }

        return rec;

      case LIST:
        Types.ListType listType = type.asNestedType().asListType();
        Type elementType = listType.elementType();
        List<Object> listValue = (List<Object>) value;

        List<Object> list = Lists.newArrayListWithExpectedSize(listValue.size());
        for (Object o : listValue) {
          list.add(convert(elementType, o));
        }

        return list;

      case MAP:
        Types.MapType mapType = type.asNestedType().asMapType();
        Map<Object, Object> mapValue = (Map<Object, Object>) value;

        Map<Object, Object> map = Maps.newLinkedHashMap();
        for (Map.Entry<Object, Object> entry : mapValue.entrySet()) {
          map.put(
              convert(mapType.keyType(), entry.getKey()),
              convert(mapType.valueType(), entry.getValue()));
        }

        return map;

      default:
        return convertPrimitive(type.asPrimitiveType(), value);
    }
  }

  private static Object convertPrimitive(Type.PrimitiveType primitive, Object value) {
    // For the primitives that Avro needs a different type than Spark, fix
    // them here.
    switch (primitive.typeId()) {
      case FIXED:
        return new GenericData.Fixed(
            AvroSchemaUtil.convert(primitive), ByteBuffers.toByteArray((ByteBuffer) value));
      default:
        return value;
    }
  }
}
