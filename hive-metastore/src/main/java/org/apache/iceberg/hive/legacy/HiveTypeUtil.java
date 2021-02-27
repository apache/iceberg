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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;


public class HiveTypeUtil {
  private HiveTypeUtil() {
  }

  public static Type convert(TypeInfo typeInfo) {
    return HiveTypeUtil.visit(typeInfo, new HiveTypeToIcebergType());
  }

  public static <T> T visit(TypeInfo typeInfo, HiveSchemaVisitor<T> visitor) {
    switch (typeInfo.getCategory()) {
      case STRUCT:
        final StructTypeInfo structTypeInfo = (StructTypeInfo) typeInfo;
        List<String> names = structTypeInfo.getAllStructFieldNames();
        List<T> results = Lists.newArrayListWithExpectedSize(names.size());
        for (String name : names) {
          results.add(visit(structTypeInfo.getStructFieldTypeInfo(name), visitor));
        }
        return visitor.struct(structTypeInfo, names, results);

      case UNION:
        throw new UnsupportedOperationException("Union data type not supported : " + typeInfo);

      case LIST:
        ListTypeInfo listTypeInfo = (ListTypeInfo) typeInfo;
        return visitor.list(listTypeInfo, visit(listTypeInfo.getListElementTypeInfo(), visitor));

      case MAP:
        final MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;
        return visitor.map(mapTypeInfo,
                           visit(mapTypeInfo.getMapKeyTypeInfo(), visitor),
                           visit(mapTypeInfo.getMapValueTypeInfo(), visitor));

      default:
        final PrimitiveTypeInfo primitiveTypeInfo = (PrimitiveTypeInfo) typeInfo;
        return visitor.primitive(primitiveTypeInfo);
    }
  }

  public static class HiveSchemaVisitor<T> {
    public T struct(StructTypeInfo struct, List<String> names, List<T> fieldResults) {
      return null;
    }

    public T list(ListTypeInfo list, T elementResult) {
      return null;
    }

    public T map(MapTypeInfo map, T keyResult, T valueResult) {
      return null;
    }

    public T primitive(PrimitiveTypeInfo primitive) {
      return null;
    }
  }
}
