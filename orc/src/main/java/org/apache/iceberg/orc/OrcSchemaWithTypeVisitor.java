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
package org.apache.iceberg.orc;

import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.orc.TypeDescription;

public abstract class OrcSchemaWithTypeVisitor<T> {
  public static <T> T visit(
      org.apache.iceberg.Schema iSchema,
      TypeDescription schema,
      OrcSchemaWithTypeVisitor<T> visitor) {
    return visit(iSchema.asStruct(), schema, visitor);
  }

  public static <T> T visit(
      Type iType, TypeDescription schema, OrcSchemaWithTypeVisitor<T> visitor) {
    switch (schema.getCategory()) {
      case STRUCT:
        return visitRecord(iType != null ? iType.asStructType() : null, schema, visitor);

      case UNION:
        throw new UnsupportedOperationException("Cannot handle " + schema);

      case LIST:
        Types.ListType list = iType != null ? iType.asListType() : null;
        return visitor.list(
            list,
            schema,
            visit(list != null ? list.elementType() : null, schema.getChildren().get(0), visitor));

      case MAP:
        Types.MapType map = iType != null ? iType.asMapType() : null;
        return visitor.map(
            map,
            schema,
            visit(map != null ? map.keyType() : null, schema.getChildren().get(0), visitor),
            visit(map != null ? map.valueType() : null, schema.getChildren().get(1), visitor));

      default:
        return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, schema);
    }
  }

  private static <T> T visitRecord(
      Types.StructType struct, TypeDescription record, OrcSchemaWithTypeVisitor<T> visitor) {
    List<TypeDescription> fields = record.getChildren();
    List<String> names = record.getFieldNames();
    List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
    for (TypeDescription field : fields) {
      int fieldId = ORCSchemaUtil.fieldId(field);
      Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
      results.add(visit(iField != null ? iField.type() : null, field, visitor));
    }
    return visitor.record(struct, record, names, results);
  }

  public T record(
      Types.StructType iStruct, TypeDescription record, List<String> names, List<T> fields) {
    return null;
  }

  public T list(Types.ListType iList, TypeDescription array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, TypeDescription map, T key, T value) {
    return null;
  }

  public T primitive(Type.PrimitiveType iPrimitive, TypeDescription primitive) {
    return null;
  }
}
