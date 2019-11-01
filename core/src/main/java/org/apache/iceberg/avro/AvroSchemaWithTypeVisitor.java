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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import java.util.Deque;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public abstract class AvroSchemaWithTypeVisitor<T> {
  public static <T> T visit(org.apache.iceberg.Schema iSchema, Schema schema, AvroSchemaWithTypeVisitor<T> visitor) {
    return visit(iSchema.asStruct(), schema, visitor);
  }

  public static <T> T visit(Type iType, Schema schema, AvroSchemaWithTypeVisitor<T> visitor) {
    switch (schema.getType()) {
      case RECORD:
        Types.StructType struct = iType != null ? iType.asStructType() : null;

        // check to make sure this hasn't been visited before
        String name = schema.getFullName();
        Preconditions.checkState(!visitor.recordLevels.contains(name),
            "Cannot process recursive Avro record %s", name);

        visitor.recordLevels.push(name);

        List<Schema.Field> fields = schema.getFields();
        List<String> names = Lists.newArrayListWithExpectedSize(fields.size());
        List<T> results = Lists.newArrayListWithExpectedSize(fields.size());
        for (Schema.Field field : schema.getFields()) {
          int fieldId = AvroSchemaUtil.getFieldId(field);
          Types.NestedField iField = struct != null ? struct.field(fieldId) : null;
          names.add(field.name());
          results.add(visit(iField != null ? iField.type() : null, field.schema(), visitor));
        }

        visitor.recordLevels.pop();

        return visitor.record(struct, schema, names, results);

      case UNION:
        List<Schema> types = schema.getTypes();
        List<T> options = Lists.newArrayListWithExpectedSize(types.size());
        for (Schema type : types) {
          if (type.getType() == Schema.Type.NULL) {
            options.add(visit((Type) null, type, visitor));
          } else {
            options.add(visit(iType, type, visitor));
          }
        }
        return visitor.union(iType, schema, options);

      case ARRAY:
        Types.ListType list = iType != null ? iType.asListType() : null;
        return visitor.array(list, schema,
            visit(list != null ? list.elementType() : null, schema.getElementType(), visitor));

      case MAP:
        Types.MapType map = iType != null ? iType.asMapType() : null;
        return visitor.map(map, schema,
            visit(map != null ? map.valueType() : null, schema.getValueType(), visitor));

      default:
        return visitor.primitive(iType != null ? iType.asPrimitiveType() : null, schema);
    }
  }

  private Deque<String> recordLevels = Lists.newLinkedList();

  public T record(Types.StructType iStruct, Schema record, List<String> names, List<T> fields) {
    return null;
  }

  public T union(Type iType, Schema union, List<T> options) {
    return null;
  }

  public T array(Types.ListType iList, Schema array, T element) {
    return null;
  }

  public T map(Types.MapType iMap, Schema map, T value) {
    return null;
  }

  public T primitive(Type.PrimitiveType iPrimitive, Schema primitive) {
    return null;
  }
}
