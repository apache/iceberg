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
import java.util.Objects;
import org.apache.avro.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

public class RemoveIds extends AvroSchemaVisitor<Schema> {
  @Override
  public Schema record(Schema record, List<String> names, List<Schema> types) {
    List<Schema.Field> fields = record.getFields();
    int length = fields.size();
    List<Schema.Field> newFields = Lists.newArrayListWithExpectedSize(length);
    for (int i = 0; i < length; i += 1) {
      newFields.add(copyField(fields.get(i), types.get(i)));
    }
    return AvroSchemaUtil.copyRecord(record, newFields, null);
  }

  @Override
  public Schema map(Schema map, Schema valueType) {
    Schema result = Schema.createMap(valueType);
    for (Map.Entry<String, Object> prop : map.getObjectProps().entrySet()) {
      String key = prop.getKey();
      if (!key.equals(AvroSchemaUtil.KEY_ID_PROP) && !key.equals(AvroSchemaUtil.VALUE_ID_PROP)) {
        result.addProp(key, prop.getValue());
      }
    }
    return result;
  }

  @Override
  public Schema array(Schema array, Schema element) {
    Schema result = Schema.createArray(element);
    for (Map.Entry<String, Object> prop : array.getObjectProps().entrySet()) {
      String key = prop.getKey();
      if (!key.equals(AvroSchemaUtil.ELEMENT_ID_PROP)) {
        result.addProp(key, prop.getValue());
      }
    }
    return result;
  }

  @Override
  public Schema primitive(Schema primitive) {
    return Schema.create(primitive.getType());
  }

  @Override
  public Schema union(Schema union, List<Schema> options) {
    return Schema.createUnion(options);
  }

  private static Schema.Field copyField(Schema.Field field, Schema newSchema) {
    Schema.Field copy =
        new Schema.Field(field.name(), newSchema, field.doc(), field.defaultVal(), field.order());
    for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
      String key = prop.getKey();
      if (!Objects.equals(key, AvroSchemaUtil.FIELD_ID_PROP)) {
        copy.addProp(key, prop.getValue());
      }
    }
    return copy;
  }

  static Schema removeIds(org.apache.iceberg.Schema schema) {
    return AvroSchemaVisitor.visit(
        AvroSchemaUtil.convert(schema.asStruct(), "table"), new RemoveIds());
  }

  public static Schema removeIds(Schema schema) {
    return AvroSchemaVisitor.visit(schema, new RemoveIds());
  }
}
