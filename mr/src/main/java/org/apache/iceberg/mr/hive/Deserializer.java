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

package org.apache.iceberg.mr.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergWriteObjectInspector;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;


class Deserializer {
  private FieldDeserializer fieldDeserializer;

  static class Builder {
    private Schema schema;
    private ObjectInspector inspector;

    Builder schema(Schema mainSchema) {
      this.schema = mainSchema;
      return this;
    }

    Builder inspector(ObjectInspector mainInspector) {
      this.inspector = mainInspector;
      return this;
    }

    Deserializer build() {
      return new Deserializer(schema, inspector);
    }
  }

  Record deserialize(Object data) {
    return (Record) fieldDeserializer.value(data);
  }

  private Deserializer(Schema schema, ObjectInspector fieldInspector) {
    this.fieldDeserializer = DeserializerVisitor.visit(schema, fieldInspector);
  }

  private static class DeserializerVisitor extends SchemaWithPartnerVisitor<ObjectInspector, FieldDeserializer> {

    public static FieldDeserializer visit(Schema schema, ObjectInspector objectInspector) {
      return visit(schema, objectInspector, new DeserializerVisitor(), new PartnerObjectInspectorByNameAccessors());
    }

    @Override
    public FieldDeserializer schema(Schema schema, ObjectInspector inspector, FieldDeserializer deserializer) {
      return deserializer;
    }

    @Override
    public FieldDeserializer field(NestedField field, ObjectInspector inspector, FieldDeserializer deserializer) {
      return deserializer;
    }

    @Override
    public FieldDeserializer primitive(PrimitiveType type, ObjectInspector inspector) {
      switch (type.typeId()) {
        case BOOLEAN:
        case INTEGER:
        case LONG:
        case FLOAT:
        case DOUBLE:
        case STRING:
          // Generic conversions where Iceberg and Hive are using the same java object
          return o -> ((PrimitiveObjectInspector) inspector).getPrimitiveJavaObject(o);
        case UUID:
          // TODO: This will not work with Parquet. Parquet UUID expect byte[], others are expecting UUID
          return o -> UUID.fromString(((StringObjectInspector) inspector).getPrimitiveJavaObject(o));
        case DATE:
        case TIMESTAMP:
        case FIXED:
        case BINARY:
        case DECIMAL:
          // Iceberg specific conversions
          return o -> ((IcebergWriteObjectInspector) inspector).getIcebergObject(o);
        case TIME:
        default:
          throw new IllegalArgumentException("Unsupported column type: " + type);
      }
    }

    @Override
    public FieldDeserializer struct(StructType type, ObjectInspector inspector, List<FieldDeserializer> deserializers) {
      return o -> {
        if (o == null) {
          return null;
        }

        List<Object> data = ((StructObjectInspector) inspector).getStructFieldsDataAsList(o);
        Record result = GenericRecord.create(type);

        for (int i = 0; i < deserializers.size(); i++) {
          Object fieldValue = data.get(i);
          if (fieldValue != null) {
            result.set(i, deserializers.get(i).value(fieldValue));
          } else {
            result.set(i, null);
          }
        }

        return result;
      };
    }

    @Override
    public FieldDeserializer list(ListType listTypeInfo, ObjectInspector inspector, FieldDeserializer deserializer) {
      return o -> {
        if (o == null) {
          return null;
        }

        List<Object> result = new ArrayList<>();
        ListObjectInspector listInspector = (ListObjectInspector) inspector;

        for (Object val : listInspector.getList(o)) {
          result.add(deserializer.value(val));
        }

        return result;
      };
    }

    @Override
    public FieldDeserializer map(MapType mapType, ObjectInspector inspector, FieldDeserializer keyDeserializer,
                                 FieldDeserializer valueDeserializer) {
      return o -> {
        if (o == null) {
          return null;
        }

        Map<Object, Object> result = new HashMap<>();
        MapObjectInspector mapObjectInspector = (MapObjectInspector) inspector;

        for (Map.Entry<?, ?> entry : mapObjectInspector.getMap(o).entrySet()) {
          result.put(keyDeserializer.value(entry.getKey()), valueDeserializer.value(entry.getValue()));
        }
        return result;
      };
    }
  }

  private static class PartnerObjectInspectorByNameAccessors
      implements SchemaWithPartnerVisitor.PartnerAccessors<ObjectInspector> {

    @Override
    public ObjectInspector fieldPartner(ObjectInspector inspector, int fieldId, String name) {
      StructObjectInspector fieldInspector  = (StructObjectInspector) inspector;
      return fieldInspector.getStructFieldRef(name).getFieldObjectInspector();
    }

    @Override
    public ObjectInspector mapKeyPartner(ObjectInspector inspector) {
      MapObjectInspector fieldInspector  = (MapObjectInspector) inspector;
      return fieldInspector.getMapKeyObjectInspector();
    }

    @Override
    public ObjectInspector mapValuePartner(ObjectInspector inspector) {
      MapObjectInspector fieldInspector  = (MapObjectInspector) inspector;
      return fieldInspector.getMapValueObjectInspector();
    }

    @Override
    public ObjectInspector listElementPartner(ObjectInspector inspector) {
      ListObjectInspector fieldInspector  = (ListObjectInspector) inspector;
      return fieldInspector.getListElementObjectInspector();
    }
  }

  private interface FieldDeserializer {
    Object value(Object object);
  }
}
