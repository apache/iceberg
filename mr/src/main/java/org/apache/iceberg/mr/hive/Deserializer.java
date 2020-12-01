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

import java.util.List;
import java.util.UUID;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergWriteObjectInspector;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

class Deserializer {
  private FiledDeserializer mainDeserializer;

  Deserializer(Schema schema, ObjectInspector fieldInspector) throws SerDeException {
    this.mainDeserializer = deserializer(schema.asStruct(), fieldInspector);
  }

  Record deserialize(Object data) {
    return (Record) mainDeserializer.value(data);
  }

  private interface FiledDeserializer {
    Object value(Object object);
  }

  private static FiledDeserializer deserializer(Type type, ObjectInspector fieldInspector) throws SerDeException {
    switch (type.typeId()) {
      case BOOLEAN:
        return o -> ((BooleanObjectInspector) fieldInspector).get(o);
      case INTEGER:
        return o -> ((IntObjectInspector) fieldInspector).get(o);
      case LONG:
        return o -> ((LongObjectInspector) fieldInspector).get(o);
      case FLOAT:
        return o -> ((FloatObjectInspector) fieldInspector).get(o);
      case DOUBLE:
        return o -> ((DoubleObjectInspector) fieldInspector).get(o);
      case STRING:
        return o -> ((StringObjectInspector) fieldInspector).getPrimitiveJavaObject(o);
      case UUID:
        // TODO: This will not work with Parquet. Parquet UUID expect byte[], others are expecting UUID
        return o -> UUID.fromString(((StringObjectInspector) fieldInspector).getPrimitiveJavaObject(o));
      case DATE:
      case TIMESTAMP:
      case FIXED:
      case BINARY:
      case DECIMAL:
        // Iceberg specific conversions
        return o -> ((IcebergWriteObjectInspector) fieldInspector).getIcebergObject(o);
      case STRUCT:
        return new StructDeserializer((Types.StructType) type, (StructObjectInspector) fieldInspector);
      case LIST:
      case MAP:
      case TIME:
      default:
        throw new SerDeException("Unsupported column type: " + type);
    }
  }

  private static class StructDeserializer implements FiledDeserializer {
    private final FiledDeserializer[] filedDeserializers;
    private final StructObjectInspector fieldInspector;
    private final Types.StructType type;

    private StructDeserializer(Types.StructType type, StructObjectInspector fieldInspector) throws SerDeException {
      List<? extends StructField> structFields = fieldInspector.getAllStructFieldRefs();
      List<Types.NestedField> nestedFields = type.fields();
      this.filedDeserializers = new FiledDeserializer[structFields.size()];
      this.fieldInspector = fieldInspector;
      this.type = type;

      for (int i = 0; i < filedDeserializers.length; i++) {
        filedDeserializers[i] =
            deserializer(nestedFields.get(i).type(), structFields.get(i).getFieldObjectInspector());
      }
    }

    @Override
    public Record value(Object object) {
      if (object == null) {
        return null;
      }

      List<Object> data = fieldInspector.getStructFieldsDataAsList(object);
      Record result = GenericRecord.create(type);

      for (int i = 0; i < filedDeserializers.length; i++) {
        Object fieldValue = data.get(i);
        if (fieldValue != null) {
          result.set(i, filedDeserializers[i].value(fieldValue));
        }
      }

      return result;
    }
  }
}
