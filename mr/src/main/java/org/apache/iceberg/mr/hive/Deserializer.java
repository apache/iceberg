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
import java.util.Map;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.serde.objectinspector.WriteObjectInspector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.schema.SchemaWithPartnerVisitor;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;

class Deserializer {
  private FieldDeserializer fieldDeserializer;

  /**
   * Builder to create a Deserializer instance. Requires an Iceberg Schema and the Hive
   * ObjectInspector for converting the data.
   */
  static class Builder {
    private Schema schema;
    private StructObjectInspector writerInspector;
    private StructObjectInspector sourceInspector;

    Builder schema(Schema mainSchema) {
      this.schema = mainSchema;
      return this;
    }

    Builder writerInspector(StructObjectInspector inspector) {
      this.writerInspector = inspector;
      return this;
    }

    Builder sourceInspector(StructObjectInspector inspector) {
      this.sourceInspector = inspector;
      return this;
    }

    Deserializer build() {
      return new Deserializer(schema, new ObjectInspectorPair(writerInspector, sourceInspector));
    }
  }

  /**
   * Deserializes the Hive result object to an Iceberg record using the provided ObjectInspectors.
   *
   * @param data The Hive data to deserialize
   * @return The resulting Iceberg Record
   */
  Record deserialize(Object data) {
    return (Record) fieldDeserializer.value(data);
  }

  private Deserializer(Schema schema, ObjectInspectorPair pair) {
    this.fieldDeserializer = DeserializerVisitor.visit(schema, pair);
  }

  private static class DeserializerVisitor
      extends SchemaWithPartnerVisitor<ObjectInspectorPair, FieldDeserializer> {

    public static FieldDeserializer visit(Schema schema, ObjectInspectorPair pair) {
      return visit(
          schema,
          new FixNameMappingObjectInspectorPair(schema, pair),
          new DeserializerVisitor(),
          new PartnerObjectInspectorByNameAccessors());
    }

    @Override
    public FieldDeserializer schema(
        Schema schema, ObjectInspectorPair pair, FieldDeserializer deserializer) {
      return deserializer;
    }

    @Override
    public FieldDeserializer field(
        NestedField field, ObjectInspectorPair pair, FieldDeserializer deserializer) {
      return deserializer;
    }

    @Override
    public FieldDeserializer primitive(PrimitiveType type, ObjectInspectorPair pair) {
      return o -> {
        if (o == null) {
          return null;
        }

        ObjectInspector writerFieldInspector = pair.writerInspector();
        ObjectInspector sourceFieldInspector = pair.sourceInspector();

        Object result = ((PrimitiveObjectInspector) sourceFieldInspector).getPrimitiveJavaObject(o);
        if (writerFieldInspector instanceof WriteObjectInspector) {
          // If we have a conversion method defined for the ObjectInspector then convert
          result = ((WriteObjectInspector) writerFieldInspector).convert(result);
        }

        return result;
      };
    }

    @Override
    public FieldDeserializer struct(
        StructType type, ObjectInspectorPair pair, List<FieldDeserializer> deserializers) {
      Preconditions.checkNotNull(type, "Can not create reader for null type");
      GenericRecord template = GenericRecord.create(type);
      return o -> {
        if (o == null) {
          return null;
        }

        List<Object> data =
            ((StructObjectInspector) pair.sourceInspector()).getStructFieldsDataAsList(o);
        // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since
        // NAME_MAP_CACHE access
        // is eliminated. Using copy here to gain performance.
        Record result = template.copy();

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
    public FieldDeserializer list(
        ListType listTypeInfo, ObjectInspectorPair pair, FieldDeserializer deserializer) {
      return o -> {
        if (o == null) {
          return null;
        }

        List<Object> result = Lists.newArrayList();
        ListObjectInspector listInspector = (ListObjectInspector) pair.sourceInspector();

        for (Object val : listInspector.getList(o)) {
          result.add(deserializer.value(val));
        }

        return result;
      };
    }

    @Override
    public FieldDeserializer map(
        MapType mapType,
        ObjectInspectorPair pair,
        FieldDeserializer keyDeserializer,
        FieldDeserializer valueDeserializer) {
      return o -> {
        if (o == null) {
          return null;
        }

        Map<Object, Object> result = Maps.newHashMap();
        MapObjectInspector mapObjectInspector = (MapObjectInspector) pair.sourceInspector();

        for (Map.Entry<?, ?> entry : mapObjectInspector.getMap(o).entrySet()) {
          result.put(
              keyDeserializer.value(entry.getKey()), valueDeserializer.value(entry.getValue()));
        }
        return result;
      };
    }
  }

  private static class PartnerObjectInspectorByNameAccessors
      implements SchemaWithPartnerVisitor.PartnerAccessors<ObjectInspectorPair> {

    @Override
    public ObjectInspectorPair fieldPartner(ObjectInspectorPair pair, int fieldId, String name) {
      String sourceName = pair.sourceName(name);
      return new ObjectInspectorPair(
          ((StructObjectInspector) pair.writerInspector())
              .getStructFieldRef(name)
              .getFieldObjectInspector(),
          ((StructObjectInspector) pair.sourceInspector())
              .getStructFieldRef(sourceName)
              .getFieldObjectInspector());
    }

    @Override
    public ObjectInspectorPair mapKeyPartner(ObjectInspectorPair pair) {
      return new ObjectInspectorPair(
          ((MapObjectInspector) pair.writerInspector()).getMapKeyObjectInspector(),
          ((MapObjectInspector) pair.sourceInspector()).getMapKeyObjectInspector());
    }

    @Override
    public ObjectInspectorPair mapValuePartner(ObjectInspectorPair pair) {
      return new ObjectInspectorPair(
          ((MapObjectInspector) pair.writerInspector()).getMapValueObjectInspector(),
          ((MapObjectInspector) pair.sourceInspector()).getMapValueObjectInspector());
    }

    @Override
    public ObjectInspectorPair listElementPartner(ObjectInspectorPair pair) {
      return new ObjectInspectorPair(
          ((ListObjectInspector) pair.writerInspector()).getListElementObjectInspector(),
          ((ListObjectInspector) pair.sourceInspector()).getListElementObjectInspector());
    }
  }

  private interface FieldDeserializer {
    Object value(Object object);
  }

  /**
   * Hive query results schema column names do not match the target Iceberg column names. Instead we
   * have to rely on the column order. To keep the other parts of the code generic we fix this with
   * a wrapper around the ObjectInspectorPair. This wrapper maps the Iceberg schema column names
   * instead of the Hive column names.
   */
  private static class FixNameMappingObjectInspectorPair extends ObjectInspectorPair {
    private final Map<String, String> sourceNameMap;

    FixNameMappingObjectInspectorPair(Schema schema, ObjectInspectorPair pair) {
      super(pair.writerInspector(), pair.sourceInspector());

      this.sourceNameMap = Maps.newHashMapWithExpectedSize(schema.columns().size());

      List<? extends StructField> fields =
          ((StructObjectInspector) sourceInspector()).getAllStructFieldRefs();
      for (int i = 0; i < schema.columns().size(); ++i) {
        sourceNameMap.put(schema.columns().get(i).name(), fields.get(i).getFieldName());
      }
    }

    @Override
    String sourceName(String originalName) {
      return sourceNameMap.get(originalName);
    }
  }

  /**
   * To get the data for Iceberg {@link Record}s we have to use both ObjectInspectors.
   *
   * <p>We use the Hive ObjectInspectors (sourceInspector) to get the Hive primitive types.
   *
   * <p>We use the Iceberg ObjectInspectors (writerInspector) only if conversion is needed for
   * generating the correct type for Iceberg Records. See: {@link WriteObjectInspector} interface on
   * the provided writerInspector.
   */
  private static class ObjectInspectorPair {
    private ObjectInspector writerInspector;
    private ObjectInspector sourceInspector;

    ObjectInspectorPair(ObjectInspector writerInspector, ObjectInspector sourceInspector) {
      this.writerInspector = writerInspector;
      this.sourceInspector = sourceInspector;
    }

    ObjectInspector writerInspector() {
      return writerInspector;
    }

    ObjectInspector sourceInspector() {
      return sourceInspector;
    }

    String sourceName(String originalName) {
      return originalName;
    }
  }
}
