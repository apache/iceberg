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
package org.apache.iceberg.connect.transforms;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

/**
 * A Kafka Connect transformation that converts fields of logical type {@code Time} to their integer
 * representation (milliseconds since midnight) to make it compatible with spark iceberg readers.
 * {@code Time} fields.
 *
 * @param <R> the type of {@link ConnectRecord} the transform operates on
 */
public abstract class ConnectTimeTypeToIntegerTypeTransform<R extends ConnectRecord<R>>
    implements Transformation<R> {

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> configs) {
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  @Override
  public R apply(R record) {
    if (operatingValue(record) == null || operatingSchema(record) == null) {
      return record;
    }
    return applyWithSchema(record);
  }

  private R applyWithSchema(R record) {
    final Struct value =
        Requirements.requireStructOrNull(operatingValue(record), "Handling Iceberg Time Type");

    Schema schema = operatingSchema(record);
    Schema updatedSchema = schemaUpdateCache.get(schema);
    if (updatedSchema == null) {
      final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
      Struct defaultValue = (Struct) schema.defaultValue();
      buildUpdatedSchema(schema, builder, schema.isOptional(), defaultValue);
      updatedSchema = builder.build();
      schemaUpdateCache.put(schema, updatedSchema);
    }
    Struct updatedValue = buildUpdatedValue(value, updatedSchema);
    return newRecord(record, updatedSchema, updatedValue);
  }

  private void buildUpdatedSchema(
      Schema schema, SchemaBuilder newSchema, boolean optional, Struct defaultFromParent) {
    for (Field field : schema.fields()) {
      Schema fieldSchema = field.schema();
      boolean fieldIsOptional = optional || fieldSchema.isOptional();

      Object fieldDefaultValue = fieldSchema.defaultValue();
      if (fieldDefaultValue == null && defaultFromParent != null) {
        fieldDefaultValue = defaultFromParent.get(field);
      }

      SchemaBuilder fieldBuilder;

      switch (fieldSchema.type()) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT32:
        case FLOAT64:
        case BOOLEAN:
        case STRING:
        case BYTES:
          if (Time.LOGICAL_NAME.equals(fieldSchema.name())) {
            fieldBuilder = SchemaBuilder.int32();
          } else {
            fieldBuilder = SchemaBuilder.type(fieldSchema.type()).name(fieldSchema.name());
          }
          break;

        case ARRAY:
          fieldBuilder =
              SchemaUtil.copySchemaBasics(
                  fieldSchema,
                  SchemaBuilder.array(
                      buildTransformedSchema(
                          fieldSchema.valueSchema(), fieldIsOptional, fieldDefaultValue)));
          break;

        case MAP:
          fieldBuilder =
              SchemaUtil.copySchemaBasics(
                  fieldSchema,
                  SchemaBuilder.map(
                      buildTransformedSchema(fieldSchema.keySchema(), fieldIsOptional, null),
                      buildTransformedSchema(
                          fieldSchema.valueSchema(), fieldIsOptional, fieldDefaultValue)));
          break;

        case STRUCT:
          SchemaBuilder structBuilder =
              SchemaUtil.copySchemaBasics(fieldSchema, SchemaBuilder.struct());
          Struct nestedDefault =
              (fieldDefaultValue instanceof Struct) ? (Struct) fieldDefaultValue : null;
          buildUpdatedSchema(fieldSchema, structBuilder, fieldIsOptional, nestedDefault);
          fieldBuilder = structBuilder;
          break;

        default:
          throw new DataException(
              "Unsupported schema type: " + fieldSchema.type() + " for field " + field.name());
      }

      if (fieldIsOptional) {
        fieldBuilder.optional();
      }
      if (fieldDefaultValue != null) {
        fieldBuilder.defaultValue(fieldDefaultValue);
      }
      newSchema.field(field.name(), fieldBuilder.build());
    }
  }

  private Struct buildUpdatedValue(Struct originalStruct, Schema updatedSchema) {
    Struct updatedStruct = new Struct(updatedSchema);
    for (Field field : updatedSchema.fields()) {
      String fieldName = field.name();
      Schema newFieldSchema = field.schema();
      Field originalField = originalStruct.schema().field(fieldName);
      if (originalField == null) {
        updatedStruct.put(fieldName, null);
        continue;
      }

      Object originalValue = originalStruct.get(originalField);
      if (originalValue == null) {
        updatedStruct.put(fieldName, null);
        continue;
      }

      Schema originalFieldSchema = originalField.schema();
      String logicalName = originalFieldSchema.name();

      if (Time.LOGICAL_NAME.equals(logicalName)) {
        java.util.Date date = (java.util.Date) originalValue;
        int millisSinceMidnight = (int) (date.getTime() % (24 * 60 * 60 * 1000));
        updatedStruct.put(fieldName, millisSinceMidnight);
      } else if (originalFieldSchema.type() == Schema.Type.STRUCT) {
        updatedStruct.put(fieldName, buildUpdatedValue((Struct) originalValue, newFieldSchema));
      } else if (originalFieldSchema.type() == Schema.Type.ARRAY) {
        updatedStruct.put(
            fieldName,
            buildUpdatedArray(
                (List<?>) originalValue,
                originalFieldSchema.valueSchema(),
                newFieldSchema.valueSchema()));
      } else if (originalFieldSchema.type() == Schema.Type.MAP) {
        updatedStruct.put(
            fieldName,
            buildUpdatedMap(
                (Map<?, ?>) originalValue,
                originalFieldSchema.valueSchema(),
                newFieldSchema.valueSchema()));
      } else {
        updatedStruct.put(fieldName, originalValue);
      }
    }
    return updatedStruct;
  }

  private List<Object> buildUpdatedArray(
      List<?> originalList, Schema originalElemSchema, Schema updatedElemSchema) {
    List<Object> updatedList = Lists.newArrayList();
    for (Object elem : originalList) {
      if (elem == null) {
        updatedList.add(null);
      } else if (originalElemSchema.type() == Schema.Type.STRUCT) {
        updatedList.add(buildUpdatedValue((Struct) elem, updatedElemSchema));
      } else if (Time.LOGICAL_NAME.equals(originalElemSchema.name())) {
        java.util.Date date = (java.util.Date) elem;
        int millis = (int) (date.getTime() % (24 * 60 * 60 * 1000));
        updatedList.add(millis);
      } else {
        updatedList.add(elem);
      }
    }
    return updatedList;
  }

  private Map<Object, Object> buildUpdatedMap(
      Map<?, ?> originalMap, Schema originalValSchema, Schema updatedValSchema) {
    Map<Object, Object> updatedMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : originalMap.entrySet()) {
      Object key = entry.getKey();
      Object val = entry.getValue();
      if (val == null) {
        updatedMap.put(key, null);
      } else if (originalValSchema.type() == Schema.Type.STRUCT) {
        updatedMap.put(key, buildUpdatedValue((Struct) val, updatedValSchema));
      } else if (Time.LOGICAL_NAME.equals(originalValSchema.name())) {
        java.util.Date date = (java.util.Date) val;
        int millis = (int) (date.getTime() % (24 * 60 * 60 * 1000));
        updatedMap.put(key, millis);
      } else {
        updatedMap.put(key, val);
      }
    }
    return updatedMap;
  }

  private Schema buildTransformedSchema(Schema schema, boolean optional, Object defaultVal) {
    SchemaBuilder builder;
    if (schema.type() == Schema.Type.STRUCT) {
      builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
      buildUpdatedSchema(schema, builder, optional, (Struct) defaultVal);
      return builder.build();
    } else if (schema.type() == Schema.Type.ARRAY) {
      SchemaBuilder inner =
          SchemaBuilder.array(buildTransformedSchema(schema.valueSchema(), optional, defaultVal));
      return SchemaUtil.copySchemaBasics(schema, inner).build();
    } else if (schema.type() == Schema.Type.MAP) {
      SchemaBuilder inner =
          SchemaBuilder.map(
              buildTransformedSchema(schema.keySchema(), optional, null),
              buildTransformedSchema(schema.valueSchema(), optional, defaultVal));
      return SchemaUtil.copySchemaBasics(schema, inner).build();
    } else {
      if (Time.LOGICAL_NAME.equals(schema.name())) {
        builder = SchemaBuilder.int32();
      } else {
        builder = SchemaBuilder.type(schema.type()).name(schema.name());
      }
      if (optional) {
        builder.optional();
      }
      if (defaultVal != null) {
        builder.defaultValue(defaultVal);
      }
      return builder.build();
    }
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {}

  /** Returns the schema used for transformation from the given record. */
  protected abstract Schema operatingSchema(R record);

  /** Returns the value used for transformation from the given record. */
  protected abstract Object operatingValue(R record);

  /** Returns a new record with the updated schema and value. */
  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  /** Implementation of the transformation that operates on the value schema. */
  public static class Value<R extends ConnectRecord<R>>
      extends ConnectTimeTypeToIntegerTypeTransform<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          record.keySchema(),
          record.key(),
          updatedSchema,
          updatedValue,
          record.timestamp());
    }
  }
}
