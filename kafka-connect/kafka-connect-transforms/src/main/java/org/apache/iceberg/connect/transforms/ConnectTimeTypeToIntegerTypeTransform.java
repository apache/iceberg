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

import java.util.Map;
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

public abstract class ConnectTimeTypeToIntegerTypeTransform<R extends ConnectRecord<R>>
    implements Transformation<R> {

  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public R apply(R record) {
    if (operatingValue(record) == null || operatingSchema(record) == null) {
      return record;
    } else {
      return applyWithSchema(record);
    }
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
    return newRecord(record, updatedSchema, value);
  }

  private void buildUpdatedSchema(
      Schema schema, SchemaBuilder newSchema, boolean optional, Struct defaultFromParent) {
    for (Field field : schema.fields()) {
      final boolean fieldIsOptional = optional || field.schema().isOptional();
      Object fieldDefaultValue = null;

      // Extract default value if present
      if (field.schema().defaultValue() != null) {
        fieldDefaultValue = field.schema().defaultValue();
      } else if (defaultFromParent != null) {
        fieldDefaultValue = defaultFromParent.get(field);
      }

      Schema fieldSchema = field.schema(); // Get original field schema
      SchemaBuilder fieldBuilder;

      switch (fieldSchema.type()) {
        case INT32:
          // Check if field has logical type "Time" and remove logical type
          if (Time.LOGICAL_NAME.equals(fieldSchema.name())) {
            fieldBuilder = SchemaBuilder.int32().defaultValue(fieldDefaultValue);
          } else {
            fieldBuilder = SchemaBuilder.type(fieldSchema.type()).defaultValue(fieldDefaultValue);
          }
          break;
        case INT8:
        case INT16:
        case INT64:
        case FLOAT32:
        case FLOAT64:
        case BOOLEAN:
        case STRING:
        case BYTES:
          fieldBuilder = SchemaBuilder.type(fieldSchema.type()).defaultValue(fieldDefaultValue);
          break;
        case ARRAY:
          // Recursively transform array elements
          SchemaBuilder arrayBuilder = SchemaUtil.copySchemaBasics(fieldSchema.valueSchema());
          buildUpdatedSchema(
              fieldSchema.valueSchema(), arrayBuilder, fieldIsOptional, (Struct) fieldDefaultValue);
          fieldBuilder = arrayBuilder;
          break;

        case MAP:
          // Recursively transform both key and value of the map
          SchemaBuilder mapBuilder =
              SchemaBuilder.map(
                  SchemaUtil.copySchemaBasics(fieldSchema.keySchema()),
                  SchemaUtil.copySchemaBasics(fieldSchema.valueSchema()));
          buildUpdatedSchema(
              fieldSchema.valueSchema(), mapBuilder, fieldIsOptional, (Struct) fieldDefaultValue);
          fieldBuilder = mapBuilder;
          break;

        case STRUCT:
          // Recursively transform nested structs
          SchemaBuilder structBuilder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
          buildUpdatedSchema(
              fieldSchema,
              structBuilder,
              fieldIsOptional,
              defaultFromParent != null ? (Struct) fieldDefaultValue : null);
          fieldBuilder = structBuilder;
          break;

        default:
          throw new DataException(
              "Schema transformation does not support type "
                  + fieldSchema.type()
                  + " for field "
                  + field.name());
      }

      // Preserve optional flag
      if (fieldIsOptional) {
        fieldBuilder.optional();
      }

      // Preserve default value
      if (fieldDefaultValue != null) {
        fieldBuilder.defaultValue(fieldDefaultValue);
      }

      // Add the transformed field to the new schema
      newSchema.field(field.name(), fieldBuilder.build());
    }
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

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
