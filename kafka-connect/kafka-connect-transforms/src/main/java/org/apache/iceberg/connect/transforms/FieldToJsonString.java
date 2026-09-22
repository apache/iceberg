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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.Collection;
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
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class FieldToJsonString<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String FIELDS = "fields";
  public static final String IGNORE_MISSING = "ignore.missing";

  private static final ObjectWriter WRITER = new ObjectMapper().writer();

  public static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              FIELDS,
              ConfigDef.Type.LIST,
              ConfigDef.Importance.HIGH,
              "Comma-separated list of dot-notation field paths whose object or array value is "
                  + "replaced with its JSON string representation "
                  + "(e.g. clientContext,account.registrationData.marketingData.metadata).")
          .define(
              IGNORE_MISSING,
              ConfigDef.Type.BOOLEAN,
              true,
              ConfigDef.Importance.LOW,
              "When true, a configured path that is missing or null is left untouched. "
                  + "When false, a missing or non-object intermediate node throws.");

  private List<String[]> fieldPaths;
  private boolean ignoreMissing;
  private Cache<Schema, Schema> schemaUpdateCache;

  @Override
  public void configure(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    List<String> rawFields = config.getList(FIELDS);
    if (rawFields == null || rawFields.isEmpty()) {
      throw new IllegalArgumentException(FIELDS + " must contain at least one field path");
    }
    fieldPaths = Lists.newArrayListWithCapacity(rawFields.size());
    for (String raw : rawFields) {
      String trimmed = raw.trim();
      if (trimmed.isEmpty()) {
        throw new IllegalArgumentException(FIELDS + " must not contain empty field paths");
      }
      fieldPaths.add(trimmed.split("\\."));
    }
    ignoreMissing = config.getBoolean(IGNORE_MISSING);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private R applySchemaless(R record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "field to json string");
    Map<String, Object> updatedValue = Maps.newHashMap(value);
    for (String[] path : fieldPaths) {
      stringifyInMap(updatedValue, path, 0);
    }
    return newRecord(record, null, updatedValue);
  }

  @SuppressWarnings("unchecked")
  private void stringifyInMap(Map<String, Object> node, String[] path, int index) {
    String key = path[index];
    boolean isLeaf = index == path.length - 1;
    if (!node.containsKey(key)) {
      if (!ignoreMissing) {
        throw new DataException("Field path segment '" + key + "' not found in record value");
      }
      return;
    }
    Object child = node.get(key);
    if (child == null) {
      return;
    }
    if (isLeaf) {
      if (child instanceof String) {
        return;
      }
      node.put(key, toJsonString(child));
      return;
    }
    if (!(child instanceof Map)) {
      if (!ignoreMissing) {
        throw new DataException("Field path segment '" + key + "' is not an object");
      }
      return;
    }
    Map<String, Object> childMap = Maps.newHashMap((Map<String, Object>) child);
    node.put(key, childMap);
    stringifyInMap(childMap, path, index + 1);
  }

  private R applyWithSchema(R record) {
    Struct value = Requirements.requireStruct(record.value(), "field to json string");
    Schema updatedSchema = schemaUpdateCache.get(value.schema());
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema(), 0);
      schemaUpdateCache.put(value.schema(), updatedSchema);
    }
    Struct updatedValue = buildUpdatedStruct(value, updatedSchema);
    return newRecord(record, updatedSchema, updatedValue);
  }

  private Schema makeUpdatedSchema(Schema schema, int depth) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      Schema fieldSchema = field.schema();
      if (matchesLeaf(field.name(), depth)) {
        fieldSchema =
            field.schema().isOptional() ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA;
      } else if (matchesIntermediate(field.name(), depth)
          && field.schema().type() == Schema.Type.STRUCT) {
        fieldSchema = makeUpdatedSchema(field.schema(), depth + 1);
      }
      builder.field(field.name(), fieldSchema);
    }
    return builder.build();
  }

  private Struct buildUpdatedStruct(Struct source, Schema targetSchema) {
    Struct target = new Struct(targetSchema);
    for (Field field : source.schema().fields()) {
      Object sourceValue = source.get(field);
      Field targetField = targetSchema.field(field.name());
      if (sourceValue == null) {
        target.put(targetField, null);
      } else if (targetField.schema().type() == Schema.Type.STRING
          && field.schema().type() != Schema.Type.STRING) {
        target.put(targetField, toJsonString(sourceValue));
      } else if (targetField.schema().type() == Schema.Type.STRUCT
          && field.schema().type() == Schema.Type.STRUCT) {
        target.put(targetField, buildUpdatedStruct((Struct) sourceValue, targetField.schema()));
      } else {
        target.put(targetField, sourceValue);
      }
    }
    return target;
  }

  private boolean matchesLeaf(String fieldName, int depth) {
    for (String[] path : fieldPaths) {
      if (depth == path.length - 1 && path[depth].equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  private boolean matchesIntermediate(String fieldName, int depth) {
    for (String[] path : fieldPaths) {
      if (depth < path.length - 1 && path[depth].equals(fieldName)) {
        return true;
      }
    }
    return false;
  }

  private String toJsonString(Object value) {
    try {
      return WRITER.writeValueAsString(toPlainObject(value));
    } catch (Exception e) {
      throw new DataException("Failed to serialize field value to JSON string", e);
    }
  }

  private Object toPlainObject(Object value) {
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Map<String, Object> result = Maps.newLinkedHashMap();
      for (Field field : struct.schema().fields()) {
        result.put(field.name(), toPlainObject(struct.get(field)));
      }
      return result;
    } else if (value instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) value;
      Map<String, Object> result = Maps.newLinkedHashMap();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        result.put(String.valueOf(entry.getKey()), toPlainObject(entry.getValue()));
      }
      return result;
    } else if (value instanceof Collection) {
      List<Object> result = Lists.newArrayList();
      for (Object item : (Collection<?>) value) {
        result.add(toPlainObject(item));
      }
      return result;
    } else {
      return value;
    }
  }

  private R newRecord(R record, Schema updatedSchema, Object updatedValue) {
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        updatedSchema,
        updatedValue,
        record.timestamp());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }
}
