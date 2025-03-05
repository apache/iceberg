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
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.base.Splitter;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

public class KafkaMetadataTransform implements Transformation<SinkRecord> {

  private interface RecordAppender {

    void addToSchema(SchemaBuilder builder);

    void addToStruct(SinkRecord record, Struct struct);

    void addToMap(SinkRecord record, Map<String, Object> map);
  }

  private static class NoOpRecordAppender implements RecordAppender {

    @Override
    public void addToSchema(SchemaBuilder builder) {}

    @Override
    public void addToStruct(SinkRecord record, Struct struct) {}

    @Override
    public void addToMap(SinkRecord record, Map<String, Object> map) {}
  }

  private static RecordAppender getExternalFieldAppender(
      String field, Function<String, String> fieldNamer) {
    if (field == null) {
      return new NoOpRecordAppender();
    }
    List<String> parts = Splitter.on(',').splitToList(field);
    if (parts.size() != 2) {
      throw new ConfigException(
          String.format("Could not parse %s for %s", field, EXTERNAL_KAFKA_METADATA));
    }
    String fieldName = fieldNamer.apply(parts.get(0));
    String fieldValue = parts.get(1);
    return new RecordAppender() {

      @Override
      public void addToSchema(SchemaBuilder builder) {
        builder.field(fieldName, Schema.STRING_SCHEMA);
      }

      @Override
      public void addToStruct(SinkRecord record, Struct struct) {
        struct.put(fieldName, fieldValue);
      }

      @Override
      public void addToMap(SinkRecord record, Map<String, Object> map) {
        map.put(fieldName, fieldValue);
      }
    };
  }

  private static final String TOPIC = "topic";
  private static final String PARTITION = "partition";
  private static final String OFFSET = "offset";
  private static final String TIMESTAMP = "timestamp";
  private static final String EXTERNAL_KAFKA_METADATA = "external_field";
  private static final String KEY_METADATA_FIELD_NAME = "field_name";
  private static final String KEY_METADATA_IS_NESTED = "nested";
  private static final String DEFAULT_METADATA_FIELD_NAME = "_kafka_metadata";
  private static RecordAppender recordAppender;

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              KEY_METADATA_FIELD_NAME,
              ConfigDef.Type.STRING,
              DEFAULT_METADATA_FIELD_NAME,
              ConfigDef.Importance.LOW,
              "the field to append Kafka metadata under (or prefix fields with)")
          .define(
              KEY_METADATA_IS_NESTED,
              ConfigDef.Type.BOOLEAN,
              false,
              ConfigDef.Importance.LOW,
              "(true/false) to make a nested record under name or prefix names on the top level")
          .define(
              EXTERNAL_KAFKA_METADATA,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.LOW,
              "key,value representing a String to be injected on Kafka metadata (e.g. Cluster)");

  private static RecordAppender getRecordAppender(Map<String, ?> props) {
    SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    return getRecordAppender(config);
  }

  private static RecordAppender getRecordAppender(SimpleConfig config) {
    RecordAppender externalFieldAppender;
    String metadataFieldName = config.getString(KEY_METADATA_FIELD_NAME);
    Boolean nestedMetadata = config.getBoolean(KEY_METADATA_IS_NESTED);

    String topicFieldName;
    String partitionFieldName;
    String offsetFieldName;
    String timestampFieldName;

    if (nestedMetadata) {
      externalFieldAppender =
          getExternalFieldAppender(config.getString(EXTERNAL_KAFKA_METADATA), name -> name);

      SchemaBuilder nestedSchemaBuilder = SchemaBuilder.struct();
      nestedSchemaBuilder
          .field(TOPIC, Schema.STRING_SCHEMA)
          .field(PARTITION, Schema.INT32_SCHEMA)
          .field(OFFSET, Schema.INT64_SCHEMA)
          .field(TIMESTAMP, Schema.OPTIONAL_INT64_SCHEMA);
      externalFieldAppender.addToSchema(nestedSchemaBuilder);

      Schema nestedSchema = nestedSchemaBuilder.build();

      return new RecordAppender() {
        @Override
        public void addToSchema(SchemaBuilder builder) {
          builder.field(metadataFieldName, nestedSchema);
        }

        @Override
        public void addToStruct(SinkRecord record, Struct struct) {
          Struct nested = new Struct(nestedSchema);
          nested.put(TOPIC, record.topic());
          nested.put(PARTITION, record.kafkaPartition());
          nested.put(OFFSET, record.kafkaOffset());
          if (record.timestamp() != null) {
            nested.put(TIMESTAMP, record.timestamp());
          }
          externalFieldAppender.addToStruct(record, nested);
          struct.put(metadataFieldName, nested);
        }

        @Override
        public void addToMap(SinkRecord record, Map<String, Object> map) {
          Map<String, Object> nested = Maps.newHashMap();
          nested.put(TOPIC, record.topic());
          nested.put(PARTITION, record.kafkaPartition());
          nested.put(OFFSET, record.kafkaOffset());
          if (record.timestamp() != null) {
            nested.put(TIMESTAMP, record.timestamp());
          }
          externalFieldAppender.addToMap(record, nested);
          map.put(metadataFieldName, nested);
        }
      };

    } else {
      Function<String, String> namer = name -> String.format("%s_%s", metadataFieldName, name);
      topicFieldName = namer.apply(TOPIC);
      partitionFieldName = namer.apply(PARTITION);
      offsetFieldName = namer.apply(OFFSET);
      timestampFieldName = namer.apply(TIMESTAMP);

      externalFieldAppender =
          getExternalFieldAppender(config.getString(EXTERNAL_KAFKA_METADATA), namer);
      return new RecordAppender() {
        @Override
        public void addToSchema(SchemaBuilder builder) {
          builder
              .field(topicFieldName, Schema.STRING_SCHEMA)
              .field(partitionFieldName, Schema.INT32_SCHEMA)
              .field(offsetFieldName, Schema.INT64_SCHEMA)
              .field(timestampFieldName, Schema.OPTIONAL_INT64_SCHEMA);
          externalFieldAppender.addToSchema(builder);
        }

        @Override
        public void addToStruct(SinkRecord record, Struct struct) {
          struct.put(topicFieldName, record.topic());
          struct.put(partitionFieldName, record.kafkaPartition());
          struct.put(offsetFieldName, record.kafkaOffset());
          if (record.timestamp() != null) {
            struct.put(timestampFieldName, record.timestamp());
          }
          externalFieldAppender.addToStruct(record, struct);
        }

        @Override
        public void addToMap(SinkRecord record, Map<String, Object> map) {
          map.put(topicFieldName, record.topic());
          map.put(partitionFieldName, record.kafkaPartition());
          map.put(offsetFieldName, record.kafkaOffset());
          if (record.timestamp() != null) {
            map.put(timestampFieldName, record.timestamp());
          }
          externalFieldAppender.addToMap(record, map);
        }
      };
    }
  }

  @Override
  public SinkRecord apply(SinkRecord record) {
    if (record.value() == null) {
      return record;
    } else if (record.valueSchema() == null) {
      return applySchemaless(record);
    } else {
      return applyWithSchema(record);
    }
  }

  private SinkRecord applyWithSchema(SinkRecord record) {
    Struct value = Requirements.requireStruct(record.value(), "KafkaMetadataTransform");
    Schema newSchema = makeUpdatedSchema(record.valueSchema());
    Struct newValue = new Struct(newSchema);
    for (Field field : record.valueSchema().fields()) {
      newValue.put(field.name(), value.get(field));
    }
    recordAppender.addToStruct(record, newValue);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        newSchema,
        newValue,
        record.timestamp(),
        record.headers());
  }

  private Schema makeUpdatedSchema(Schema schema) {
    SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
    for (Field field : schema.fields()) {
      builder.field(field.name(), field.schema());
    }
    recordAppender.addToSchema(builder);
    return builder.build();
  }

  private SinkRecord applySchemaless(SinkRecord record) {
    Map<String, Object> value = Requirements.requireMap(record.value(), "KafkaMetadata transform");
    Map<String, Object> newValue = Maps.newHashMap(value);
    recordAppender.addToMap(record, newValue);

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        newValue,
        record.timestamp(),
        record.headers());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    recordAppender = getRecordAppender(configs);
  }
}
