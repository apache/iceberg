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
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.ExtractField;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.debezium.connector.mongodb.transforms.ArrayEncoding;
import org.debezium.connector.mongodb.transforms.MongoDataConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debezium Mongo Connector generates the CDC before/after fields as BSON strings. This SMT converts
 * those strings into typed SinkRecord Structs by inferring the schema from the BSON node types.
 */
public class MongoDebeziumTransform implements Transformation<SinkRecord> {

  public static final String ARRAY_HANDLING_MODE_KEY = "array_handling_mode";

  public static final String RECORD_ENVELOPE_KEY_SCHEMA_NAME_SUFFIX = ".Key";

  public static final String SCHEMA_NAME_SUFFIX = ".Envelope";

  private static final String UPDATE_DESCRIPTION = "updateDescription";

  private static final String REMOVED_FIELDS = "removedFields";
  private static final String UPDATED_FIELDS = "updatedFields";

  private static final String AFTER_FIELD_NAME = "after";
  private static final String BEFORE_FIELD_NAME = "before";
  private final ExtractField<SinkRecord> updateDescriptionExtractor =
      extractorValueField(UPDATE_DESCRIPTION);

  private final ExtractField<SinkRecord> afterExtractor = extractorValueField(AFTER_FIELD_NAME);

  private final ExtractField<SinkRecord> beforeExtractor = extractorValueField(BEFORE_FIELD_NAME);

  private final ExtractField<SinkRecord> keyIdExtractor = extractorForKeyField("id");

  private MongoDataConverter converter;

  private static final Logger LOG = LoggerFactory.getLogger(MongoDebeziumTransform.class.getName());

  private static final ConfigDef CONFIG_DEF =
      new ConfigDef()
          .define(
              ARRAY_HANDLING_MODE_KEY,
              ConfigDef.Type.STRING,
              null,
              ConfigDef.Importance.MEDIUM,
              "array or document handling for mongodb arrays");

  @Override
  public SinkRecord apply(SinkRecord record) {
    // pass tombstones as-is
    if (record.value() == null) {
      return record;
    }

    // if they don't contain key/envelope convert to tombstone
    if (!isValidKey(record) || !isValidValue(record)) {
      LOG.debug(
          "Expected Key Schema/Envelope for transformation, converting to tombstone. Message key: \"{}\"",
          record.key());
      return record.newRecord(
          record.topic(),
          record.kafkaPartition(),
          null,
          null,
          null,
          null,
          record.timestamp(),
          record.headers());
    }

    final SinkRecord keyIdRecord = keyIdExtractor.apply(record);
    final SinkRecord afterRecord = afterExtractor.apply(record);
    final SinkRecord beforeRecord = beforeExtractor.apply(record);
    final SinkRecord updateDescriptionRecord = updateDescriptionExtractor.apply(record);

    if (beforeRecord.value() == null
        && afterRecord.value() == null
        && updateDescriptionRecord.value() == null) {
      throw new IllegalArgumentException(
          String.format(
              "malformed record topic: %s, partition: %s, offset: %s",
              record.topic(), record.kafkaPartition(), record.kafkaOffset()));
    }

    BsonDocument afterBson = null;
    BsonDocument beforeBson = null;
    BsonDocument keyBson = BsonDocument.parse("{ \"id\" : " + keyIdRecord.key().toString() + "}");

    if (beforeRecord.value() != null) {
      beforeBson = BsonDocument.parse(beforeRecord.value().toString());
    }
    if (afterRecord.value() == null && updateDescriptionRecord.value() != null) {
      afterBson =
          buildAfterBsonFromPartials(
              updateDescriptionRecord,
              (beforeBson == null) ? new BsonDocument() : beforeBson.clone(),
              keyBson);
    } else if (afterRecord.value() != null) {
      afterBson = BsonDocument.parse(afterRecord.value().toString());
    }

    return newRecord(record, keyBson, beforeBson, afterBson);
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {
    ArrayEncoding arrayMode =
        ArrayEncoding.parse((String) configs.get(ARRAY_HANDLING_MODE_KEY), "array");
    converter = new MongoDataConverter(arrayMode);
  }

  /**
   * Debezium can produce partial updates in three different configurations. It may contain a before
   * value if `capture.mode` is set to one of the `*_with_pre_image` options. It may contain an
   * after value if `capture.mode` is set to change_streams_update_full
   *
   * <p>Enter this method when there is no after value but updateDescription is present.
   *
   * <p>If before is present, it will merge the updateDescription with the fields present in before
   * If before is not present, it constructs an after containing only the values present in
   * updatedFields
   *
   * @param updateDescriptionRecord Struct on key updateDescriptions
   * @param initialDocument parsed Bson of the before field (maybe an empty document) to bootstrap
   * @return Bson representing the After fields
   */
  private BsonDocument buildAfterBsonFromPartials(
      SinkRecord updateDescriptionRecord, BsonDocument initialDocument, BsonDocument keyBson) {

    Struct updateAsStruct =
        Requirements.requireStruct(updateDescriptionRecord.value(), UPDATE_DESCRIPTION);

    String updated = updateAsStruct.getString(UPDATED_FIELDS);
    List<String> removed = updateAsStruct.getArray(REMOVED_FIELDS);

    BsonDocument updatedBson = BsonDocument.parse(updated);
    for (Map.Entry<String, BsonValue> valueEntry : updatedBson.entrySet()) {
      initialDocument.append(valueEntry.getKey(), valueEntry.getValue());
    }

    if (removed != null) {
      for (String field : removed) {
        initialDocument.keySet().remove(field);
      }
    }
    // in a partial update it's possible the updated fields do not include the primary key
    // so bump it from the key.  Note: type may be downcast.
    if (!initialDocument.containsKey("_id")) {
      initialDocument.append("_id", keyBson.get("id"));
    }

    return initialDocument;
  }

  private SinkRecord newRecord(
      SinkRecord record,
      BsonDocument keyDocument,
      BsonDocument beforeBson,
      BsonDocument afterBson) {
    SchemaBuilder keySchemaBuilder = SchemaBuilder.struct();
    Set<Map.Entry<String, BsonValue>> keyPairs = keyDocument.entrySet();
    for (Map.Entry<String, BsonValue> keyPairsForSchema : keyPairs) {
      converter.addFieldSchema(keyPairsForSchema, keySchemaBuilder);
    }

    Schema newKeySchema = keySchemaBuilder.build();
    Struct newKeyStruct = new Struct(newKeySchema);

    for (Map.Entry<String, BsonValue> keyPairsForStruct : keyPairs) {
      converter.convertRecord(keyPairsForStruct, newKeySchema, newKeyStruct);
    }

    Struct oldValue =
        Requirements.requireStruct(record.value(), "copying existing fields besides before/after");
    Schema oldSchema = oldValue.schema();

    SchemaBuilder newValueSchemaBuilder = SchemaBuilder.struct().name(oldSchema.name());

    oldSchema
        .fields()
        .forEach(
            field -> {
              if (field.name().equals(AFTER_FIELD_NAME)) {
                if (afterBson != null) {
                  mutateBuilderFromBson(newValueSchemaBuilder, afterBson, AFTER_FIELD_NAME);
                }
              } else if (field.name().equals(BEFORE_FIELD_NAME)) {
                if (beforeBson != null) {
                  mutateBuilderFromBson(newValueSchemaBuilder, beforeBson, BEFORE_FIELD_NAME);
                }
              } else {
                newValueSchemaBuilder.field(field.name(), field.schema());
              }
            });

    Schema newValueSchema = newValueSchemaBuilder.build();
    Struct newValueStruct = new Struct(newValueSchemaBuilder.build());

    newValueSchema
        .fields()
        .forEach(
            field -> {
              if (field.name().equals(AFTER_FIELD_NAME)) {
                if (afterBson != null) {
                  newValueStruct.put(field.name(), fillStructFromBson(field.schema(), afterBson));
                }
              } else if (field.name().equals(BEFORE_FIELD_NAME)) {
                if (beforeBson != null) {
                  newValueStruct.put(field.name(), fillStructFromBson(field.schema(), beforeBson));
                }
              } else {
                newValueStruct.put(field.name(), oldValue.get(field.name()));
              }
            });
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        newKeySchema,
        newKeyStruct,
        newValueSchema,
        newValueStruct,
        record.timestamp(),
        record.headers());
  }

  private void mutateBuilderFromBson(SchemaBuilder builder, BsonDocument bson, String fieldName) {
    SchemaBuilder innerBuilder = SchemaBuilder.struct();
    Set<Map.Entry<String, BsonValue>> pairs = bson.entrySet();
    for (Map.Entry<String, BsonValue> pairsForSchema : pairs) {
      converter.addFieldSchema(pairsForSchema, innerBuilder);
    }
    builder.field(fieldName, innerBuilder.optional().build());
  }

  private Struct fillStructFromBson(Schema schema, BsonDocument bson) {
    Struct struct = new Struct(schema);
    Set<Map.Entry<String, BsonValue>> pairs = bson.entrySet();
    for (Map.Entry<String, BsonValue> pairsForSchema : pairs) {
      converter.convertRecord(pairsForSchema, schema, struct);
    }
    return struct;
  }

  private boolean isValidKey(final SinkRecord record) {
    return record.keySchema() != null
        && record.keySchema().name() != null
        && record.keySchema().name().endsWith(RECORD_ENVELOPE_KEY_SCHEMA_NAME_SUFFIX);
  }

  private boolean isValidValue(final SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().name() != null
        && record.valueSchema().name().endsWith(SCHEMA_NAME_SUFFIX);
  }

  private static <R extends ConnectRecord<R>> ExtractField<R> extractorValueField(String field) {
    ExtractField<R> extractField = new ExtractField.Value<>();
    Map<String, String> target = Maps.newHashMap();
    target.put("field", field);
    extractField.configure(target);
    return extractField;
  }

  private static ExtractField<SinkRecord> extractorForKeyField(String field) {
    ExtractField<SinkRecord> extractField = new ExtractField.Key<>();
    Map<String, String> target = Maps.newHashMap();
    target.put("field", field);
    extractField.configure(target);
    return extractField;
  }

  @SuppressWarnings("unused")
  private String kafkaMetadataForException(SinkRecord record) {
    return String.format(
        "topic: %s, partition: %s, offset: %s",
        record.topic(), record.kafkaPartition(), record.kafkaOffset());
  }
}
