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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.util.Requirements;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

public class MongoDebeziumTransformTest {
  private static final String TIMESTAMP_KEY = "ts_ms";
  private static final String TEST_TOPIC = "topic";
  private static final int TEST_PARTITION = 0;
  private static final long DEFAULT_ID_LONG = 1004L;
  private static final int DEFAULT_ID_INT = 1004;
  private static final long DEFAULT_TS_MS = 1558965515240L;
  private static final long DEFAULT_SOURCE_TS_MS = 1558965508000L;
  private static final String DEFAULT_SOURCE_VERSION = "2.5.2.final";
  private static final String DEFAULT_CONNECTOR = "mongo";
  private static final Schema DEFAULT_KEY_SCHEMA = getMongoKeySchema(true);
  private static final Struct DEFAULT_KEY_STRUCT = getMongoKeyStruct(DEFAULT_KEY_SCHEMA);
  private static final Schema DEFAULT_VALUE_SCHEMA = getMongoValueSchema(true);
  private static final Struct DEFAULT_SOURCE_STRUCT = getDefaultSourceStruct(DEFAULT_VALUE_SCHEMA);

  private String getFile(String fileName) throws IOException, URISyntaxException {
    URL jsonResource = getClass().getClassLoader().getResource(fileName);
    return new String(Files.readAllBytes(Paths.get(jsonResource.toURI())), StandardCharsets.UTF_8);
  }

  private MongoDebeziumTransform getTransformer(String mode) {
    MongoDebeziumTransform transform = new MongoDebeziumTransform();
    transform.configure(
        Collections.singletonMap(MongoDebeziumTransform.ARRAY_HANDLING_MODE_KEY, mode));
    return transform;
  }

  private static Schema optionalJsonField() {
    return SchemaBuilder.string().optional().name("io.debezium.data.Json").version(1).build();
  }

  private static Schema getUpdateDescriptionSchema() {
    Schema truncatedArraySchema =
        SchemaBuilder.struct()
            .name("io.debezium.connector.mongodb.changestream.truncatedarray")
            .version(1)
            .field("field", Schema.STRING_SCHEMA)
            .field("size", Schema.INT32_SCHEMA)
            .build();

    return SchemaBuilder.struct()
        .optional()
        .name("io.debezium.connector.mongodb.changestream.updatedescription")
        .version(1)
        .field("removedFields", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
        .field("updatedFields", optionalJsonField())
        .field("truncatedArrays", SchemaBuilder.array(truncatedArraySchema).optional().build())
        .build();
  }

  private static Struct getUpdateDescriptionStruct(
      Optional<List<String>> removedFields, String updatedFields) {
    Struct struct = new Struct(getUpdateDescriptionSchema());
    removedFields.map(fields -> struct.put("removedFields", fields));
    struct.put("updatedFields", updatedFields);
    return struct;
  }

  private static Schema getMongoValueSchema(boolean validEnvelope) {
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.field("after", optionalJsonField());
    builder.field("before", optionalJsonField());
    builder.field("op", Schema.OPTIONAL_STRING_SCHEMA);
    builder.field("ts_ms", Schema.OPTIONAL_INT64_SCHEMA);
    builder.name(
        (validEnvelope) ? "dbserver1.inventory.customers.Envelope" : "some_invalid_envelope_name");
    // in the docs this doesn't appear on create or delete events
    // but in debezium mongo code + their mongo SMT, it assumes it is present in the schema
    builder.field("updateDescription", getUpdateDescriptionSchema());

    SchemaBuilder sourceBuilder = SchemaBuilder.struct();
    sourceBuilder.name("io.debezium.connector.mongo.Source");
    sourceBuilder.field("version", Schema.STRING_SCHEMA);
    sourceBuilder.field("connector", Schema.STRING_SCHEMA);
    sourceBuilder.field("ts_ms", Schema.INT64_SCHEMA);
    // ignoring other fields like db, rs, tord, stxnid, lsid, txnNumber, etc
    // metadata not important to the test.
    builder.field("source", sourceBuilder.build());
    return builder.build();
  }

  private static Schema getMongoKeySchema(boolean validKey) {
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name((validKey) ? "dbserver1.inventory.customers.Key" : "some_invalid_key_name");
    builder.field("id", Schema.INT64_SCHEMA);
    return builder.build();
  }

  private static Struct getMongoKeyStruct(Schema schema) {
    Struct struct = new Struct(schema);
    struct.put("id", DEFAULT_ID_LONG);
    return struct;
  }

  private static Struct getDefaultSourceStruct(Schema schema) {
    Schema sourceSchema = schema.field("source").schema();
    Struct struct = new Struct(sourceSchema);
    struct.put("version", DEFAULT_SOURCE_VERSION);
    struct.put("connector", DEFAULT_CONNECTOR);
    struct.put("ts_ms", DEFAULT_SOURCE_TS_MS);
    return struct;
  }

  private static <T> T extractFieldAtImpl(
      Object struct, String path, Function<Object, T> fn, Boolean safe) throws Exception {
    try {
      String[] paths = path.split("\\.");
      int size = paths.length;
      Object target = Requirements.requireStruct(struct, "extractFieldAt for test");
      for (int i = 0; i < size; i++) {
        String localPath = paths[i];
        if (i == size - 1) {
          return fn.apply(((Struct) target).get(localPath));
        }
        target = Requirements.requireStruct(target, "extractFieldAt dive").get(localPath);
      }
      throw new Exception(String.format("could not extract field: %s", path));
    } catch (Exception e) {
      if (safe) {
        return null;
      } else {
        throw e;
      }
    }
  }

  private static <T> T extractFieldAt(Object struct, String path, Function<Object, T> fn)
      throws Exception {
    return extractFieldAtImpl(struct, path, fn, false);
  }

  private static String extractStringAt(Object struct, String path) throws Exception {
    return extractFieldAt(struct, path, s -> (String) s);
  }

  private static Struct extractStructAt(Object struct, String path) throws Exception {
    return extractFieldAt(struct, path, obj -> (Struct) obj);
  }

  private static long extractLongAt(Object struct, String path) throws Exception {
    return extractFieldAt(struct, path, l -> (Long) l);
  }

  private static int extractIntAt(Object struct, String path) throws Exception {
    return extractFieldAt(struct, path, i -> (int) i);
  }

  private static void assertFieldNotExists(Object struct, String path) throws Exception {
    if (extractFieldAtImpl(struct, path, i -> i, true) != null) {
      throw new AssertionError(String.format("%s exists on struct", path));
    }
  }

  private static Schema.Type extractSchemaTypeAt(Object struct, String path) throws Exception {
    Struct asStruct = Requirements.requireStruct(struct, "extractSchemaAt in test");
    String[] paths = path.split("\\.");
    int size = paths.length;

    Schema target = asStruct.schema();
    for (int i = 0; i < size; i++) {
      String localPath = paths[i];
      if (i == size - 1) {
        return target.field(localPath).schema().type();
      }
      target = target.field(localPath).schema();
    }
    throw new Exception(String.format("could not extract schema at: %s", path));
  }

  @Test
  @DisplayName("Tombstone records are returned as-is")
  public void shouldConvertHeartbeatMessagesToTombstones() {
    MongoDebeziumTransform smt = getTransformer("array");

    final SinkRecord record =
        new SinkRecord(TEST_TOPIC, TEST_PARTITION, null, null, null, null, 0L);
    assertThat(smt.apply(record)).isSameAs(record);
  }

  @Test
  @DisplayName("Records with incorrect key are converted to null values")
  public void shouldConvertRecordsWithIncorrectKey() {

    MongoDebeziumTransform smt = getTransformer("array");

    // name should end with .Key according to the spec
    Schema valueSchema =
        SchemaBuilder.struct()
            .name("some_key_name_missing_key_suffix")
            .field(TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema).put(TIMESTAMP_KEY, 1444587054854L);

    Schema keySchema =
        SchemaBuilder.struct()
            .name("some_key_name.Envelope")
            .field("serverName", Schema.STRING_SCHEMA)
            .build();

    Struct key = new Struct(keySchema).put("serverName", "op.with.heartbeat");

    final SinkRecord record =
        new SinkRecord(TEST_TOPIC, TEST_PARTITION, keySchema, key, valueSchema, value, 0L);

    SinkRecord result = smt.apply(record);

    assertThat(result).isNotSameAs(record);
    assertThat(result.value()).isNull();
  }

  @Test
  @DisplayName("Records with incorrect envelope are converted to null values")
  public void shouldConvertRecordsWithIncorrectEnvelope() {
    MongoDebeziumTransform smt = getTransformer("array");

    Schema valueSchema =
        SchemaBuilder.struct()
            .name("some_key_name.Key")
            .field(TIMESTAMP_KEY, Schema.INT64_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema).put(TIMESTAMP_KEY, 1444587054854L);

    // name should end with .Envelope according to the spec
    Schema keySchema =
        SchemaBuilder.struct()
            .name("some_key_name_missing_envelope_suffix")
            .field("serverName", Schema.STRING_SCHEMA)
            .build();

    Struct key = new Struct(keySchema).put("serverName", "op.with.heartbeat");

    final SinkRecord record =
        new SinkRecord(TEST_TOPIC, TEST_PARTITION, keySchema, key, valueSchema, value, 0L);

    SinkRecord result = smt.apply(record);

    assertThat(result).isNotSameAs(record);
    assertThat(result.value()).isNull();
  }

  @Test
  @DisplayName("key events may be casted from the original record")
  public void shouldMaintainKey() throws Exception {

    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("after", getFile("mongo_create_event_after.json"));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "c");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");

    SinkRecord result = smt.apply(record);
    Struct keyResult = (Struct) result.key();

    // has to do with BSON type inference
    assertThat(extractSchemaTypeAt(DEFAULT_KEY_STRUCT, "id")).isEqualTo(Schema.Type.INT64);
    assertThat(extractIntAt(keyResult, "id")).isEqualTo(DEFAULT_ID_INT);
    assertThat(result.keySchema().field("id").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(extractSchemaTypeAt(keyResult, "id")).isEqualTo(Schema.Type.INT32);
  }

  @Test
  @DisplayName("create events are converted to SinkRecord structs")
  public void shouldConvertCreateEvents() throws Exception {

    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("after", getFile("mongo_create_event_after.json"));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "c");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");

    SinkRecord result = smt.apply(record);
    assertThat(result).isNotSameAs(record);

    assertFieldNotExists(result.value(), "before");
    assertThat(extractStructAt(result.value(), "source"))
        .isEqualTo(extractStructAt(valueStruct, "source"));
    assertThat(extractStringAt(result.value(), "op")).isEqualTo("c");
    assertThat(extractLongAt(result.value(), "ts_ms")).isEqualTo(DEFAULT_TS_MS);
    assertThat(result.kafkaOffset()).isEqualTo(0L);
    assertThat(result.kafkaPartition()).isEqualTo(TEST_PARTITION);
    assertThat(result.topic()).isEqualTo(TEST_TOPIC);
    assertThat(result.timestamp()).isEqualTo(DEFAULT_TS_MS);
    assertThat(extractLongAt(result.value(), "after._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "after.first_name")).isEqualTo("Anne");
    assertThat(extractStringAt(result.value(), "after.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "after.email")).isEqualTo("annek@noanswer.org");
  }

  @Test
  @DisplayName("delete events are converted to SinkRecord structs")
  public void shouldConvertDeleteEvents() throws Exception {
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("before", getFile("mongo_delete_event_before.json"));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "d");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");

    SinkRecord result = smt.apply(record);
    assertThat(result).isNotSameAs(record);
    assertFieldNotExists(result.value(), "after");
    assertThat(extractStringAt(result.value(), "op")).isEqualTo("d");
    assertThat(extractLongAt(result.value(), "before._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "before.first_name")).isEqualTo("unknown");
    assertThat(extractStringAt(result.value(), "before.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "before.email")).isEqualTo("annek@noanswer.org");
  }

  @Test
  @DisplayName("fully specified update events are converted to SinkRecord structs")
  public void shouldConvertFullySpecifiedUpdateEvents() throws Exception {
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("before", getFile("mongo_update_event_before.json"));
    valueStruct.put("after", getFile("mongo_update_event_after.json"));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "u");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");
    SinkRecord result = smt.apply(record);

    assertThat(extractStringAt(result.value(), "op")).isEqualTo("u");
    assertThat(extractLongAt(result.value(), "before._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "before.first_name")).isEqualTo("unknown");
    assertThat(extractStringAt(result.value(), "before.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "before.email")).isEqualTo("annek@noanswer.org");
    assertThat(extractLongAt(result.value(), "after._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "after.first_name")).isEqualTo("Anne Marie");
    assertThat(extractStringAt(result.value(), "after.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "after.email")).isEqualTo("annek@noanswer.org");
  }

  @Test
  @DisplayName(
      "partial updates with before values should merge updatedFields with before values for SinkRecord after struct")
  public void shouldConvertPartialUpdateWithBeforeValues() throws Exception {
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("before", getFile("mongo_update_event_before.json"));
    valueStruct.put(
        "updateDescription",
        getUpdateDescriptionStruct(
            Optional.empty(), getFile("mongo_update_event_updated_fields.json")));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "u");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");
    SinkRecord result = smt.apply(record);

    assertThat(extractStringAt(result.value(), "op")).isEqualTo("u");
    assertThat(extractLongAt(result.value(), "before._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "before.first_name")).isEqualTo("unknown");
    assertThat(extractStringAt(result.value(), "before.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "before.email")).isEqualTo("annek@noanswer.org");
    assertThat(extractLongAt(result.value(), "after._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "after.first_name")).isEqualTo("Anne Marie");
    assertThat(extractStringAt(result.value(), "after.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "after.email")).isEqualTo("annek@noanswer.org");
  }

  @Test
  @DisplayName("partial updates with removedFields should not be present on merged after struct")
  public void shouldConvertPartialUpdateWithBeforeValuesRemovedFields() throws Exception {
    List<String> removedFields = Lists.newArrayList();
    removedFields.add("last_name");
    removedFields.add("email");
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("before", getFile("mongo_update_event_before.json"));
    valueStruct.put(
        "updateDescription",
        getUpdateDescriptionStruct(
            Optional.of(removedFields), getFile("mongo_update_event_updated_fields.json")));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "u");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");
    SinkRecord result = smt.apply(record);

    assertThat(extractStringAt(result.value(), "op")).isEqualTo("u");
    assertThat(extractLongAt(result.value(), "before._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "before.first_name")).isEqualTo("unknown");
    assertThat(extractStringAt(result.value(), "before.last_name")).isEqualTo("Kretchmar");
    assertThat(extractStringAt(result.value(), "before.email")).isEqualTo("annek@noanswer.org");
    assertThat(extractLongAt(result.value(), "after._id")).isEqualTo(1004L);
    assertThat(extractStringAt(result.value(), "after.first_name")).isEqualTo("Anne Marie");
    assertFieldNotExists(result.value(), "after.last_name");
    assertFieldNotExists(result.value(), "after.email");
  }

  @Test
  @DisplayName(
      "partial updates without before/after values should include updateFields and id column for SinkRecord after struct")
  public void shouldConvertPartiaulUpdateBeforeAfterMissing() throws Exception {
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put(
        "updateDescription",
        getUpdateDescriptionStruct(
            Optional.empty(), getFile("mongo_update_event_updated_fields.json")));
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "u");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");
    SinkRecord result = smt.apply(record);

    assertFieldNotExists(result.value(), "before");
    assertThat(extractStringAt(result.value(), "op")).isEqualTo("u");

    // PK is Long on the key but when bumped to the value it becomes Int  in the converted record
    // due to BSON type inference
    assertThat(extractSchemaTypeAt(result.value(), "after._id")).isEqualTo(Schema.Type.INT32);
    assertThat(result.valueSchema().field("after").schema().field("_id").schema().type())
        .isEqualTo(Schema.Type.INT32);
    assertThat(extractIntAt(result.value(), "after._id")).isEqualTo(1004);
    assertThat(extractStringAt(result.value(), "after.first_name")).isEqualTo("Anne Marie");
  }

  @Test
  @DisplayName("missing all of before, after, and updatedDescription fields should throw exception")
  public void shouldThrowExceptionWhenMissingAllRequiredFields() {
    Struct valueStruct = new Struct(DEFAULT_VALUE_SCHEMA);
    valueStruct.put("source", DEFAULT_SOURCE_STRUCT);
    valueStruct.put("ts_ms", DEFAULT_TS_MS);
    valueStruct.put("op", "u");
    SinkRecord record =
        new SinkRecord(
            TEST_TOPIC,
            TEST_PARTITION,
            DEFAULT_KEY_SCHEMA,
            DEFAULT_KEY_STRUCT,
            DEFAULT_VALUE_SCHEMA,
            valueStruct,
            0L,
            DEFAULT_TS_MS,
            TimestampType.CREATE_TIME);

    MongoDebeziumTransform smt = getTransformer("array");
    assertThatThrownBy(() -> smt.apply(record)).isInstanceOf(IllegalArgumentException.class);
  }
}
