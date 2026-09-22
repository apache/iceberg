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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TestFieldToJsonString {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testNullValue() {
    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext"));
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);
      SinkRecord result = smt.apply(record);
      assertThat(result.value()).isNull();
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessTopLevelObject() {
    Map<String, Object> clientContext = Maps.newLinkedHashMap();
    clientContext.put("initiator", "System");

    Map<String, Object> value = Maps.newHashMap();
    value.put("userId", "u1");
    value.put("clientContext", clientContext);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext"));
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);
      SinkRecord result = smt.apply(record);

      Map<String, Object> newValue = (Map<String, Object>) result.value();
      assertThat(newValue.get("userId")).isEqualTo("u1");
      assertThat(newValue.get("clientContext")).isInstanceOf(String.class);
      assertThat(readJsonAsMap((String) newValue.get("clientContext")).get("initiator"))
          .isEqualTo("System");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessNestedPath() {
    Map<String, Object> metadata = Maps.newLinkedHashMap();
    metadata.put("org", "direct");
    Map<String, Object> marketingData = Maps.newHashMap();
    marketingData.put("promoCode", "string");
    marketingData.put("metadata", metadata);
    Map<String, Object> registrationData = Maps.newHashMap();
    registrationData.put("marketingData", marketingData);
    Map<String, Object> account = Maps.newHashMap();
    account.put("registrationData", registrationData);
    Map<String, Object> value = Maps.newHashMap();
    value.put("account", account);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "account.registrationData.marketingData.metadata"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, null, value, 0));

      Map<String, Object> outAccount =
          (Map<String, Object>) ((Map<String, Object>) result.value()).get("account");
      Map<String, Object> outMarketing =
          (Map<String, Object>)
              ((Map<String, Object>) outAccount.get("registrationData")).get("marketingData");
      assertThat(outMarketing.get("promoCode")).isEqualTo("string");
      assertThat(outMarketing.get("metadata")).isInstanceOf(String.class);
      assertThat(readJsonAsMap((String) outMarketing.get("metadata")).get("org"))
          .isEqualTo("direct");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessMultipleFields() {
    Map<String, Object> clientContext = Maps.newHashMap();
    clientContext.put("initiator", "System");
    Map<String, Object> metadata = Maps.newHashMap();
    metadata.put("k", "v");
    Map<String, Object> account = Maps.newHashMap();
    account.put("metadata", metadata);
    Map<String, Object> value = Maps.newHashMap();
    value.put("clientContext", clientContext);
    value.put("account", account);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext,account.metadata"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, null, value, 0));

      Map<String, Object> newValue = (Map<String, Object>) result.value();
      Map<String, Object> outAccount = (Map<String, Object>) newValue.get("account");
      assertThat(newValue.get("clientContext")).isInstanceOf(String.class);
      assertThat(outAccount.get("metadata")).isInstanceOf(String.class);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessAlreadyStringUntouched() {
    Map<String, Object> value = Maps.newHashMap();
    value.put("metadata", "{\"already\":\"json\"}");

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "metadata"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, null, value, 0));
      Map<String, Object> newValue = (Map<String, Object>) result.value();
      assertThat(newValue.get("metadata")).isEqualTo("{\"already\":\"json\"}");
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSchemalessMissingAndNullIgnored() {
    Map<String, Object> value = Maps.newHashMap();
    value.put("clientContext", null);
    value.put("userId", "u1");

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext,account.metadata"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, null, value, 0));
      Map<String, Object> newValue = (Map<String, Object>) result.value();
      assertThat(newValue.get("clientContext")).isNull();
      assertThat(newValue.get("userId")).isEqualTo("u1");
      assertThat(newValue).doesNotContainKey("account");
    }
  }

  @Test
  public void testSchemalessMissingThrowsWhenIgnoreDisabled() {
    Map<String, Object> value = Maps.newHashMap();
    value.put("userId", "u1");

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext", "ignore.missing", "false"));
      SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);
      assertThatThrownBy(() -> smt.apply(record))
          .isInstanceOf(DataException.class)
          .hasMessageContaining("clientContext");
    }
  }

  @Test
  public void testSchemaTopLevelStructBecomesString() {
    Schema clientContextSchema =
        SchemaBuilder.struct().field("initiator", Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("userId", Schema.STRING_SCHEMA)
            .field("clientContext", clientContextSchema)
            .build();
    Struct clientContext = new Struct(clientContextSchema).put("initiator", "System");
    Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", clientContext);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, valueSchema, value, 0));

      Struct newValue = (Struct) result.value();
      assertThat(newValue.schema().field("clientContext").schema().type())
          .isEqualTo(Schema.Type.STRING);
      assertThat(newValue.get("clientContext")).isInstanceOf(String.class);
      assertThat(readJsonAsMap((String) newValue.get("clientContext")).get("initiator"))
          .isEqualTo("System");
      assertThat(newValue.get("userId")).isEqualTo("u1");
    }
  }

  @Test
  public void testSchemaNestedStructBecomesString() {
    Schema metadataSchema =
        SchemaBuilder.struct().field("org", Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    Schema accountSchema =
        SchemaBuilder.struct()
            .field("currency", Schema.OPTIONAL_STRING_SCHEMA)
            .field("metadata", metadataSchema)
            .build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("userId", Schema.STRING_SCHEMA)
            .field("account", accountSchema)
            .build();
    Struct metadata = new Struct(metadataSchema).put("org", "direct");
    Struct account = new Struct(accountSchema).put("currency", "EUR").put("metadata", metadata);
    Struct value = new Struct(valueSchema).put("userId", "u1").put("account", account);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "account.metadata"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, valueSchema, value, 0));

      Struct newValue = (Struct) result.value();
      Struct outAccount = (Struct) newValue.get("account");
      assertThat(newValue.schema().field("account").schema().type()).isEqualTo(Schema.Type.STRUCT);
      assertThat(outAccount.schema().field("metadata").schema().type())
          .isEqualTo(Schema.Type.STRING);
      assertThat(outAccount.get("currency")).isEqualTo("EUR");
      assertThat(readJsonAsMap((String) outAccount.get("metadata")).get("org")).isEqualTo("direct");
    }
  }

  @Test
  public void testSchemaNullLeafStaysNull() {
    Schema clientContextSchema =
        SchemaBuilder.struct().field("initiator", Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("userId", Schema.STRING_SCHEMA)
            .field("clientContext", clientContextSchema)
            .build();
    Struct value = new Struct(valueSchema).put("userId", "u1").put("clientContext", null);

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext"));
      SinkRecord result = smt.apply(new SinkRecord("topic", 0, null, null, valueSchema, value, 0));

      Struct newValue = (Struct) result.value();
      assertThat(newValue.schema().field("clientContext").schema().type())
          .isEqualTo(Schema.Type.STRING);
      assertThat(newValue.get("clientContext")).isNull();
    }
  }

  @Test
  public void testSchemaCacheReused() {
    Schema clientContextSchema =
        SchemaBuilder.struct().field("initiator", Schema.OPTIONAL_STRING_SCHEMA).optional().build();
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("userId", Schema.STRING_SCHEMA)
            .field("clientContext", clientContextSchema)
            .build();

    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      smt.configure(ImmutableMap.of("fields", "clientContext"));
      Schema firstSchema = null;
      for (int i = 0; i < 5; i++) {
        Struct value =
            new Struct(valueSchema)
                .put("userId", "u" + i)
                .put("clientContext", new Struct(clientContextSchema).put("initiator", "System"));
        SinkRecord result =
            smt.apply(new SinkRecord("topic", 0, null, null, valueSchema, value, i));
        if (firstSchema == null) {
          firstSchema = result.valueSchema();
        } else {
          assertThat(result.valueSchema()).isSameAs(firstSchema);
        }
      }
    }
  }

  @Test
  public void testConfigureRequiresFields() {
    try (FieldToJsonString<SinkRecord> smt = new FieldToJsonString<>()) {
      assertThatThrownBy(() -> smt.configure(Maps.newHashMap()))
          .isInstanceOf(Exception.class)
          .hasMessageContaining("fields");
    }
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> readJsonAsMap(String json) {
    try {
      JsonNode node = MAPPER.readTree(json);
      return MAPPER.convertValue(node, Map.class);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
