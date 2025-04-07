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

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ConnectTimeTypeToIntegerTypeTransformTest {
  private final ConnectTimeTypeToIntegerTypeTransform<SourceRecord> xformValue =
      new ConnectTimeTypeToIntegerTypeTransform.Value<>();

  @AfterEach
  public void after() {
    xformValue.close();
  }

  @BeforeEach
  public void configure() {
    xformValue.configure(Collections.emptyMap());
  }

  @Test
  public void topLevelStructRequired() {
    xformValue.configure(Collections.emptyMap());
      assertThatThrownBy(
        () -> xformValue.apply(new SourceRecord(null, null, "topic", 0, Schema.INT32_SCHEMA, 42))).isExactlyInstanceOf(DataException.class);
  }

  @Test
  public void testTimeMillisConversionToInteger() {
    // Apply your transform
    SourceRecord transformed = xformValue.apply(sourceRecord());

    Struct value = (Struct) transformed.value();
    Schema schema = transformed.valueSchema();

    // Root level time fields
    assertThat(schema.field("timeMillisTest").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(schema.field("timeMillisTest").schema().name()).isNull();
    assertThat(value.get("timeMillisTest")).isEqualTo(5657625);

    assertThat(schema.field("timeMillisUnion").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(schema.field("timeMillisUnion").schema().name()).isNull();
    assertThat(value.get("timeMillisUnion")).isEqualTo(21097067);

    // Nested struct fields
    Struct nested = value.getStruct("timeFieldsNested");
    Schema nestedSchema = schema.field("timeFieldsNested").schema();

    assertThat(nestedSchema.field("timeMillisTest").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(nestedSchema.field("timeMillisTest").schema().name()).isNull();
    assertThat(nested.get("timeMillisTest")).isEqualTo(80860507);

    assertThat(nestedSchema.field("timeMillisUnion").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(nestedSchema.field("timeMillisUnion").schema().name()).isNull();
    assertThat(nested.get("timeMillisUnion")).isEqualTo(20492278);

    // Array of structs
    List<Struct> array = value.getArray("timeFieldsArray");
    Schema arrayItemSchema = schema.field("timeFieldsArray").schema().valueSchema();

    Struct item1 = array.get(0);
    assertThat(arrayItemSchema.field("timeMillisTest").schema().type())
        .isEqualTo(Schema.Type.INT32);
    assertThat(arrayItemSchema.field("timeMillisTest").schema().name()).isNull();
    assertThat(item1.get("timeMillisTest")).isEqualTo(53704441);
    assertThat(item1.get("timeMillisUnion")).isEqualTo(56111565);

    Struct item2 = array.get(1);
    assertThat(item2.get("timeMillisTest")).isEqualTo(15461634);
    assertThat(item2.get("timeMillisUnion")).isEqualTo(69350762);

    // Map of structs
    Map<String, Struct> map = value.getMap("timeFieldsMap");
    Schema mapValueSchema = schema.field("timeFieldsMap").schema().valueSchema();

    Struct mapEntry1 = map.get("key1");
    assertThat(mapValueSchema.field("timeMillisTest").schema().type()).isEqualTo(Schema.Type.INT32);
    assertThat(mapValueSchema.field("timeMillisTest").schema().name()).isNull();
    assertThat(mapEntry1.get("timeMillisTest")).isEqualTo(77234352);
    assertThat(mapEntry1.get("timeMillisUnion")).isEqualTo(65571934);

    Struct mapEntry2 = map.get("key2");
    assertThat(mapEntry2.get("timeMillisTest")).isEqualTo(2305273);
    assertThat(mapEntry2.get("timeMillisUnion")).isEqualTo(71387788);
  }

  public static SourceRecord sourceRecord() {
    // Define logical time-millis schemas
    Schema timeMillisSchema =
        SchemaBuilder.int32().name("org.apache.kafka.connect.data.Time").build();

    Schema optionalTimeMillisSchema =
        SchemaBuilder.int32().optional().name("org.apache.kafka.connect.data.Time").build();

    // Nested struct schema
    Schema timeFieldsNestedSchema =
        SchemaBuilder.struct()
            .name("my.TimeFieldsNested")
            .field("timeMillisTest", timeMillisSchema)
            .field("timeMillisUnion", optionalTimeMillisSchema)
            .build();

    // Array item schema
    Schema timeFieldsArrayItemSchema =
        SchemaBuilder.struct()
            .name("my.TimeFieldsArrayItem")
            .field("timeMillisTest", timeMillisSchema)
            .field("timeMillisUnion", optionalTimeMillisSchema)
            .build();

    Schema timeFieldsArraySchema = SchemaBuilder.array(timeFieldsArrayItemSchema);

    // Map entry schema
    Schema timeFieldsMapEntrySchema =
        SchemaBuilder.struct()
            .name("my.TimeFieldsMapEntry")
            .field("timeMillisTest", timeMillisSchema)
            .field("timeMillisUnion", optionalTimeMillisSchema)
            .build();

    Schema timeFieldsMapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, timeFieldsMapEntrySchema);

    // Root schema
    Schema schema =
        SchemaBuilder.struct()
            .name("my.TimeMillisComplexSchema")
            .field("id", Schema.INT64_SCHEMA)
            .field("timeMillisTest", timeMillisSchema)
            .field("timeMillisUnion", optionalTimeMillisSchema)
            .field("timeFieldsNested", timeFieldsNestedSchema)
            .field("timeFieldsArray", timeFieldsArraySchema)
            .field("timeFieldsMap", timeFieldsMapSchema)
            .build();

    // Populate values
    Struct timeFieldsNested =
        new Struct(timeFieldsNestedSchema)
            .put("timeMillisTest", new Date(80860507))
            .put("timeMillisUnion", new Date(20492278));

    Struct arrayItem1 =
        new Struct(timeFieldsArrayItemSchema)
            .put("timeMillisTest", new Date(53704441))
            .put("timeMillisUnion", new Date(56111565));
    Struct arrayItem2 =
        new Struct(timeFieldsArrayItemSchema)
            .put("timeMillisTest", new Date(15461634))
            .put("timeMillisUnion", new Date(69350762));

    List<Struct> timeFieldsArray = Arrays.asList(arrayItem1, arrayItem2);

    Struct mapEntry1 =
        new Struct(timeFieldsMapEntrySchema)
            .put("timeMillisTest", new Date(77234352))
            .put("timeMillisUnion", new Date(65571934));
    Struct mapEntry2 =
        new Struct(timeFieldsMapEntrySchema)
            .put("timeMillisTest", new Date(2305273))
            .put("timeMillisUnion", new Date(71387788));

    Map<String, Struct> timeFieldsMap = Maps.newHashMap();
    timeFieldsMap.put("key1", mapEntry1);
    timeFieldsMap.put("key2", mapEntry2);

    // Final struct
    Struct struct =
        new Struct(schema)
            .put("id", 1132048948483187745L)
            .put("timeMillisTest", new Date(5657625))
            .put("timeMillisUnion", new Date(21097067))
            .put("timeFieldsNested", timeFieldsNested)
            .put("timeFieldsArray", timeFieldsArray)
            .put("timeFieldsMap", timeFieldsMap);

    return new SourceRecord(null, null, "testTopic", 0, schema, struct);
  }
}
