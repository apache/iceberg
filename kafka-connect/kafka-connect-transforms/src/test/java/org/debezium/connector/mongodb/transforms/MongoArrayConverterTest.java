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
package org.debezium.connector.mongodb.transforms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit test for {@code MongoDataConverter} that verifies array types.
 *
 * @author Jiri Pechanec
 */
public class MongoArrayConverterTest {

  public static String lines(String... lines) {
    final StringBuilder sb = new StringBuilder();
    Arrays.stream(lines).forEach(line -> sb.append(line).append(System.lineSeparator()));
    return sb.toString();
  }

  private static final String HETEROGENOUS_ARRAY =
      lines(
          "{", "    \"_id\": 1,", "    \"a2\": [", "        11,", "        \"abc\"", "    ]", "}");

  private static final String EMPTY_ARRAY = lines("{", "    \"_id\": 1,", "    \"f\": []", "}");

  private static final String HETEROGENOUS_DOCUMENT_IN_ARRAY =
      lines(
          "{",
          "    \"_id\": 1,",
          "    \"a1\": [",
          "        {",
          "            \"a\": 1",
          "        },",
          "        {",
          "            \"a\": \"c\"",
          "        }",
          "    ],",
          "}");

  private static final String HOMOGENOUS_ARRAYS =
      lines(
          "{",
          "  \"_id\": 1,",
          "  \"a1\": [",
          "      {",
          "          \"a\": 1",
          "      },",
          "      {",
          "          \"b\": \"c\"",
          "      }",
          "  ],",
          "  \"a2\": [",
          "      \"11\",",
          "      \"abc\"",
          "  ],",
          "  \"empty\": [],",
          "  \"additionalContacts\": [",
          "    {",
          "      \"firstName\": \"John\",",
          "      \"lastName\": \"Doe\",",
          "      \"comment\": null",
          "    },",
          "    {",
          "      \"firstName\": \"Jane\",",
          "      \"lastName\": \"Doe\",",
          "      \"comment\": \"A comment\"",
          "    }",
          "  ]",
          "}");

  private SchemaBuilder builder;

  @BeforeEach
  public void setUpBeforeEach() throws Exception {
    builder = SchemaBuilder.struct().name("array");
  }

  @Test
  public void shouldDetectHeterogenousArray() {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);

    assertThatThrownBy(
            () -> {
              for (Entry<String, BsonValue> entry : val.entrySet()) {
                converter.addFieldSchema(entry, builder);
              }
            })
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void shouldDetectHeterogenousDocumentInArray() {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);

    assertThatThrownBy(
            () -> {
              for (Entry<String, BsonValue> entry : val.entrySet()) {
                converter.addFieldSchema(entry, builder);
              }
            })
        .isInstanceOf(RuntimeException.class);
  }

  @Test
  public void shouldCreateSchemaForHomogenousArray() {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, builder);
    }
    final Schema finalSchema = builder.build();

    assertThat(finalSchema)
        .isEqualTo(
            SchemaBuilder.struct()
                .name("array")
                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                .field(
                    "a1",
                    SchemaBuilder.array(
                            SchemaBuilder.struct()
                                .name("array.a1")
                                .optional()
                                .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("b", Schema.OPTIONAL_STRING_SCHEMA)
                                .build())
                        .optional()
                        .build())
                .field("a2", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field(
                    "empty", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .field(
                    "additionalContacts",
                    SchemaBuilder.array(
                            SchemaBuilder.struct()
                                .name("array.additionalContacts")
                                .optional()
                                .field("firstName", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("lastName", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("comment", Schema.OPTIONAL_STRING_SCHEMA)
                                .build())
                        .optional()
                        .build())
                .build());
  }

  @Test
  public void shouldCreateStructForHomogenousArray() {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.ARRAY);
    final BsonDocument val = BsonDocument.parse(HOMOGENOUS_ARRAYS);
    final SchemaBuilder schemaBuilder = SchemaBuilder.struct().name("array");

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, schemaBuilder);
    }

    final Schema finalSchema = schemaBuilder.build();
    final Struct struct = new Struct(finalSchema);

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.convertRecord(entry, finalSchema, struct);
    }

    // @formatter:off
    assertThat(struct.toString())
        .isEqualTo(
            "Struct{"
                + "_id=1,"
                + "a1=["
                + "Struct{a=1}, "
                + "Struct{b=c}"
                + "],"
                + "a2=[11, abc],"
                + "empty=[],"
                + "additionalContacts=["
                + "Struct{firstName=John,lastName=Doe}, "
                + "Struct{firstName=Jane,lastName=Doe,comment=A comment}"
                + "]}");
    // @formatter:on
  }

  @Test
  public void shouldCreateSchemaForEmptyArrayEncodingArray() throws Exception {
    final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

    final MongoDataConverter arrayConverter = new MongoDataConverter(ArrayEncoding.ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      arrayConverter.addFieldSchema(entry, builder);
    }
    final Schema arraySchema = builder.build();

    assertThat(arraySchema)
        .isEqualTo(
            SchemaBuilder.struct()
                .name("array")
                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                .field("f", SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).optional().build())
                .build());
  }

  @Test
  public void shouldCreateStructForEmptyArrayEncodingArray() throws Exception {
    final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

    final MongoDataConverter arrayConverter = new MongoDataConverter(ArrayEncoding.ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      arrayConverter.addFieldSchema(entry, builder);
    }
    final Schema arraySchema = builder.build();

    final Struct struct = new Struct(arraySchema);

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      arrayConverter.convertRecord(entry, arraySchema, struct);
    }

    // @formatter:off
    assertThat(struct.toString()).isEqualTo("Struct{" + "_id=1," + "f=[]" + "}");
    // @formatter:on
  }

  @Test
  public void shouldCreateSchemaForEmptyArrayEncodingDocument() throws Exception {
    final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

    final MongoDataConverter documentConverter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      documentConverter.addFieldSchema(entry, builder);
    }
    final Schema documentSchema = builder.build();

    assertThat(documentSchema)
        .isEqualTo(
            SchemaBuilder.struct()
                .name("array")
                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                .field("f", SchemaBuilder.struct().name("array.f").optional().build())
                .build());
  }

  @Test
  public void shouldCreateStructForEmptyArrayEncodingDocument() throws Exception {
    final BsonDocument val = BsonDocument.parse(EMPTY_ARRAY);

    final MongoDataConverter documentConverter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      documentConverter.addFieldSchema(entry, builder);
    }
    final Schema documentSchema = builder.build();

    final Struct struct = new Struct(documentSchema);

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      documentConverter.convertRecord(entry, documentSchema, struct);
    }

    assertThat(struct.toString()).isEqualTo("Struct{" + "_id=1," + "f=Struct{}}");
  }

  @Test
  public void shouldCreateSchemaForHeterogenousArray() throws Exception {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, builder);
    }
    final Schema finalSchema = builder.build();

    assertThat(finalSchema)
        .isEqualTo(
            SchemaBuilder.struct()
                .name("array")
                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                .field(
                    "a2",
                    SchemaBuilder.struct()
                        .name("array.a2")
                        .optional()
                        .field("_0", Schema.OPTIONAL_INT32_SCHEMA)
                        .field("_1", Schema.OPTIONAL_STRING_SCHEMA)
                        .build())
                .build());
  }

  @Test
  public void shouldCreateStructForHeterogenousArray() throws Exception {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, builder);
    }
    final Schema finalSchema = builder.build();

    final Struct struct = new Struct(finalSchema);

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.convertRecord(entry, finalSchema, struct);
    }

    assertThat(struct.toString()).isEqualTo("Struct{" + "_id=1," + "a2=Struct{_0=11,_1=abc}}");
  }

  @Test
  public void shouldCreateSchemaForHeterogenousDocumentInArray() throws Exception {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, builder);
    }
    final Schema finalSchema = builder.build();

    assertThat(finalSchema)
        .isEqualTo(
            SchemaBuilder.struct()
                .name("array")
                .field("_id", Schema.OPTIONAL_INT32_SCHEMA)
                .field(
                    "a1",
                    SchemaBuilder.struct()
                        .name("array.a1")
                        .optional()
                        .field(
                            "_0",
                            SchemaBuilder.struct()
                                .name("array.a1._0")
                                .optional()
                                .field("a", Schema.OPTIONAL_INT32_SCHEMA)
                                .build())
                        .field(
                            "_1",
                            SchemaBuilder.struct()
                                .name("array.a1._1")
                                .optional()
                                .field("a", Schema.OPTIONAL_STRING_SCHEMA)
                                .build())
                        .build())
                .build());
  }

  @Test
  public void shouldCreateStructForHeterogenousDocumentInArray() throws Exception {
    final MongoDataConverter converter = new MongoDataConverter(ArrayEncoding.DOCUMENT);
    final BsonDocument val = BsonDocument.parse(HETEROGENOUS_DOCUMENT_IN_ARRAY);
    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.addFieldSchema(entry, builder);
    }
    final Schema finalSchema = builder.build();

    final Struct struct = new Struct(finalSchema);

    for (Entry<String, BsonValue> entry : val.entrySet()) {
      converter.convertRecord(entry, finalSchema, struct);
    }

    // @formatter:off
    assertThat(struct.toString())
        .isEqualTo(
            "Struct{" + "_id=1," + "a1=Struct{" + "_0=Struct{a=1}," + "_1=Struct{a=c}" + "}" + "}");
    // @formatter:on
  }
}
