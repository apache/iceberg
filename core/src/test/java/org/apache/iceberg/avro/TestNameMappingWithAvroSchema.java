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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.avro.Schema;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class TestNameMappingWithAvroSchema {
  @Test
  public void testNameMappingWithAvroSchema() {

    // Create an example Avro schema with a nested record but not using the SchemaBuilder
    Schema schema =
        Schema.createRecord(
            "test",
            null,
            null,
            false,
            Lists.newArrayList(
                new Schema.Field("id", Schema.create(Schema.Type.INT)),
                new Schema.Field("data", Schema.create(Schema.Type.STRING)),
                new Schema.Field(
                    "location",
                    Schema.createRecord(
                        "location",
                        null,
                        null,
                        false,
                        Lists.newArrayList(
                            new Schema.Field("lat", Schema.create(Schema.Type.DOUBLE)),
                            new Schema.Field("long", Schema.create(Schema.Type.DOUBLE))))),
                new Schema.Field("friends", Schema.createArray(Schema.create(Schema.Type.STRING))),
                new Schema.Field(
                    "simpleUnion",
                    Schema.createUnion(
                        Lists.newArrayList(
                            Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING)))),
                new Schema.Field(
                    "complexUnion",
                    Schema.createUnion(
                        Schema.create(Schema.Type.NULL),
                        Schema.create(Schema.Type.STRING),
                        Schema.createRecord(
                            "innerRecord1",
                            null,
                            "namespace1",
                            false,
                            Lists.newArrayList(
                                new Schema.Field("lat", Schema.create(Schema.Type.DOUBLE)),
                                new Schema.Field("long", Schema.create(Schema.Type.DOUBLE)))),
                        Schema.createRecord(
                            "innerRecord2",
                            null,
                            "namespace2",
                            false,
                            Lists.newArrayList(
                                new Schema.Field("lat", Schema.create(Schema.Type.DOUBLE)),
                                new Schema.Field("long", Schema.create(Schema.Type.DOUBLE)))),
                        Schema.createRecord(
                            "innerRecord3",
                            null,
                            "namespace3",
                            false,
                            Lists.newArrayList(
                                new Schema.Field(
                                    "innerUnion",
                                    Schema.createUnion(
                                        Lists.newArrayList(
                                            Schema.create(Schema.Type.STRING),
                                            Schema.create(Schema.Type.INT)))))),
                        Schema.createEnum(
                            "timezone", null, null, Lists.newArrayList("UTC", "PST", "EST")),
                        Schema.createFixed("bitmap", null, null, 1)))));

    NameMappingWithAvroSchema nameMappingWithAvroSchema = new NameMappingWithAvroSchema();

    // Convert Avro schema to Iceberg schema
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(schema);
    MappedFields expected =
        MappedFields.of(
            MappedField.of(1, "id"),
            MappedField.of(2, "data"),
            MappedField.of(
                3,
                "location",
                MappedFields.of(MappedField.of(7, "lat"), MappedField.of(8, "long"))),
            MappedField.of(4, "friends", MappedFields.of(MappedField.of(9, "element"))),
            MappedField.of(5, "simpleUnion"),
            MappedField.of(
                6,
                "complexUnion",
                MappedFields.of(
                    MappedField.of(18, "string"),
                    MappedField.of(
                        19,
                        "innerRecord1",
                        MappedFields.of(MappedField.of(10, "lat"), MappedField.of(11, "long"))),
                    MappedField.of(
                        20,
                        "innerRecord2",
                        MappedFields.of(MappedField.of(12, "lat"), MappedField.of(13, "long"))),
                    MappedField.of(
                        21,
                        "innerRecord3",
                        MappedFields.of(
                            MappedField.of(
                                17,
                                "innerUnion",
                                MappedFields.of(
                                    MappedField.of(14, "string"), MappedField.of(15, "int"))))),
                    MappedField.of(22, "timezone"),
                    MappedField.of(23, "bitmap"))));
    assertThat(
            AvroWithPartnerByStructureVisitor.visit(
                icebergSchema.asStruct(), schema, nameMappingWithAvroSchema))
        .isEqualTo(expected);
  }
}
