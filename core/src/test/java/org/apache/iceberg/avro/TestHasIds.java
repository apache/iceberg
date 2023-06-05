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

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestHasIds {
  @Test
  public void test() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "location",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.optional(2, "long", Types.FloatType.get())))));

    org.apache.avro.Schema avroSchema = RemoveIds.removeIds(schema);
    assertThat(AvroSchemaUtil.hasIds(avroSchema)).as("Avro schema should not have IDs").isFalse();
    avroSchema.getFields().get(0).addProp("field-id", 1);
    assertThat(AvroSchemaUtil.hasIds(avroSchema)).as("Avro schema should have IDs").isTrue();

    // Create a fresh copy
    avroSchema = RemoveIds.removeIds(schema);
    avroSchema
        .getFields()
        .get(1)
        .schema()
        .getTypes()
        .get(1)
        .getValueType()
        .getTypes()
        .get(1)
        .getFields()
        .get(1)
        .addProp("field-id", 1);
    assertThat(AvroSchemaUtil.hasIds(avroSchema)).as("Avro schema should have IDs").isTrue();
  }
}
