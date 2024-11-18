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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

public class TestSparkValueConverter {
  @Test
  public void testSparkNullMapConvert() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "locations",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "lat", Types.FloatType.get()),
                        Types.NestedField.required(2, "long", Types.FloatType.get())))));

    assertCorrectNullConversion(schema);
  }

  @Test
  public void testSparkNullListConvert() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5, "locations", Types.ListType.ofOptional(6, Types.StringType.get())));

    assertCorrectNullConversion(schema);
  }

  @Test
  public void testSparkNullStructConvert() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()))));

    assertCorrectNullConversion(schema);
  }

  @Test
  public void testSparkNullPrimitiveConvert() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(5, "location", Types.StringType.get()));
    assertCorrectNullConversion(schema);
  }

  private void assertCorrectNullConversion(Schema schema) {
    Row sparkRow = RowFactory.create(1, null);
    Record record = GenericRecord.create(schema);
    record.set(0, 1);
    assertThat(SparkValueConverter.convert(schema, sparkRow))
        .as("Round-trip conversion should produce original value")
        .isEqualTo(record);
  }
}
