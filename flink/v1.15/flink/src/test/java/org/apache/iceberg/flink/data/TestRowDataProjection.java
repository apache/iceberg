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
package org.apache.iceberg.flink.data;

import java.util.Iterator;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.junit.Assert;
import org.junit.Test;

public class TestRowDataProjection {

  @Test
  public void testFullProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    generateAndValidate(schema, schema);
  }

  @Test
  public void testReorderedFullProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Schema reordered =
        new Schema(
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(0, "id", Types.LongType.get()));

    generateAndValidate(schema, reordered);
  }

  @Test
  public void testBasicProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));
    Schema id = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));
    Schema data = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));
    generateAndValidate(schema, id);
    generateAndValidate(schema, data);
  }

  @Test
  public void testEmptyProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));
    generateAndValidate(schema, schema.select());
  }

  @Test
  public void testRename() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Schema renamed =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "renamed", Types.StringType.get()));
    generateAndValidate(schema, renamed);
  }

  @Test
  public void testNestedProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()))));

    // Project id only.
    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));
    generateAndValidate(schema, idOnly);

    // Project lat only.
    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));
    generateAndValidate(schema, latOnly);

    // Project long only.
    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));
    generateAndValidate(schema, longOnly);

    // Project location.
    Schema locationOnly = schema.select("location");
    generateAndValidate(schema, locationOnly);
  }

  @Test
  public void testPrimitiveTypeProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(2, "b", Types.BooleanType.get()),
            Types.NestedField.optional(3, "i", Types.IntegerType.get()),
            Types.NestedField.required(4, "l", Types.LongType.get()),
            Types.NestedField.optional(5, "f", Types.FloatType.get()),
            Types.NestedField.required(6, "d", Types.DoubleType.get()),
            Types.NestedField.optional(7, "date", Types.DateType.get()),
            Types.NestedField.optional(8, "time", Types.TimeType.get()),
            Types.NestedField.required(9, "ts", Types.TimestampType.withoutZone()),
            Types.NestedField.required(10, "ts_tz", Types.TimestampType.withZone()),
            Types.NestedField.required(11, "s", Types.StringType.get()),
            Types.NestedField.required(12, "fixed", Types.FixedType.ofLength(7)),
            Types.NestedField.optional(13, "bytes", Types.BinaryType.get()),
            Types.NestedField.required(14, "dec_9_0", Types.DecimalType.of(9, 0)),
            Types.NestedField.required(15, "dec_11_2", Types.DecimalType.of(11, 2)),
            Types.NestedField.required(
                16, "dec_38_10", Types.DecimalType.of(38, 10)) // maximum precision
            );

    generateAndValidate(schema, schema);
  }

  @Test
  public void testPrimitiveMapTypeProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                3,
                "map",
                Types.MapType.ofOptional(1, 2, Types.IntegerType.get(), Types.StringType.get())));

    // Project id only.
    Schema idOnly = schema.select("id");
    generateAndValidate(schema, idOnly);

    // Project map only.
    Schema mapOnly = schema.select("map");
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);
  }

  @Test
  public void testNestedMapTypeProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                7,
                "map",
                Types.MapType.ofOptional(
                    5,
                    6,
                    Types.StructType.of(
                        Types.NestedField.required(1, "key", Types.LongType.get()),
                        Types.NestedField.required(2, "keyData", Types.LongType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(3, "value", Types.LongType.get()),
                        Types.NestedField.required(4, "valueData", Types.LongType.get())))));

    // Project id only.
    Schema idOnly = schema.select("id");
    generateAndValidate(schema, idOnly);

    // Project map only.
    Schema mapOnly = schema.select("map");
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);

    // Project partial map key.
    Schema partialMapKey =
        new Schema(
            Types.NestedField.optional(
                7,
                "map",
                Types.MapType.ofOptional(
                    5,
                    6,
                    Types.StructType.of(Types.NestedField.required(1, "key", Types.LongType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(3, "value", Types.LongType.get()),
                        Types.NestedField.required(4, "valueData", Types.LongType.get())))));
    AssertHelpers.assertThrows(
        "Should not allow to project a partial map key with non-primitive type.",
        IllegalArgumentException.class,
        "Cannot project a partial map key or value",
        () -> generateAndValidate(schema, partialMapKey));

    // Project partial map key.
    Schema partialMapValue =
        new Schema(
            Types.NestedField.optional(
                7,
                "map",
                Types.MapType.ofOptional(
                    5,
                    6,
                    Types.StructType.of(
                        Types.NestedField.required(1, "key", Types.LongType.get()),
                        Types.NestedField.required(2, "keyData", Types.LongType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(3, "value", Types.LongType.get())))));
    AssertHelpers.assertThrows(
        "Should not allow to project a partial map value with non-primitive type.",
        IllegalArgumentException.class,
        "Cannot project a partial map key or value",
        () -> generateAndValidate(schema, partialMapValue));
  }

  @Test
  public void testPrimitiveListTypeProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                2, "list", Types.ListType.ofOptional(1, Types.StringType.get())));

    // Project id only.
    Schema idOnly = schema.select("id");
    generateAndValidate(schema, idOnly);

    // Project list only.
    Schema mapOnly = schema.select("list");
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);
  }

  @Test
  public void testNestedListTypeProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "list",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        Types.NestedField.required(1, "nestedListField1", Types.LongType.get()),
                        Types.NestedField.required(2, "nestedListField2", Types.LongType.get()),
                        Types.NestedField.required(3, "nestedListField3", Types.LongType.get())))));

    // Project id only.
    Schema idOnly = schema.select("id");
    generateAndValidate(schema, idOnly);

    // Project list only.
    Schema mapOnly = schema.select("list");
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);

    // Project partial list value.
    Schema partialList =
        new Schema(
            Types.NestedField.optional(
                5,
                "list",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        Types.NestedField.required(2, "nestedListField2", Types.LongType.get())))));
    AssertHelpers.assertThrows(
        "Should not allow to project a partial list element with non-primitive type.",
        IllegalArgumentException.class,
        "Cannot project a partial list element",
        () -> generateAndValidate(schema, partialList));
  }

  private void generateAndValidate(Schema schema, Schema projectSchema) {
    int numRecords = 100;
    Iterable<Record> recordList = RandomGenericData.generate(schema, numRecords, 102L);
    Iterable<RowData> rowDataList = RandomRowData.generate(schema, numRecords, 102L);

    StructProjection structProjection = StructProjection.create(schema, projectSchema);
    RowDataProjection rowDataProjection = RowDataProjection.create(schema, projectSchema);

    Iterator<Record> recordIter = recordList.iterator();
    Iterator<RowData> rowDataIter = rowDataList.iterator();

    for (int i = 0; i < numRecords; i++) {
      Assert.assertTrue("Should have more records", recordIter.hasNext());
      Assert.assertTrue("Should have more RowData", rowDataIter.hasNext());

      StructLike expected = structProjection.wrap(recordIter.next());
      RowData actual = rowDataProjection.wrap(rowDataIter.next());

      TestHelpers.assertRowData(projectSchema, expected, actual);
    }

    Assert.assertFalse("Shouldn't have more record", recordIter.hasNext());
    Assert.assertFalse("Shouldn't have more RowData", rowDataIter.hasNext());
  }
}
