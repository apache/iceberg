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

package org.apache.iceberg;

import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaEquate {

  @Test
  public void testUpdateSchemaEquatesNewStructField() {
    Schema persisted = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()),
        Types.NestedField.optional(101, "partition", Types.DoubleType.get()));

    Schema inbound = new Schema(
        Types.NestedField.required(1, "_id", Types.LongType.get()),
        Types.NestedField.optional(2, "location", Types.StructType.of(
            Types.NestedField.required(3, "lat", Types.FloatType.get()),
            Types.NestedField.required(4, "long", Types.FloatType.get()),
            Types.NestedField.optional(5, "projection", Types.StructType.of(
                Types.NestedField.required(6, "mercator", Types.FloatType.get())
            ))
        )),
        Types.NestedField.required(7, "tz", Types.TimestampType.withoutZone()));

    Schema applied = new SchemaUpdate(persisted, 101).equateNewFields(inbound).apply();

    Schema expected = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()),
        Types.NestedField.optional(101, "partition", Types.DoubleType.get()),
        Types.NestedField.optional(102, "location", Types.StructType.of(
            Types.NestedField.optional(103, "lat", Types.FloatType.get()),
            Types.NestedField.optional(104, "long", Types.FloatType.get()),
            Types.NestedField.optional(105, "projection", Types.StructType.of(
                Types.NestedField.optional(106, "mercator", Types.FloatType.get())
            ))
        )),
        Types.NestedField.optional(107, "tz", Types.TimestampType.withoutZone())
    );

    Assert.assertEquals("Should add new fields and reassign column IDs",
        expected.asStruct(), applied.asStruct());
  }

  @Test
  public void testUpdateSchemaEquatesNewPrimitivesTypes() {
    Schema persisted = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()));

    Schema inbound = new Schema(
        Types.NestedField.required(1, "integer", Types.IntegerType.get()),
        Types.NestedField.required(2, "long", Types.LongType.get()),
        Types.NestedField.required(3, "decimal", Types.DecimalType.of(10, 10)),
        Types.NestedField.required(4, "double", Types.DoubleType.get()),
        Types.NestedField.required(5, "float", Types.FloatType.get()),
        Types.NestedField.required(6, "fixed", Types.FixedType.ofLength(10)),
        Types.NestedField.required(7, "date", Types.DateType.get()),
        Types.NestedField.required(8, "binary", Types.BinaryType.get()),
        Types.NestedField.required(9, "boolean", Types.BooleanType.get()),
        Types.NestedField.required(10, "string", Types.StringType.get()),
        Types.NestedField.required(11, "uuid", Types.UUIDType.get()),
        Types.NestedField.required(12, "time", Types.TimeType.get()),
        Types.NestedField.required(13, "tz", Types.TimestampType.withoutZone()),
        Types.NestedField.required(14, "t", Types.TimestampType.withZone()));

    Schema applied = new SchemaUpdate(persisted, 100).equateNewFields(inbound).apply();

    Schema expected = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()),
        Types.NestedField.optional(101, "integer", Types.IntegerType.get()),
        Types.NestedField.optional(102, "long", Types.LongType.get()),
        Types.NestedField.optional(103, "decimal", Types.DecimalType.of(10, 10)),
        Types.NestedField.optional(104, "double", Types.DoubleType.get()),
        Types.NestedField.optional(105, "float", Types.FloatType.get()),
        Types.NestedField.optional(106, "fixed", Types.FixedType.ofLength(10)),
        Types.NestedField.optional(107, "date", Types.DateType.get()),
        Types.NestedField.optional(108, "binary", Types.BinaryType.get()),
        Types.NestedField.optional(109, "boolean", Types.BooleanType.get()),
        Types.NestedField.optional(110, "string", Types.StringType.get()),
        Types.NestedField.optional(111, "uuid", Types.UUIDType.get()),
        Types.NestedField.optional(112, "time", Types.TimeType.get()),
        Types.NestedField.optional(113, "tz", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(114, "t", Types.TimestampType.withZone()));

    Assert.assertEquals("Should add new fields and reassign column IDs",
        expected.asStruct(), applied.asStruct());
  }

  @Test
  public void testUpdateSchemaEquatesNewMapField() {
    Schema persisted = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()));

    Schema inbound = new Schema(
        Types.NestedField.required(1, "map", Types.MapType.ofRequired(
            12,
            13,
            Types.LongType.get(),
            Types.StructType.of(
                Types.NestedField.required(14, "lat", Types.FloatType.get()),
                Types.NestedField.optional(15, "long", Types.FloatType.get()),
                Types.NestedField.optional(16, "projection", Types.StructType.of(
                    Types.NestedField.optional(17, "mollweide", Types.FloatType.get())
                ))
            ))
        ));

    Schema applied = new SchemaUpdate(persisted, 100).equateNewFields(inbound).apply();

    Schema expected = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()),
        Types.NestedField.optional(101, "map", Types.MapType.ofRequired(
            102,
            103,
            Types.LongType.get(),
            Types.StructType.of(
                Types.NestedField.optional(104, "lat", Types.FloatType.get()),
                Types.NestedField.optional(105, "long", Types.FloatType.get()),
                Types.NestedField.optional(106, "projection", Types.StructType.of(
                    Types.NestedField.optional(107, "mollweide", Types.FloatType.get())
                ))
            ))));

    Assert.assertEquals("Should add new fields and reassign column IDs",
        expected.asStruct().toString(), applied.asStruct().toString());
  }

  @Test
  public void testUpdateSchemaEquatesNewListField() {
    Schema persisted = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()));

    Schema inbound = new Schema(
        Types.NestedField.required(1, "list", Types.ListType.ofRequired(
            12,
            Types.StructType.of(
                Types.NestedField.required(14, "lat", Types.FloatType.get()),
                Types.NestedField.optional(15, "long", Types.FloatType.get()),
                Types.NestedField.optional(16, "projection", Types.StructType.of(
                    Types.NestedField.optional(17, "mollweide", Types.FloatType.get())
                ))
            ))
        ));

    Schema applied = new SchemaUpdate(persisted, 100).equateNewFields(inbound).apply();

    Schema expected = new Schema(
        Types.NestedField.required(100, "_id", Types.LongType.get()),
        Types.NestedField.optional(101, "list", Types.ListType.ofRequired(
            102,
            Types.StructType.of(
                Types.NestedField.optional(103, "lat", Types.FloatType.get()),
                Types.NestedField.optional(104, "long", Types.FloatType.get()),
                Types.NestedField.optional(105, "projection", Types.StructType.of(
                    Types.NestedField.optional(106, "mollweide", Types.FloatType.get())
                ))
            ))));

    Assert.assertEquals("Should add new fields and reassign column IDs",
        expected.asStruct().toString(), applied.asStruct().toString());
  }
}
