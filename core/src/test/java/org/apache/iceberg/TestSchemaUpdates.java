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
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaUpdates {

  @Test
  public void testUpdates() {
    Schema schema = new Schema(
        NestedField.required(1, "id", Types.IntegerType.get()),
        NestedField.optional(2, "partition", Types.DoubleType.get()));

    Schema newSchema = new Schema(
        // type promotion with documentation
        NestedField.required(1, "id", Types.LongType.get(), "type promoted"),
        NestedField.optional(2, "partition", Types.DoubleType.get()),
        // struct is added, as an optional field
        NestedField.optional(3, "location", StructType.of(
            NestedField.required(4, "lat", Types.FloatType.get()))),
        // top level field added as optional
        NestedField.optional(5, "tz", Types.TimestampType.withoutZone()));

    Schema applied = new SchemaUpdate(schema, 2).updateSchema(newSchema).apply();
    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());

    // nested adds
    newSchema = new Schema(
        NestedField.required(1, "id", Types.IntegerType.get()),
        NestedField.optional(2, "partition", Types.DoubleType.get()),
        // struct is added, as an optional field
        NestedField.optional(3, "a", StructType.of(
            NestedField.required(4, "a_b", StructType.of(
                NestedField.required(5, "a_b_c", StructType.of(
                    NestedField.required(6, "a_b_c_d", Types.IntegerType.get()))))))));

    applied = new SchemaUpdate(schema, 2).updateSchema(newSchema).apply();
    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());
  }

  @Test
  public void testMapUpdates() {
    Schema schema = new Schema(
        NestedField.required(1, "id", Types.LongType.get()));

    Schema newSchema = new Schema(
        // map addition
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "location", Types.MapType.ofRequired(
            3, 4, Types.LongType.get(), StructType.of(
                NestedField.required(5, "lat", Types.FloatType.get())))));

    Schema applied = new SchemaUpdate(schema, 1).updateSchema(newSchema).apply();

    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());

    // complex maps
    schema = new Schema(
        NestedField.required(1, "locations", Types.MapType.ofOptional(2, 3,
            StructType.of(
                NestedField.required(4, "k1", Types.IntegerType.get())
            ),
            StructType.of(
                NestedField.required(5, "lat", Types.FloatType.get()),
                NestedField.required(6, "long", Types.FloatType.get())
            )
        )));

    newSchema = new Schema(
        NestedField.required(1, "locations", Types.MapType.ofOptional(2, 3,
            StructType.of(
                NestedField.required(4, "k1", Types.IntegerType.get())
            ),
            StructType.of(
                NestedField.required(5, "lat", Types.FloatType.get()),
                NestedField.required(6, "long", Types.DoubleType.get())
            )
        )));


    applied = new SchemaUpdate(schema, 6).updateSchema(newSchema).apply();
    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());
  }

  @Test
  public void testListUpdates() {
    Schema schema = new Schema(
        NestedField.required(1, "id", Types.LongType.get()));

    Schema newSchema = new Schema(
        // optional list field added
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "list", Types.ListType.ofRequired(
            3,
            StructType.of(
                NestedField.required(4, "lat", Types.FloatType.get()),
                NestedField.required(5, "long", Types.FloatType.get())))));

    Schema applied = new SchemaUpdate(schema, 1).updateSchema(newSchema).apply();
    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());


    schema = new Schema(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "list", Types.ListType.ofRequired(
            3,
            StructType.of(
                NestedField.required(4, "lat", Types.FloatType.get()),
                NestedField.required(5, "long", Types.FloatType.get())))));

    newSchema = new Schema(
        NestedField.required(1, "id", Types.LongType.get()),
        NestedField.optional(2, "list", Types.ListType.ofRequired(
            3,
            StructType.of(
                NestedField.required(4, "lat", Types.DoubleType.get()),
                NestedField.required(5, "long", Types.DoubleType.get())))));

    applied = new SchemaUpdate(schema, 5).updateSchema(newSchema).apply();
    Assert.assertEquals(newSchema.asStruct(), applied.asStruct());
  }
}

