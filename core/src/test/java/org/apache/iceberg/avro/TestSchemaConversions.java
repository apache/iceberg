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

import java.util.List;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.avro.AvroTestHelpers.addElementId;
import static org.apache.iceberg.avro.AvroTestHelpers.addKeyId;
import static org.apache.iceberg.avro.AvroTestHelpers.addValueId;
import static org.apache.iceberg.avro.AvroTestHelpers.optionalField;
import static org.apache.iceberg.avro.AvroTestHelpers.record;
import static org.apache.iceberg.avro.AvroTestHelpers.requiredField;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSchemaConversions {
  @Test
  public void testPrimitiveTypes() {
    List<Type> primitives = Lists.newArrayList(
        Types.BooleanType.get(),
        Types.IntegerType.get(),
        Types.LongType.get(),
        Types.FloatType.get(),
        Types.DoubleType.get(),
        Types.DateType.get(),
        Types.TimeType.get(),
        Types.TimestampType.withZone(),
        Types.TimestampType.withoutZone(),
        Types.StringType.get(),
        Types.UUIDType.get(),
        Types.FixedType.ofLength(12),
        Types.BinaryType.get(),
        Types.DecimalType.of(9, 4)
    );

    List<Schema> avroPrimitives = Lists.newArrayList(
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE),
        LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)),
        LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
        addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), true),
        addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), false),
        Schema.create(Schema.Type.STRING),
        LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16)),
        Schema.createFixed("fixed_12", null, null, 12),
        Schema.create(Schema.Type.BYTES),
        LogicalTypes.decimal(9, 4).addToSchema(Schema.createFixed("decimal_9_4", null, null, 4))
    );

    for (int i = 0; i < primitives.size(); i += 1) {
      Type type = primitives.get(i);
      Schema avro = avroPrimitives.get(i);
      Assert.assertEquals("Avro schema to primitive: " + avro,
          type, AvroSchemaUtil.convert(avro));
      Assert.assertEquals("Primitive to avro schema: " + type,
          avro, AvroSchemaUtil.convert(type));
    }
  }

  private Schema addAdjustToUtc(Schema schema, boolean adjustToUTC) {
    schema.addProp(AvroSchemaUtil.ADJUST_TO_UTC_PROP, adjustToUTC);
    return schema;
  }

  @Test
  public void testStructAndPrimitiveTypes() {
    Types.StructType struct = Types.StructType.of(
        optional(20, "bool", Types.BooleanType.get()),
        optional(21, "int", Types.IntegerType.get()),
        optional(22, "long", Types.LongType.get()),
        optional(23, "float", Types.FloatType.get()),
        optional(24, "double", Types.DoubleType.get()),
        optional(25, "date", Types.DateType.get()),
        optional(27, "time", Types.TimeType.get()),
        optional(28, "timestamptz", Types.TimestampType.withZone()),
        optional(29, "timestamp", Types.TimestampType.withoutZone()),
        optional(30, "string", Types.StringType.get()),
        optional(31, "uuid", Types.UUIDType.get()),
        optional(32, "fixed", Types.FixedType.ofLength(16)),
        optional(33, "binary", Types.BinaryType.get()),
        optional(34, "decimal", Types.DecimalType.of(14, 2))
    );

    Schema schema = record("primitives",
        optionalField(20, "bool", Schema.create(Schema.Type.BOOLEAN)),
        optionalField(21, "int", Schema.create(Schema.Type.INT)),
        optionalField(22, "long", Schema.create(Schema.Type.LONG)),
        optionalField(23, "float", Schema.create(Schema.Type.FLOAT)),
        optionalField(24, "double", Schema.create(Schema.Type.DOUBLE)),
        optionalField(25, "date", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
        optionalField(27, "time", LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))),
        optionalField(
            28,
            "timestamptz",
            addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), true)),
        optionalField(
            29,
            "timestamp",
            addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), false)),
        optionalField(30, "string", Schema.create(Schema.Type.STRING)),
        optionalField(31, "uuid", LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16))),
        optionalField(32, "fixed", Schema.createFixed("fixed_16", null, null, 16)),
        optionalField(33, "binary", Schema.create(Schema.Type.BYTES)),
        optionalField(
            34,
            "decimal",
            LogicalTypes.decimal(14, 2).addToSchema(Schema.createFixed("decimal_14_2", null, null, 6)))
    );

    Assert.assertEquals("Test conversion from Avro schema",
        struct, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Test conversion to Avro schema",
        schema, AvroSchemaUtil.convert(struct, "primitives"));
  }

  @Test
  public void testList() {
    Type list = Types.ListType.ofRequired(34, Types.UUIDType.get());
    Schema schema = addElementId(34, SchemaBuilder.array().items(
        LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16))));

    Assert.assertEquals("Avro schema to list",
        list, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("List to Avro schema",
        schema, AvroSchemaUtil.convert(list));
  }

  @Test
  public void testListOfStructs() {
    Type list = Types.ListType.ofRequired(34, Types.StructType.of(
        required(35, "lat", Types.FloatType.get()),
        required(36, "long", Types.FloatType.get())
    ));

    Schema schema = addElementId(34, SchemaBuilder.array().items(
        record("r34",
            requiredField(35, "lat", Schema.create(Schema.Type.FLOAT)),
            requiredField(36, "long", Schema.create(Schema.Type.FLOAT)))
    ));

    Assert.assertEquals("Avro schema to list",
        list, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("List to Avro schema",
        schema, AvroSchemaUtil.convert(list));
  }

  @Test
  public void testMapOfLongToBytes() {
    Type map = Types.MapType.ofRequired(33, 34, Types.LongType.get(), Types.BinaryType.get());
    Schema schema = AvroSchemaUtil.createMap(
        33, Schema.create(Schema.Type.LONG),
        34, Schema.create(Schema.Type.BYTES));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

  @Test
  public void testMapOfStringToBytes() {
    Type map = Types.MapType.ofRequired(33, 34, Types.StringType.get(), Types.BinaryType.get());
    Schema schema = addKeyId(33, addValueId(34, SchemaBuilder.map().values(
        Schema.create(Schema.Type.BYTES))));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

  @Test
  public void testMapOfListToStructs() {
    Type map = Types.MapType.ofRequired(33, 34,
        Types.ListType.ofRequired(35, Types.IntegerType.get()),
        Types.StructType.of(
            required(36, "a", Types.IntegerType.get()),
            optional(37, "b", Types.IntegerType.get())
        ));
    Schema schema = AvroSchemaUtil.createMap(
        33, addElementId(35, Schema.createArray(Schema.create(Schema.Type.INT))),
        34, record("r34",
            requiredField(36, "a", Schema.create(Schema.Type.INT)),
            optionalField(37, "b", Schema.create(Schema.Type.INT))));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

  @Test
  public void testMapOfStringToStructs() {
    Type map = Types.MapType.ofRequired(33, 34, Types.StringType.get(), Types.StructType.of(
        required(35, "a", Types.IntegerType.get()),
        optional(36, "b", Types.IntegerType.get())
    ));
    Schema schema = addKeyId(33, addValueId(34, SchemaBuilder.map().values(
        record("r34",
            requiredField(35, "a", Schema.create(Schema.Type.INT)),
            optionalField(36, "b", Schema.create(Schema.Type.INT))))));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

  @Test
  public void testComplexSchema() {
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(
            3,
            "preferences",
            Types.StructType
                .of(required(
                    8,
                    "feature1",
                    Types.BooleanType.get()), optional(9, "feature2", Types.BooleanType.get()))),
        required(
            4,
            "locations",
            Types.MapType.ofRequired(
                10,
                11,
                Types.StructType.of(
                    required(20, "address", Types.StringType.get()),
                    required(21, "city", Types.StringType.get()),
                    required(22, "state", Types.StringType.get()),
                    required(23, "zip", Types.IntegerType.get())
                ),
                Types.StructType.of(required(
                    12, "lat", Types.FloatType.get()), required(13, "long", Types.FloatType.get()))
            )
        ),
        optional(
            5,
            "points",
            Types.ListType.ofOptional(
                14,
                Types.StructType.of(required(
                    15, "x", Types.LongType.get()), required(16, "y", Types.LongType.get())))),
        required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
        optional(7, "properties", Types.MapType.ofOptional(
            18, 19, Types.StringType.get(), Types.StringType.get())));

    AvroSchemaUtil.convert(schema, "newTableName").toString(true);
  }

  @Test
  public void testSpecialChars() {
    List<String> names = Lists.newArrayList("9x", "x_", "a.b", "☃", "a#b");
    org.apache.iceberg.Schema schema = new org.apache.iceberg.Schema(
        required(1, names.get(0), Types.IntegerType.get()),
        required(2, names.get(1), Types.StringType.get()),
        required(3, names.get(2), Types.IntegerType.get()),
        required(4, names.get(3), Types.IntegerType.get()),
        required(5, names.get(4), Types.IntegerType.get()));

    Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());
    List<String> sanitizedNames = Lists.newArrayList(Iterables.transform(avroSchema.getFields(), Schema.Field::name));
    List<String> expectedSanitizedNames = Lists.newArrayList("_9x", "x_", "a_x2Eb", "_x2603", "a_x23b");
    Assert.assertEquals(expectedSanitizedNames, sanitizedNames);

    List<String> origNames = Lists.newArrayList(
        Iterables.transform(avroSchema.getFields(), f -> f.getProp(AvroSchemaUtil.ICEBERG_FIELD_NAME_PROP)));
    List<String> expectedOrigNames = Lists.newArrayList(names);
    expectedOrigNames.set(1, null);  // Name at pos 1 is valid so ICEBERG_FIELD_NAME_PROP is not set
    Assert.assertEquals(expectedOrigNames, origNames);
  }
}
