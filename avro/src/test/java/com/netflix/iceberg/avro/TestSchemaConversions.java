/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.avro;

import com.google.common.collect.Lists;
import com.netflix.iceberg.types.Type;
import com.netflix.iceberg.types.Types;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;
import java.util.List;

import static com.netflix.iceberg.avro.AvroTestHelpers.addElementId;
import static com.netflix.iceberg.avro.AvroTestHelpers.addKeyId;
import static com.netflix.iceberg.avro.AvroTestHelpers.addValueId;
import static com.netflix.iceberg.avro.AvroTestHelpers.optionalField;
import static com.netflix.iceberg.avro.AvroTestHelpers.record;
import static com.netflix.iceberg.avro.AvroTestHelpers.requiredField;

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
        Types.TimeType.withZone(),
        Types.TimeType.withoutZone(),
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
        addAdjustToUtc(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), true),
        addAdjustToUtc(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), false),
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
        Types.NestedField.optional(20, "bool", Types.BooleanType.get()),
        Types.NestedField.optional(21, "int", Types.IntegerType.get()),
        Types.NestedField.optional(22, "long", Types.LongType.get()),
        Types.NestedField.optional(23, "float", Types.FloatType.get()),
        Types.NestedField.optional(24, "double", Types.DoubleType.get()),
        Types.NestedField.optional(25, "date", Types.DateType.get()),
        Types.NestedField.optional(26, "timetz", Types.TimeType.withZone()),
        Types.NestedField.optional(27, "time", Types.TimeType.withoutZone()),
        Types.NestedField.optional(28, "timestamptz", Types.TimestampType.withZone()),
        Types.NestedField.optional(29, "timestamp", Types.TimestampType.withoutZone()),
        Types.NestedField.optional(30, "string", Types.StringType.get()),
        Types.NestedField.optional(31, "uuid", Types.UUIDType.get()),
        Types.NestedField.optional(32, "fixed", Types.FixedType.ofLength(16)),
        Types.NestedField.optional(33, "binary", Types.BinaryType.get()),
        Types.NestedField.optional(34, "decimal", Types.DecimalType.of(14, 2))
    );

    Schema schema = record("primitives",
        optionalField(20, "bool", Schema.create(Schema.Type.BOOLEAN)),
        optionalField(21, "int", Schema.create(Schema.Type.INT)),
        optionalField(22, "long", Schema.create(Schema.Type.LONG)),
        optionalField(23, "float", Schema.create(Schema.Type.FLOAT)),
        optionalField(24, "double", Schema.create(Schema.Type.DOUBLE)),
        optionalField(25, "date", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))),
        optionalField(26, "timetz", addAdjustToUtc(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), true)),
        optionalField(27, "time", addAdjustToUtc(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)), false)),
        optionalField(28, "timestamptz", addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), true)),
        optionalField(29, "timestamp", addAdjustToUtc(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)), false)),
        optionalField(30, "string", Schema.create(Schema.Type.STRING)),
        optionalField(31, "uuid", LogicalTypes.uuid().addToSchema(Schema.createFixed("uuid_fixed", null, null, 16))),
        optionalField(32, "fixed", Schema.createFixed("fixed_16", null, null, 16)),
        optionalField(33, "binary", Schema.create(Schema.Type.BYTES)),
        optionalField(34, "decimal", LogicalTypes.decimal(14, 2).addToSchema(Schema.createFixed("decimal_14_2", null, null, 6)))
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
        Types.NestedField.required(35, "lat", Types.FloatType.get()),
        Types.NestedField.required(36, "long", Types.FloatType.get())
    ));

    Schema schema = addElementId(34, SchemaBuilder.array().items(
        record(null,
            requiredField(35, "lat", Schema.create(Schema.Type.FLOAT)),
            requiredField(36, "long", Schema.create(Schema.Type.FLOAT)))
    ));

    Assert.assertEquals("Avro schema to list",
        list, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("List to Avro schema",
        schema, AvroSchemaUtil.convert(list));
  }

  @Test
  public void testMap() {
    Type map = Types.MapType.ofRequired(33, 34, Types.BinaryType.get());
    Schema schema = addKeyId(33, addValueId(34, SchemaBuilder.map().values(
        Schema.create(Schema.Type.BYTES))));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

  @Test
  public void testMapOfStructs() {
    Type map = Types.MapType.ofRequired(33, 34, Types.StructType.of(
        Types.NestedField.required(35, "a", Types.IntegerType.get()),
        Types.NestedField.optional(36, "b", Types.IntegerType.get())
    ));
    Schema schema = addKeyId(33, addValueId(34, SchemaBuilder.map().values(
        record(null,
            requiredField(35, "a", Schema.create(Schema.Type.INT)),
            optionalField(36, "b", Schema.create(Schema.Type.INT))))));

    Assert.assertEquals("Avro schema to map",
        map, AvroSchemaUtil.convert(schema));
    Assert.assertEquals("Map to Avro schema",
        schema, AvroSchemaUtil.convert(map));
  }

}
