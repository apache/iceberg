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

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData.Record;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record record) throws IOException;

  @TempDir protected Path temp;

  @Test
  public void testFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Record projected = writeAndRead("full_projection", schema, schema, record);

    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");

    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("data"));
    Assertions.assertTrue(cmp == 0, "Should contain the correct data value");
  }

  @Test
  public void testReorderedFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema reordered =
        new Schema(
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    Assertions.assertEquals(
        "test", projected.get(0).toString(), "Should contain the correct 0 value");
    Assertions.assertEquals(34L, projected.get(1), "Should contain the correct 1 value");
  }

  @Test
  public void testReorderedProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema reordered =
        new Schema(
            Types.NestedField.optional(2, "missing_1", Types.StringType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "missing_2", Types.LongType.get()));

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    Assertions.assertNull(projected.get(0), "Should contain the correct 0 value");
    Assertions.assertEquals(
        "test", projected.get(1).toString(), "Should contain the correct 1 value");
    Assertions.assertNull(projected.get(2), "Should contain the correct 2 value");
  }

  @Test
  public void testEmptyProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Record projected = writeAndRead("empty_projection", schema, schema.select(), record);

    Assertions.assertNotNull(projected, "Should read a non-null record");
    // this is expected because there are no values
    Assertions.assertThrows(ArrayIndexOutOfBoundsException.class, () -> projected.get(0));
  }

  @Test
  public void testBasicProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("basic_projection_id", writeSchema, idOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "data");
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    TestHelpers.assertEmptyAvroField(projected, "id");
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("data"));
    Assertions.assertEquals(0, cmp, "Should contain the correct data value");
  }

  @Test
  public void testRename() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema readSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "renamed", Types.StringType.get()));

    Record projected = writeAndRead("project_and_rename", writeSchema, readSchema, record);

    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("renamed"));
    Assertions.assertTrue(cmp == 0, "Should contain the correct data/renamed value");
  }

  @Test
  public void testNestedStructProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location =
        new Record(AvroSchemaUtil.fromOption(record.getSchema().getField("location").schema()));
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "location");
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    Record projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("location"), "Should project location");
    TestHelpers.assertEmptyAvroField(projectedLocation, "long");
    Assertions.assertEquals(
        52.995143f, (float) projectedLocation.get("lat"), 0.000001f, "Should project latitude");

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("location"), "Should project location");
    TestHelpers.assertEmptyAvroField(projectedLocation, "lat");
    Assertions.assertEquals(
        -1.539054f, (float) projectedLocation.get("long"), 0.000001f, "Should project longitude");

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("location"), "Should project location");
    Assertions.assertEquals(
        52.995143f, (float) projectedLocation.get("lat"), 0.000001f, "Should project latitude");
    Assertions.assertEquals(
        -1.539054f, (float) projectedLocation.get("long"), 0.000001f, "Should project longitude");
  }

  @Test
  public void testMapProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                5,
                "properties",
                Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StringType.get())));

    Map<String, String> properties = ImmutableMap.of("a", "A", "b", "B");

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("properties", properties);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");
    TestHelpers.assertEmptyAvroField(projected, "properties");

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(
        properties, toStringMap((Map) projected.get("properties")), "Should project entire map");

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(
        properties, toStringMap((Map) projected.get("properties")), "Should project entire map");

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(
        properties, toStringMap((Map) projected.get("properties")), "Should project entire map");
  }

  private Map<String, ?> toStringMap(Map<?, ?> map) {
    Map<String, Object> stringMap = Maps.newHashMap();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      if (entry.getValue() instanceof CharSequence) {
        stringMap.put(entry.getKey().toString(), entry.getValue().toString());
      } else {
        stringMap.put(entry.getKey().toString(), entry.getValue());
      }
    }
    return stringMap;
  }

  @Test
  public void testMapOfStructsProjection() throws IOException {
    Schema writeSchema =
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

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record l1 =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("locations").schema())
                    .getValueType()));
    l1.put("lat", 53.992811f);
    l1.put("long", -1.542616f);
    Record l2 = new Record(l1.getSchema());
    l2.put("lat", 52.995143f);
    l2.put("long", -1.539054f);
    record.put("locations", ImmutableMap.of("L1", l1, "L2", l2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");
    TestHelpers.assertEmptyAvroField(projected, "locations");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(
        record.get("locations"),
        toStringMap((Map) projected.get("locations")),
        "Should project locations map");

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Map<String, ?> locations = toStringMap((Map) projected.get("locations"));
    Assertions.assertNotNull(locations, "Should project locations map");
    Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    Record projectedL1 = (Record) locations.get("L1");
    Assertions.assertNotNull(projectedL1, "L1 should not be null");
    Assertions.assertEquals(
        53.992811f, (float) projectedL1.get("lat"), 0.000001, "L1 should contain lat");
    TestHelpers.assertEmptyAvroField(projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    Assertions.assertNotNull(projectedL2, "L2 should not be null");
    Assertions.assertEquals(
        52.995143f, (float) projectedL2.get("lat"), 0.000001, "L2 should contain lat");
    TestHelpers.assertEmptyAvroField(projectedL2, "long");

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    Assertions.assertNotNull(locations, "Should project locations map");
    Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    projectedL1 = (Record) locations.get("L1");
    Assertions.assertNotNull(projectedL1, "L1 should not be null");
    TestHelpers.assertEmptyAvroField(projectedL1, "lat");
    Assertions.assertEquals(
        -1.542616f, (float) projectedL1.get("long"), 0.000001, "L1 should contain long");
    projectedL2 = (Record) locations.get("L2");
    Assertions.assertNotNull(projectedL2, "L2 should not be null");
    TestHelpers.assertEmptyAvroField(projectedL2, "lat");
    Assertions.assertEquals(
        -1.539054f, (float) projectedL2.get("long"), 0.000001, "L2 should contain long");

    Schema latitiudeRenamed =
        new Schema(
            Types.NestedField.optional(
                5,
                "locations",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(1, "latitude", Types.FloatType.get())))));

    projected = writeAndRead("latitude_renamed", writeSchema, latitiudeRenamed, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    Assertions.assertNotNull(locations, "Should project locations map");
    Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    projectedL1 = (Record) locations.get("L1");
    Assertions.assertNotNull(projectedL1, "L1 should not be null");
    Assertions.assertEquals(
        53.992811f, (float) projectedL1.get("latitude"), 0.000001, "L1 should contain latitude");
    TestHelpers.assertEmptyAvroField(projectedL1, "lat");
    TestHelpers.assertEmptyAvroField(projectedL1, "long");
    projectedL2 = (Record) locations.get("L2");
    Assertions.assertNotNull(projectedL2, "L2 should not be null");
    Assertions.assertEquals(
        52.995143f, (float) projectedL2.get("latitude"), 0.000001, "L2 should contain latitude");
    TestHelpers.assertEmptyAvroField(projectedL2, "lat");
    TestHelpers.assertEmptyAvroField(projectedL2, "long");
  }

  @Test
  public void testListProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                10, "values", Types.ListType.ofOptional(11, Types.LongType.get())));

    List<Long> values = ImmutableList.of(56L, 57L, 58L);

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("values", values);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");
    TestHelpers.assertEmptyAvroField(projected, "values");

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(values, projected.get("values"), "Should project entire list");

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(values, projected.get("values"), "Should project entire list");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListOfStructsProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.required(19, "x", Types.IntegerType.get()),
                        Types.NestedField.optional(18, "y", Types.IntegerType.get())))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record p1 =
        new Record(
            AvroSchemaUtil.fromOption(
                AvroSchemaUtil.fromOption(record.getSchema().getField("points").schema())
                    .getElementType()));
    p1.put("x", 1);
    p1.put("y", 2);
    Record p2 = new Record(p1.getSchema());
    p2.put("x", 3);
    p2.put("y", null);
    record.put("points", ImmutableList.of(p1, p2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assertions.assertEquals(34L, (long) projected.get("id"), "Should contain the correct id value");
    TestHelpers.assertEmptyAvroField(projected, "points");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertEquals(
        record.get("points"), projected.get("points"), "Should project points list");

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("points"), "Should project points list");
    List<Record> points = (List<Record>) projected.get("points");
    Assertions.assertEquals(2, points.size(), "Should read 2 points");
    Record projectedP1 = points.get(0);
    Assertions.assertEquals(1, (int) projectedP1.get("x"), "Should project x");
    TestHelpers.assertEmptyAvroField(projectedP1, "y");
    Record projectedP2 = points.get(1);
    Assertions.assertEquals(3, (int) projectedP2.get("x"), "Should project x");
    TestHelpers.assertEmptyAvroField(projectedP2, "y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("points"), "Should project points list");
    points = (List<Record>) projected.get("points");
    Assertions.assertEquals(2, points.size(), "Should read 2 points");
    projectedP1 = points.get(0);
    TestHelpers.assertEmptyAvroField(projectedP1, "x");
    Assertions.assertEquals(2, (int) projectedP1.get("y"), "Should project y");
    projectedP2 = points.get(1);
    TestHelpers.assertEmptyAvroField(projectedP2, "x");
    Assertions.assertNull(projectedP2.get("y"), "Should project null y");

    Schema yRenamed =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.optional(18, "z", Types.IntegerType.get())))));

    projected = writeAndRead("y_renamed", writeSchema, yRenamed, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Assertions.assertNotNull(projected.get("points"), "Should project points list");
    points = (List<Record>) projected.get("points");
    Assertions.assertEquals(2, points.size(), "Should read 2 points");
    projectedP1 = points.get(0);
    TestHelpers.assertEmptyAvroField(projectedP1, "x");
    TestHelpers.assertEmptyAvroField(projectedP1, "y");
    Assertions.assertEquals(2, (int) projectedP1.get("z"), "Should project z");
    projectedP2 = points.get(1);
    TestHelpers.assertEmptyAvroField(projectedP2, "x");
    TestHelpers.assertEmptyAvroField(projectedP2, "y");
    Assertions.assertNull(projectedP2.get("z"), "Should project null z");
  }
}
