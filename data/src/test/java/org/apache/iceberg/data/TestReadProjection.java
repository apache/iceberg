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
package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record record) throws IOException;

  // @Rule public TemporaryFolder temp = new TemporaryFolder();

  @TempDir public File temp;

  @Test
  public void testFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");

    Record projected = writeAndRead("full_projection", schema, schema, record);

    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");

    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data"));
    org.junit.jupiter.api.Assertions.assertTrue(cmp == 0, "Should contain the correct data value");
  }

  @Test
  public void testSpecialCharacterProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "user id", Types.LongType.get()),
            Types.NestedField.optional(1, "data%0", Types.StringType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("user id", 34L);
    record.setField("data%0", "test");

    Record full = writeAndRead("special_chars", schema, schema, record);

    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) full.getField("user id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertEquals(
        0,
        Comparators.charSequences().compare("test", (CharSequence) full.getField("data%0")),
        "Should contain the correct data value");

    Record projected = writeAndRead("special_characters", schema, schema.select("data%0"), record);

    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("user id"), "Should not contain id value");
    org.junit.jupiter.api.Assertions.assertEquals(
        0,
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data%0")),
        "Should contain the correct data value");
  }

  @Test
  public void testReorderedFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");

    Schema reordered =
        new Schema(
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    org.junit.jupiter.api.Assertions.assertEquals(
        "Should contain the correct 0 value", "test", projected.get(0).toString());
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, projected.get(1), "Should contain the correct 1 value");
  }

  @Test
  public void testReorderedProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");

    Schema reordered =
        new Schema(
            Types.NestedField.optional(2, "missing_1", Types.StringType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "missing_2", Types.LongType.get()));

    Record projected = writeAndRead("full_projection", schema, reordered, record);

    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(0), "Should contain the correct 0 value");
    org.junit.jupiter.api.Assertions.assertEquals(
        "Should contain the correct 1 value", "test", projected.get(1).toString());
    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(2), "Should contain the correct 2 value");
  }

  @Test
  public void testRenamedAddedField() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.LongType.get()),
            Types.NestedField.required(2, "b", Types.LongType.get()),
            Types.NestedField.required(3, "d", Types.LongType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("a", 100L);
    record.setField("b", 200L);
    record.setField("d", 300L);

    Schema renamedAdded =
        new Schema(
            Types.NestedField.optional(1, "a", Types.LongType.get()),
            Types.NestedField.optional(2, "b", Types.LongType.get()),
            Types.NestedField.optional(3, "c", Types.LongType.get()),
            Types.NestedField.optional(4, "d", Types.LongType.get()));

    Record projected =
        writeAndRead("rename_and_add_column_projection", schema, renamedAdded, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.get(0), 100L, "Should contain the correct value in column 1");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.getField("a"), 100L, "Should contain the correct value in column a");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.get(1), 200L, "Should contain the correct value in column 2");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.getField("b"), 200L, "Should contain the correct value in column b");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.get(2), 300L, "Should contain the correct value in column 3");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.getField("c"), 300L, "Should contain the correct value in column c");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(3), "Should contain empty value on new column 4");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("d"), "Should contain the correct value in column d");
  }

  @Test
  public void testEmptyProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");

    Record projected = writeAndRead("empty_projection", schema, schema.select(), record);

    org.junit.jupiter.api.Assertions.assertNotNull(projected, "Should read a non-null record");
    // this is expected because there are no values
    Assertions.assertThatThrownBy(() -> projected.get(0))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class);
  }

  @Test
  public void testBasicProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(2, "time", Types.TimestampType.withZone()));

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");
    record.setField("time", OffsetDateTime.now(ZoneOffset.UTC));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("basic_projection_id", writeSchema, idOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("data"), "Should not project data");
    org.junit.jupiter.api.Assertions.assertEquals(34L, (long) projected.getField("id"));

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data"));
    org.junit.jupiter.api.Assertions.assertTrue(cmp == 0, "Should contain the correct data value");
  }

  @Test
  public void testRename() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    record.setField("data", "test");

    Schema readSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "renamed", Types.StringType.get()));

    Record projected = writeAndRead("project_and_rename", writeSchema, readSchema, record);

    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("renamed"));
    org.junit.jupiter.api.Assertions.assertTrue(
        cmp == 0, "Should contain the correct data/renamed value");
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

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    Record location = GenericRecord.create(writeSchema.findType("location").asStructType());
    location.setField("lat", 52.995143f);
    location.setField("long", -1.539054f);
    record.setField("location", location);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Record projectedLocation = (Record) projected.getField("location");
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertNull(projectedLocation, "Should not project location");

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    projectedLocation = (Record) projected.getField("location");
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("location"), "Should project location");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedLocation.getField("long"), "Should not project longitude");
    org.junit.jupiter.api.Assertions.assertEquals(
        52.995143f,
        (float) projectedLocation.getField("lat"),
        0.000001f,
        "Should project latitude");

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.getField("location");
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("location"), "Should project location");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedLocation.getField("lat"), "Should not project latitutde");
    org.junit.jupiter.api.Assertions.assertEquals(
        -1.539054f,
        (float) projectedLocation.getField("long"),
        0.000001f,
        "Should project longitude");

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.getField("location");
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("location"), "Should project location");
    org.junit.jupiter.api.Assertions.assertEquals(
        52.995143f,
        (float) projectedLocation.getField("lat"),
        0.000001f,
        "Should project latitude");
    org.junit.jupiter.api.Assertions.assertEquals(
        -1.539054f,
        (float) projectedLocation.getField("long"),
        0.000001f,
        "Should project longitude");
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

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    record.setField("properties", properties);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("properties"), "Should not project properties map");

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        properties,
        toStringMap((Map) projected.getField("properties")),
        "Should project entire map");

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        properties,
        toStringMap((Map) projected.getField("properties")),
        "Should project entire map");

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        properties,
        toStringMap((Map) projected.getField("properties")),
        "Should project entire map");
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

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    Record l1 =
        GenericRecord.create(
            writeSchema.findType("locations").asMapType().valueType().asStructType());
    l1.setField("lat", 53.992811f);
    l1.setField("long", -1.542616f);
    Record l2 =
        GenericRecord.create(
            writeSchema.findType("locations").asMapType().valueType().asStructType());
    l2.setField("lat", 52.995143f);
    l2.setField("long", -1.539054f);
    record.setField("locations", ImmutableMap.of("L1", l1, "L2", l2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("locations"), "Should not project locations map");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        record.getField("locations"),
        toStringMap((Map) projected.getField("locations")),
        "Should project locations map");

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    Map<String, ?> locations = toStringMap((Map) projected.getField("locations"));
    org.junit.jupiter.api.Assertions.assertNotNull(locations, "Should project locations map");
    org.junit.jupiter.api.Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    Record projectedL1 = (Record) locations.get("L1");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL1, "L1 should not be null");
    org.junit.jupiter.api.Assertions.assertEquals(
        53.992811f, (float) projectedL1.getField("lat"), 0.000001, "L1 should contain lat");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL1.getField("long"), "L1 should not contain long");
    Record projectedL2 = (Record) locations.get("L2");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL2, "L2 should not be null");
    org.junit.jupiter.api.Assertions.assertEquals(
        52.995143f, (float) projectedL2.getField("lat"), 0.000001, "L2 should contain lat");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL2.getField("long"), "L2 should not contain long");

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    locations = toStringMap((Map) projected.getField("locations"));
    org.junit.jupiter.api.Assertions.assertNotNull(locations, "Should project locations map");
    org.junit.jupiter.api.Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    projectedL1 = (Record) locations.get("L1");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL1, "L1 should not be null");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL1.getField("lat"), "L1 should not contain lat");
    org.junit.jupiter.api.Assertions.assertEquals(
        -1.542616f, (float) projectedL1.getField("long"), 0.000001, "L1 should contain long");
    projectedL2 = (Record) locations.get("L2");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL2, "L2 should not be null");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL2.getField("lat"), "L2 should not contain lat");
    org.junit.jupiter.api.Assertions.assertEquals(
        -1.539054f, (float) projectedL2.getField("long"), 0.000001, "L2 should contain long");

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
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    locations = toStringMap((Map) projected.getField("locations"));
    org.junit.jupiter.api.Assertions.assertNotNull(locations, "Should project locations map");
    org.junit.jupiter.api.Assertions.assertEquals(
        Sets.newHashSet("L1", "L2"), locations.keySet(), "Should contain L1 and L2");
    projectedL1 = (Record) locations.get("L1");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL1, "L1 should not be null");
    org.junit.jupiter.api.Assertions.assertEquals(
        53.992811f,
        (float) projectedL1.getField("latitude"),
        0.000001,
        "L1 should contain latitude");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL1.getField("lat"), "L1 should not contain lat");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL1.getField("long"), "L1 should not contain long");
    projectedL2 = (Record) locations.get("L2");
    org.junit.jupiter.api.Assertions.assertNotNull(projectedL2, "L2 should not be null");
    org.junit.jupiter.api.Assertions.assertEquals(
        52.995143f,
        (float) projectedL2.getField("latitude"),
        0.000001,
        "L2 should contain latitude");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL2.getField("lat"), "L2 should not contain lat");
    org.junit.jupiter.api.Assertions.assertNull(
        projectedL2.getField("long"), "L2 should not contain long");
  }

  @Test
  public void testListProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                10, "values", Types.ListType.ofOptional(11, Types.LongType.get())));

    List<Long> values = ImmutableList.of(56L, 57L, 58L);

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    record.setField("values", values);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("values"), "Should not project values list");

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        values, projected.getField("values"), "Should project entire list");

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        values, projected.getField("values"), "Should project entire list");
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

    Record record = GenericRecord.create(writeSchema.asStruct());
    record.setField("id", 34L);
    Record p1 =
        GenericRecord.create(
            writeSchema.findType("points").asListType().elementType().asStructType());
    p1.setField("x", 1);
    p1.setField("y", 2);
    Record p2 =
        GenericRecord.create(
            writeSchema.findType("points").asListType().elementType().asStructType());
    p2.setField("x", 3);
    p2.setField("y", null);
    record.setField("points", ImmutableList.of(p1, p2));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        34L, (long) projected.getField("id"), "Should contain the correct id value");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("points"), "Should not project points list");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertEquals(
        record.getField("points"), projected.getField("points"), "Should project points list");

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("points"), "Should project points list");
    List<Record> points = (List<Record>) projected.getField("points");
    org.junit.jupiter.api.Assertions.assertEquals(2, points.size(), "Should read 2 points");
    Record projectedP1 = points.get(0);
    org.junit.jupiter.api.Assertions.assertEquals(
        1, (int) projectedP1.getField("x"), "Should project x");
    org.junit.jupiter.api.Assertions.assertNull(projectedP1.getField("y"), "Should not project y");
    Record projectedP2 = points.get(1);
    org.junit.jupiter.api.Assertions.assertEquals(
        3, (int) projectedP2.getField("x"), "Should project x");
    org.junit.jupiter.api.Assertions.assertNull(projectedP2.getField("y"), "Should not project y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("points"), "Should project points list");
    points = (List<Record>) projected.getField("points");
    org.junit.jupiter.api.Assertions.assertEquals(2, points.size(), "Should read 2 points");
    projectedP1 = points.get(0);
    org.junit.jupiter.api.Assertions.assertNull(projectedP1.getField("x"), "Should not project x");
    org.junit.jupiter.api.Assertions.assertEquals(
        2, (int) projectedP1.getField("y"), "Should project y");
    projectedP2 = points.get(1);
    org.junit.jupiter.api.Assertions.assertNull(projectedP2.getField("x"), "Should not project x");
    org.junit.jupiter.api.Assertions.assertEquals(
        null, projectedP2.getField("y"), "Should project null y");

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
    org.junit.jupiter.api.Assertions.assertNull(projected.getField("id"), "Should not project id");
    org.junit.jupiter.api.Assertions.assertNotNull(
        projected.getField("points"), "Should project points list");
    points = (List<Record>) projected.getField("points");
    org.junit.jupiter.api.Assertions.assertEquals(2, points.size(), "Should read 2 points");
    projectedP1 = points.get(0);
    org.junit.jupiter.api.Assertions.assertNull(projectedP1.getField("x"), "Should not project x");
    org.junit.jupiter.api.Assertions.assertNull(projectedP1.getField("y"), "Should not project y");
    org.junit.jupiter.api.Assertions.assertEquals(
        2, (int) projectedP1.getField("z"), "Should project z");
    projectedP2 = points.get(1);
    org.junit.jupiter.api.Assertions.assertNull(projectedP2.getField("x"), "Should not project x");
    org.junit.jupiter.api.Assertions.assertNull(projectedP2.getField("y"), "Should not project y");
    org.junit.jupiter.api.Assertions.assertEquals(
        null, projectedP2.getField("z"), "Should project null z");
  }

  @Test
  public void testAddedFieldsWithRequiredChildren() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "a", Types.LongType.get()));

    Record record = GenericRecord.create(schema.asStruct());
    record.setField("a", 100L);

    Schema addedFields =
        new Schema(
            Types.NestedField.optional(1, "a", Types.LongType.get()),
            Types.NestedField.optional(
                2,
                "b",
                Types.StructType.of(Types.NestedField.required(3, "c", Types.LongType.get()))),
            Types.NestedField.optional(4, "d", Types.ListType.ofRequired(5, Types.LongType.get())),
            Types.NestedField.optional(
                6,
                "e",
                Types.MapType.ofRequired(7, 8, Types.LongType.get(), Types.LongType.get())));

    Record projected =
        writeAndRead("add_fields_with_required_children_projection", schema, addedFields, record);
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.get(0), 100L, "Should contain the correct value in column 1");
    org.junit.jupiter.api.Assertions.assertEquals(
        projected.getField("a"), 100L, "Should contain the correct value in column a");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(1), "Should contain empty value in new column 2");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("b"), "Should contain empty value in column b");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(2), "Should contain empty value in new column 4");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("d"), "Should contain empty value in column d");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.get(3), "Should contain empty value in new column 6");
    org.junit.jupiter.api.Assertions.assertNull(
        projected.getField("e"), "Should contain empty value in column e");
  }
}
