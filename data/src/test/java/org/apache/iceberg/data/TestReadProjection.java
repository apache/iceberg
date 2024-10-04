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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record record) throws IOException;

  @TempDir protected File tempDir;

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

    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);

    assertThat((CharSequence) projected.getField("data"))
        .as("Should contain the correct data value")
        .isEqualTo("test");
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

    assertThat((long) full.getField("user id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat((CharSequence) full.getField("data%0"))
        .as("Should contain the correct data value")
        .isEqualTo("test");

    Record projected = writeAndRead("special_characters", schema, schema.select("data%0"), record);

    assertThat(projected.getField("user id")).as("Should not contain id value").isNull();
    assertThat(((CharSequence) projected.getField("data%0")))
        .as("Should contain the correct data value")
        .isEqualTo("test");
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

    assertThat(projected.get(0).toString())
        .as("Should contain the correct 0 value")
        .isEqualTo("test");
    assertThat(projected.get(1)).as("Should contain the correct 1 value").isEqualTo(34L);
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

    assertThat(projected.get(0)).as("Should contain the correct 0 value").isNull();
    assertThat(projected.get(1).toString())
        .as("Should contain the correct 1 value")
        .isEqualTo("test");
    assertThat(projected.get(2)).as("Should contain the correct 2 value").isNull();
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
    assertThat(projected.get(0)).as("Should contain the correct value in column 1").isEqualTo(100L);
    assertThat(projected.getField("a"))
        .as("Should contain the correct value in column a")
        .isEqualTo(100L);
    assertThat(projected.get(1)).as("Should contain the correct value in column 2").isEqualTo(200L);
    assertThat(projected.getField("b"))
        .as("Should contain the correct value in column b")
        .isEqualTo(200L);
    assertThat(projected.get(2)).as("Should contain the correct value in column 3").isEqualTo(300L);
    assertThat(projected.getField("c"))
        .as("Should contain the correct value in column c")
        .isEqualTo(300L);
    assertThat(projected.get(3)).as("Should contain the correct value in column 4").isNull();
    assertThat(projected.getField("d")).as("Should contain the correct value in column d").isNull();
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

    assertThat(projected).as("Should read a non-null record").isNotNull();
    // this is expected because there are no values
    assertThatThrownBy(() -> projected.get(0)).isInstanceOf(ArrayIndexOutOfBoundsException.class);
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
    assertThat(projected.getField("data")).as("Should not project data").isNull();
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat((CharSequence) projected.getField("data"))
        .as("Should contain the correct data value")
        .isEqualTo("test");
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

    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat((CharSequence) projected.getField("renamed"))
        .as("Should contain the correct data/renamed value")
        .isEqualTo("test");
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
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat(projectedLocation).as("Should not project location").isNull();

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    projectedLocation = (Record) projected.getField("location");
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("location")).as("Should project location").isNotNull();
    assertThat(projected.getField("long")).as("Should not project longitude").isNull();
    assertThat((float) projectedLocation.getField("lat"))
        .as("Should project latitude")
        .isCloseTo(52.995143f, within(0.000001f));

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.getField("location");
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("location")).as("Should project location").isNotNull();
    assertThat(projectedLocation.getField("lat")).as("Should not project latitude").isNull();
    assertThat((float) projectedLocation.getField("long"))
        .as("Should project longitude")
        .isCloseTo(-1.539054f, within(0.000001f));

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.getField("location");
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("location")).as("Should project location").isNotNull();

    assertThat((float) projectedLocation.getField("lat"))
        .as("Should project latitude")
        .isCloseTo(52.995143f, within(0.000001f));
    assertThat((float) projectedLocation.getField("long"))
        .as("Should project longitude")
        .isCloseTo(-1.539054f, within(0.000001f));
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
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat(projected.getField("properties")).as("Should not project properties map").isNull();

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(toStringMap((Map) projected.getField("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(toStringMap((Map) projected.getField("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(toStringMap((Map) projected.getField("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);
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
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat(projected.getField("locations")).as("Should not project locations map").isNull();

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(toStringMap((Map) projected.getField("locations")))
        .as("Should project locations map")
        .isEqualTo(record.getField("locations"));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    Map<String, ?> locations = toStringMap((Map) projected.getField("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet())
        .as("Should contain L1 and L2")
        .containsExactlyInAnyOrder("L1", "L2");

    Record projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.getField("lat"))
        .as("L1 should contain lat")
        .isCloseTo(53.992811f, within(0.000001f));
    assertThat(projectedL1.getField("long")).as("L1 should not contain long").isNull();

    Record projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.getField("lat"))
        .as("L2 should contain lat")
        .isCloseTo(52.995143f, within(0.000001f));
    assertThat(projectedL2.getField("long")).as("L2 should not contain long").isNull();

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    locations = toStringMap((Map) projected.getField("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet())
        .as("Should contain L1 and L2")
        .containsExactlyInAnyOrder("L1", "L2");

    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat(projectedL1.getField("lat")).as("L1 should not contain lat").isNull();
    assertThat((float) projectedL1.getField("long"))
        .as("L1 should contain long")
        .isCloseTo(-1.542616f, within(0.000001f));

    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat(projectedL2.getField("lat")).as("L2 should not contain lat").isNull();
    assertThat((float) projectedL2.getField("long"))
        .as("L2 should contain long")
        .isCloseTo(-1.539054f, within(0.000001f));

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
    assertThat(projected.getField("id")).as("Should not project id").isNull();

    locations = toStringMap((Map) projected.getField("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet())
        .as("Should contain L1 and L2")
        .containsExactlyInAnyOrder("L1", "L2");

    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.getField("latitude"))
        .as("L1 should contain latitude")
        .isCloseTo(53.992811f, within(0.000001f));
    assertThat(projectedL1.getField("lat")).as("L1 should not contain lat").isNull();
    assertThat(projectedL1.getField("long")).as("L1 should not contain long").isNull();

    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.getField("latitude"))
        .as("L2 should contain latitude")
        .isCloseTo(52.995143f, within(0.000001f));
    assertThat(projectedL2.getField("lat")).as("L2 should not contain lat").isNull();
    assertThat(projectedL2.getField("long")).as("L2 should not contain long").isNull();
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
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat(projected.getField("values")).as("Should not project values list").isNull();

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("values")).as("Should project entire list").isEqualTo(values);

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("values")).as("Should project entire list").isEqualTo(values);
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
    assertThat((long) projected.getField("id"))
        .as("Should contain the correct id value")
        .isEqualTo(34L);
    assertThat(projected.getField("points")).as("Should not project points list").isNull();

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("points"))
        .as("Should project points list")
        .isEqualTo(record.getField("points"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("points")).as("Should project points list").isNotNull();

    List<Record> points = (List<Record>) projected.getField("points");
    assertThat(points).as("Should read 2 points").hasSize(2);

    Record projectedP1 = points.get(0);
    assertThat((int) projectedP1.getField("x")).as("Should project x").isEqualTo(1);
    assertThat(projectedP1.getField("y")).as("Should not project y").isNull();

    Record projectedP2 = points.get(1);
    assertThat((int) projectedP2.getField("x")).as("Should project x").isEqualTo(3);
    assertThat(projectedP2.getField("y")).as("Should not project y").isNull();

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("points")).as("Should project points list").isNotNull();

    points = (List<Record>) projected.getField("points");
    assertThat(points).as("Should read 2 points").hasSize(2);

    projectedP1 = points.get(0);
    assertThat(projectedP1.getField("x")).as("Should not project x").isNull();
    assertThat((int) projectedP1.getField("y")).as("Should project y").isEqualTo(2);

    projectedP2 = points.get(1);
    assertThat(projectedP2.getField("x")).as("Should not project x").isNull();
    assertThat(projectedP2.getField("y")).as("Should project null y").isNull();

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
    assertThat(projected.getField("id")).as("Should not project id").isNull();
    assertThat(projected.getField("points")).as("Should project points list").isNotNull();

    points = (List<Record>) projected.getField("points");
    assertThat(points).as("Should read 2 points").hasSize(2);

    projectedP1 = points.get(0);
    assertThat(projectedP1.getField("x")).as("Should not project x").isNull();
    assertThat(projectedP1.getField("y")).as("Should not project y").isNull();
    assertThat((int) projectedP1.getField("z")).as("Should project z").isEqualTo(2);

    projectedP2 = points.get(1);
    assertThat(projectedP2.getField("x")).as("Should not project x").isNull();
    assertThat(projectedP2.getField("y")).as("Should not project y").isNull();
    assertThat(projectedP2.getField("z")).as("Should project null z").isNull();
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
    assertThat((long) projected.get(0))
        .as("Should contain the correct value in column 1")
        .isEqualTo(100L);
    assertThat((long) projected.getField("a"))
        .as("Should contain the correct value in column a")
        .isEqualTo(100L);
    assertThat(projected.get(1)).as("Should contain empty value in new column 2").isNull();
    assertThat(projected.getField("b")).as("Should contain empty value in column b").isNull();
    assertThat(projected.get(2)).as("Should contain empty value in new column 4").isNull();
    assertThat(projected.getField("d")).as("Should contain empty value in column d").isNull();
    assertThat(projected.get(3)).as("Should contain empty value in new column 6").isNull();
    assertThat(projected.getField("e")).as("Should contain empty value in column e").isNull();
  }
}
