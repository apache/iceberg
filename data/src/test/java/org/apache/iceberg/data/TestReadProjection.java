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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record record) throws IOException;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

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

    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));

    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data"));
    Assert.assertTrue("Should contain the correct data value", cmp == 0);
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

    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) full.getField("user id"));
    Assert.assertEquals(
        "Should contain the correct data value",
        0,
        Comparators.charSequences().compare("test", (CharSequence) full.getField("data%0")));

    Record projected = writeAndRead("special_characters", schema, schema.select("data%0"), record);

    Assert.assertNull("Should not contain id value", projected.getField("user id"));
    Assert.assertEquals(
        "Should contain the correct data value",
        0,
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data%0")));
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

    Assert.assertEquals("Should contain the correct 0 value", "test", projected.get(0).toString());
    Assert.assertEquals("Should contain the correct 1 value", 34L, projected.get(1));
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

    Assert.assertNull("Should contain the correct 0 value", projected.get(0));
    Assert.assertEquals("Should contain the correct 1 value", "test", projected.get(1).toString());
    Assert.assertNull("Should contain the correct 2 value", projected.get(2));
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
    Assert.assertEquals("Should contain the correct value in column 1", projected.get(0), 100L);
    Assert.assertEquals(
        "Should contain the correct value in column a", projected.getField("a"), 100L);
    Assert.assertEquals("Should contain the correct value in column 2", projected.get(1), 200L);
    Assert.assertEquals(
        "Should contain the correct value in column b", projected.getField("b"), 200L);
    Assert.assertEquals("Should contain the correct value in column 3", projected.get(2), 300L);
    Assert.assertEquals(
        "Should contain the correct value in column c", projected.getField("c"), 300L);
    Assert.assertNull("Should contain empty value on new column 4", projected.get(3));
    Assert.assertNull("Should contain the correct value in column d", projected.getField("d"));
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

    Assert.assertNotNull("Should read a non-null record", projected);
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
    Assert.assertNull("Should not project data", projected.getField("data"));
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    Assert.assertNull("Should not project id", projected.getField("id"));
    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("data"));
    Assert.assertTrue("Should contain the correct data value", cmp == 0);
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

    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    int cmp =
        Comparators.charSequences().compare("test", (CharSequence) projected.getField("renamed"));
    Assert.assertTrue("Should contain the correct data/renamed value", cmp == 0);
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
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    Assert.assertNull("Should not project location", projectedLocation);

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    projectedLocation = (Record) projected.getField("location");
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project location", projected.getField("location"));
    Assert.assertNull("Should not project longitude", projectedLocation.getField("long"));
    Assert.assertEquals(
        "Should project latitude",
        52.995143f,
        (float) projectedLocation.getField("lat"),
        0.000001f);

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.getField("location");
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project location", projected.getField("location"));
    Assert.assertNull("Should not project latitutde", projectedLocation.getField("lat"));
    Assert.assertEquals(
        "Should project longitude",
        -1.539054f,
        (float) projectedLocation.getField("long"),
        0.000001f);

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.getField("location");
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project location", projected.getField("location"));
    Assert.assertEquals(
        "Should project latitude",
        52.995143f,
        (float) projectedLocation.getField("lat"),
        0.000001f);
    Assert.assertEquals(
        "Should project longitude",
        -1.539054f,
        (float) projectedLocation.getField("long"),
        0.000001f);
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
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    Assert.assertNull("Should not project properties map", projected.getField("properties"));

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals(
        "Should project entire map",
        properties,
        toStringMap((Map) projected.getField("properties")));

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals(
        "Should project entire map",
        properties,
        toStringMap((Map) projected.getField("properties")));

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals(
        "Should project entire map",
        properties,
        toStringMap((Map) projected.getField("properties")));
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
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    Assert.assertNull("Should not project locations map", projected.getField("locations"));

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals(
        "Should project locations map",
        record.getField("locations"),
        toStringMap((Map) projected.getField("locations")));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Map<String, ?> locations = toStringMap((Map) projected.getField("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    Record projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals(
        "L1 should contain lat", 53.992811f, (float) projectedL1.getField("lat"), 0.000001);
    Assert.assertNull("L1 should not contain long", projectedL1.getField("long"));
    Record projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals(
        "L2 should contain lat", 52.995143f, (float) projectedL2.getField("lat"), 0.000001);
    Assert.assertNull("L2 should not contain long", projectedL2.getField("long"));

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    locations = toStringMap((Map) projected.getField("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertNull("L1 should not contain lat", projectedL1.getField("lat"));
    Assert.assertEquals(
        "L1 should contain long", -1.542616f, (float) projectedL1.getField("long"), 0.000001);
    projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertNull("L2 should not contain lat", projectedL2.getField("lat"));
    Assert.assertEquals(
        "L2 should contain long", -1.539054f, (float) projectedL2.getField("long"), 0.000001);

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
    Assert.assertNull("Should not project id", projected.getField("id"));
    locations = toStringMap((Map) projected.getField("locations"));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals(
        "Should contain L1 and L2", Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Record) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals(
        "L1 should contain latitude",
        53.992811f,
        (float) projectedL1.getField("latitude"),
        0.000001);
    Assert.assertNull("L1 should not contain lat", projectedL1.getField("lat"));
    Assert.assertNull("L1 should not contain long", projectedL1.getField("long"));
    projectedL2 = (Record) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals(
        "L2 should contain latitude",
        52.995143f,
        (float) projectedL2.getField("latitude"),
        0.000001);
    Assert.assertNull("L2 should not contain lat", projectedL2.getField("lat"));
    Assert.assertNull("L2 should not contain long", projectedL2.getField("long"));
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
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    Assert.assertNull("Should not project values list", projected.getField("values"));

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals("Should project entire list", values, projected.getField("values"));

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals("Should project entire list", values, projected.getField("values"));
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
    Assert.assertEquals(
        "Should contain the correct id value", 34L, (long) projected.getField("id"));
    Assert.assertNull("Should not project points list", projected.getField("points"));

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertEquals(
        "Should project points list", record.getField("points"), projected.getField("points"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project points list", projected.getField("points"));
    List<Record> points = (List<Record>) projected.getField("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    Record projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.getField("x"));
    Assert.assertNull("Should not project y", projectedP1.getField("y"));
    Record projectedP2 = points.get(1);
    Assert.assertEquals("Should project x", 3, (int) projectedP2.getField("x"));
    Assert.assertNull("Should not project y", projectedP2.getField("y"));

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project points list", projected.getField("points"));
    points = (List<Record>) projected.getField("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertNull("Should not project x", projectedP1.getField("x"));
    Assert.assertEquals("Should project y", 2, (int) projectedP1.getField("y"));
    projectedP2 = points.get(1);
    Assert.assertNull("Should not project x", projectedP2.getField("x"));
    Assert.assertEquals("Should project null y", null, projectedP2.getField("y"));

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
    Assert.assertNull("Should not project id", projected.getField("id"));
    Assert.assertNotNull("Should project points list", projected.getField("points"));
    points = (List<Record>) projected.getField("points");
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertNull("Should not project x", projectedP1.getField("x"));
    Assert.assertNull("Should not project y", projectedP1.getField("y"));
    Assert.assertEquals("Should project z", 2, (int) projectedP1.getField("z"));
    projectedP2 = points.get(1);
    Assert.assertNull("Should not project x", projectedP2.getField("x"));
    Assert.assertNull("Should not project y", projectedP2.getField("y"));
    Assert.assertEquals("Should project null z", null, projectedP2.getField("z"));
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
    Assert.assertEquals("Should contain the correct value in column 1", projected.get(0), 100L);
    Assert.assertEquals(
        "Should contain the correct value in column a", projected.getField("a"), 100L);
    Assert.assertNull("Should contain empty value in new column 2", projected.get(1));
    Assert.assertNull("Should contain empty value in column b", projected.getField("b"));
    Assert.assertNull("Should contain empty value in new column 4", projected.get(2));
    Assert.assertNull("Should contain empty value in column d", projected.getField("d"));
    Assert.assertNull("Should contain empty value in new column 6", projected.get(3));
    Assert.assertNull("Should contain empty value in column e", projected.getField("e"));
  }
}
