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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

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
import org.apache.iceberg.types.Types;
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

    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    assertThat(projected.get("data").toString())
        .as("Should contain the correct data value")
        .isEqualTo("test");
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

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

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
  public void testEmptyProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(schema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

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
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("data", "test");

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("basic_projection_id", writeSchema, idOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "data");
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("data").toString())
        .as("Should contain the correct data value")
        .isEqualTo("test");
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

    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    assertThat(projected.get("renamed").toString())
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
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    Record projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    TestHelpers.assertEmptyAvroField(projectedLocation, "long");
    assertThat((float) projectedLocation.get("lat"))
        .as("Should project latitude")
        .isCloseTo(52.995143f, within(0.000001f));

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    TestHelpers.assertEmptyAvroField(projectedLocation, "lat");
    assertThat((float) projectedLocation.get("long"))
        .as("Should project longitude")
        .isCloseTo(-1.539054f, within(0.000001f));

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.get("location");
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    assertThat((float) projectedLocation.get("lat"))
        .as("Should project latitude")
        .isCloseTo(52.995143f, within(0.000001f));

    assertThat((float) projectedLocation.get("long"))
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

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    record.put("properties", properties);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    Record projected = writeAndRead("id_only", writeSchema, idOnly, record);
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    TestHelpers.assertEmptyAvroField(projected, "properties");

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("properties")))
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
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    TestHelpers.assertEmptyAvroField(projected, "locations");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("locations")))
        .as("Should project locations map")
        .isEqualTo(record.get("locations"));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    Map<String, ?> locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    Record projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.get("lat"))
        .as("L1 should contain lat")
        .isCloseTo(53.992811f, within(0.000001f));

    TestHelpers.assertEmptyAvroField(projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.get("lat"))
        .as("L2 should contain lat")
        .isCloseTo(52.995143f, within(0.000001f));

    TestHelpers.assertEmptyAvroField(projectedL2, "long");

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    TestHelpers.assertEmptyAvroField(projectedL1, "lat");
    assertThat((float) projectedL1.get("long"))
        .as("L1 should contain long")
        .isCloseTo(-1.542616f, within(0.000001f));

    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    TestHelpers.assertEmptyAvroField(projectedL2, "lat");
    assertThat((float) projectedL2.get("long"))
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
    TestHelpers.assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.get("latitude"))
        .as("L1 should contain latitude")
        .isCloseTo(53.992811f, within(0.000001f));
    TestHelpers.assertEmptyAvroField(projectedL1, "lat");
    TestHelpers.assertEmptyAvroField(projectedL1, "long");
    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.get("latitude"))
        .as("L2 should contain latitude")
        .isCloseTo(52.995143f, within(0.000001f));

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
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    TestHelpers.assertEmptyAvroField(projected, "values");

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("values")).as("Should project entire list").isEqualTo(values);

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("values")).as("Should project entire list").isEqualTo(values);
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
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    TestHelpers.assertEmptyAvroField(projected, "points");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points"))
        .as("Should project points list")
        .isEqualTo(record.get("points"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    List<Record> points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    Record projectedP1 = points.get(0);
    assertThat((int) projectedP1.get("x")).as("Should project x").isEqualTo(1);
    TestHelpers.assertEmptyAvroField(projectedP1, "y");
    Record projectedP2 = points.get(1);
    assertThat((int) projectedP2.get("x")).as("Should project x").isEqualTo(3);
    TestHelpers.assertEmptyAvroField(projectedP2, "y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    TestHelpers.assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    projectedP1 = points.get(0);
    TestHelpers.assertEmptyAvroField(projectedP1, "x");
    assertThat((int) projectedP1.get("y")).as("Should project y").isEqualTo(2);
    projectedP2 = points.get(1);
    TestHelpers.assertEmptyAvroField(projectedP2, "x");
    assertThat(projectedP2.get("y")).as("Should project null y").isNull();

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
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    projectedP1 = points.get(0);
    TestHelpers.assertEmptyAvroField(projectedP1, "x");
    TestHelpers.assertEmptyAvroField(projectedP1, "y");
    assertThat((int) projectedP1.get("z")).as("Should project z").isEqualTo(2);
    projectedP2 = points.get(1);
    TestHelpers.assertEmptyAvroField(projectedP2, "x");
    TestHelpers.assertEmptyAvroField(projectedP2, "y");
    assertThat(projectedP2.get("z")).as("Should project null z").isNull();
  }
}
