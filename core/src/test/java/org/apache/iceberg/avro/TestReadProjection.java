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
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class TestReadProjection {
  protected abstract Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, Record record) throws IOException;

  @TempDir Path temp;

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

    assertThat((Long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("data"));
    assertThat(cmp).as("Should contain the correct data value").isEqualTo(0);
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
    assertEmptyAvroField(projected, "data");
    assertThat((Long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, record);

    assertEmptyAvroField(projected, "id");
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("data"));
    assertThat(cmp).as("Should contain the correct data value").isEqualTo(0);
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

    assertThat((Long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.get("renamed"));
    assertThat(cmp).as("Should contain the correct data/renamed value").isEqualTo(0);
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
    assertEmptyAvroField(projected, "location");
    assertThat((long) projected.get("id")).as("Should contain the correct id value").isEqualTo(34L);

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    Record projectedLocation = (Record) projected.get("location");
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    assertEmptyAvroField(projectedLocation, "long");
    assertThat((Float) projectedLocation.get("lat"))
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
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    assertEmptyAvroField(projectedLocation, "lat");
    assertThat((Float) projectedLocation.get("long"))
        .as("Should project longitude")
        .isCloseTo(-1.539054f, within(0.000001f));

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Record) projected.get("location");
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("location")).as("Should project location").isNotNull();
    assertThat((Float) projectedLocation.get("lat"))
        .as("Should project latitude")
        .isCloseTo(52.995143f, within(0.000001f));
    assertThat((Float) projectedLocation.get("long"))
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
    assertEmptyAvroField(projected, "properties");

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, record);
    assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, record);
    assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("properties")))
        .as("Should project entire map")
        .isEqualTo(properties);

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, record);
    assertEmptyAvroField(projected, "id");
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
    assertEmptyAvroField(projected, "locations");

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), record);
    assertEmptyAvroField(projected, "id");
    assertThat(toStringMap((Map) projected.get("locations")))
        .as("Should project locations map")
        .isEqualTo(record.get("locations"));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), record);
    assertEmptyAvroField(projected, "id");
    Map<String, ?> locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    Record projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.get("lat"))
        .as("L1 should contain lat")
        .isCloseTo(53.992811f, within(0.000001f));
    assertEmptyAvroField(projectedL1, "long");
    Record projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.get("lat"))
        .as("L2 should contain lat")
        .isCloseTo(52.995143f, within(0.000001f));
    assertEmptyAvroField(projectedL2, "y");

    projected =
        writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), record);
    assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertEmptyAvroField(projectedL1, "lat");
    assertThat((float) projectedL1.get("long"))
        .as("L1 should contain long")
        .isCloseTo(-1.542616f, within(0.000001f));
    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertEmptyAvroField(projectedL2, "lat");
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
    assertEmptyAvroField(projected, "id");
    locations = toStringMap((Map) projected.get("locations"));
    assertThat(locations).as("Should project locations map").isNotNull();
    assertThat(locations.keySet()).as("Should contain L1 and L2").containsExactly("L1", "L2");
    projectedL1 = (Record) locations.get("L1");
    assertThat(projectedL1).as("L1 should not be null").isNotNull();
    assertThat((float) projectedL1.get("latitude"))
        .as("L1 should contain latitude")
        .isCloseTo(53.992811f, within(0.000001f));
    assertEmptyAvroField(projectedL1, "lat");
    assertEmptyAvroField(projectedL1, "long");
    projectedL2 = (Record) locations.get("L2");
    assertThat(projectedL2).as("L2 should not be null").isNotNull();
    assertThat((float) projectedL2.get("latitude"))
        .as("L2 should contain latitude")
        .isCloseTo(52.995143f, within(0.000001f));
    assertEmptyAvroField(projectedL2, "lat");
    assertEmptyAvroField(projectedL2, "long");
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
    assertEmptyAvroField(projected, "values");

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, record);
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("values")).as("Should project entire list").isEqualTo(values);

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, record);
    assertEmptyAvroField(projected, "id");
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
    assertEmptyAvroField(projected, "points");

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), record);
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points"))
        .as("Should project points list")
        .isEqualTo(record.get("points"));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), record);
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    List<Record> points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    Record projectedP1 = points.get(0);
    assertThat((int) projectedP1.get("x")).as("Should project x").isEqualTo(1);
    assertEmptyAvroField(projectedP1, "y");
    Record projectedP2 = points.get(1);
    assertThat((int) projectedP2.get("x")).as("Should project x").isEqualTo(3);
    assertEmptyAvroField(projectedP2, "y");

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), record);
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    projectedP1 = points.get(0);
    assertEmptyAvroField(projectedP1, "x");
    assertThat((int) projectedP1.get("y")).as("Should project y").isEqualTo(2);
    projectedP2 = points.get(1);
    assertEmptyAvroField(projectedP2, "x");
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
    assertEmptyAvroField(projected, "id");
    assertThat(projected.get("points")).as("Should project points list").isNotNull();
    points = (List<Record>) projected.get("points");
    assertThat(points).as("Should read 2 points").hasSize(2);
    projectedP1 = points.get(0);
    assertEmptyAvroField(projectedP1, "x");
    assertEmptyAvroField(projectedP1, "y");
    assertThat((int) projectedP1.get("z")).as("Should project z").isEqualTo(2);
    projectedP2 = points.get(1);
    assertEmptyAvroField(projectedP2, "x");
    assertEmptyAvroField(projectedP2, "y");
    assertThat(projectedP2.get("z")).as("Should project null z").isNull();
  }

  @Test
  public void testEmptyStructProjection() throws Exception {
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

    Schema emptyStruct =
        new Schema(Types.NestedField.required(3, "location", Types.StructType.of()));

    Record projected = writeAndRead("empty_proj", writeSchema, emptyStruct, record);
    assertEmptyAvroField(projected, "id");
    Record result = (Record) projected.get("location");

    assertThat(projected.get(0)).as("location should be in the 0th position").isEqualTo(result);
    assertThat(result).as("Should contain an empty record").isNotNull();
    assertEmptyAvroField(result, "lat");
    assertEmptyAvroField(result, "long");
  }

  @Test
  public void testEmptyStructRequiredProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(record.getSchema().getField("location").schema());
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct =
        new Schema(Types.NestedField.required(3, "location", Types.StructType.of()));

    Record projected = writeAndRead("empty_req_proj", writeSchema, emptyStruct, record);
    assertEmptyAvroField(projected, "id");
    Record result = (Record) projected.get("location");
    assertThat(projected.get(0)).as("location should be in the 0th position").isEqualTo(result);
    assertThat(result).as("Should contain an empty record").isNotNull();
    assertEmptyAvroField(result, "lat");
    assertEmptyAvroField(result, "long");
  }

  @Test
  public void testRequiredEmptyStructInRequiredStruct() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()),
                    Types.NestedField.required(4, "empty", Types.StructType.of()))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record location = new Record(record.getSchema().getField("location").schema());
    location.put("lat", 52.995143f);
    location.put("long", -1.539054f);
    record.put("location", location);

    Schema emptyStruct =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(4, "empty", Types.StructType.of()))));

    Record projected = writeAndRead("req_empty_req_proj", writeSchema, emptyStruct, record);
    assertThat(projected.get("id")).as("Should project id").isEqualTo(34L);
    Record result = (Record) projected.get("location");
    assertThat(projected.get(1)).as("location should be in the 1st position").isEqualTo(result);
    assertThat(result).as("Should contain an empty record").isNotNull();
    assertEmptyAvroField(result, "lat");
    assertEmptyAvroField(result, "long");
    assertThat(result.getSchema().getField("empty")).as("Should project empty").isNotNull();
    assertThat(result.get("empty")).as("Empty should not be null").isNotNull();
    assertThat(((Record) result.get("empty")).getSchema().getFields())
        .as("Empty should be empty")
        .isEmpty();
  }

  @Test
  public void testEmptyNestedStructProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                3,
                "outer",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.optional(
                        2,
                        "inner",
                        Types.StructType.of(
                            Types.NestedField.required(5, "lon", Types.FloatType.get()))))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record outer =
        new Record(AvroSchemaUtil.fromOption(record.getSchema().getField("outer").schema()));
    Record inner =
        new Record(AvroSchemaUtil.fromOption(outer.getSchema().getField("inner").schema()));
    inner.put("lon", 32.14f);
    outer.put("lat", 52.995143f);
    outer.put("inner", inner);
    record.put("outer", outer);

    Schema emptyStruct =
        new Schema(
            Types.NestedField.required(
                3,
                "outer",
                Types.StructType.of(
                    Types.NestedField.required(2, "inner", Types.StructType.of()))));

    Record projected = writeAndRead("nested_empty_proj", writeSchema, emptyStruct, record);
    assertEmptyAvroField(projected, "id");
    Record outerResult = (Record) projected.get("outer");
    assertThat(projected.get(0)).as("Outer should be in the 0th position").isEqualTo(outerResult);
    assertThat(outerResult).as("Should contain the outer record").isNotNull();
    assertEmptyAvroField(outerResult, "lat");
    Record innerResult = (Record) outerResult.get("inner");
    assertThat(outerResult.get(0)).as("Inner should be in the 0th position").isEqualTo(innerResult);
    assertThat(innerResult).as("Should contain the inner record").isNotNull();
    assertEmptyAvroField(innerResult, "lon");
  }

  @Test
  public void testEmptyNestedStructRequiredProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.required(
                3,
                "outer",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(
                        2,
                        "inner",
                        Types.StructType.of(
                            Types.NestedField.required(5, "lon", Types.FloatType.get()))))));

    Record record = new Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("id", 34L);
    Record outer = new Record(record.getSchema().getField("outer").schema());
    Record inner = new Record(outer.getSchema().getField("inner").schema());
    inner.put("lon", 32.14f);
    outer.put("lat", 52.995143f);
    outer.put("inner", inner);
    record.put("outer", outer);

    Schema emptyStruct =
        new Schema(
            Types.NestedField.required(
                3,
                "outer",
                Types.StructType.of(
                    Types.NestedField.required(2, "inner", Types.StructType.of()))));

    Record projected = writeAndRead("nested_empty_req_proj", writeSchema, emptyStruct, record);
    assertEmptyAvroField(projected, "id");
    Record outerResult = (Record) projected.get("outer");
    assertThat(projected.get(0)).as("Outer should be in the 0th position").isEqualTo(outerResult);
    assertThat(outerResult).as("Should contain the outer record").isNotNull();
    assertEmptyAvroField(outerResult, "lat");
    Record innerResult = (Record) outerResult.get("inner");
    assertThat(outerResult.get(0)).as("Inner should be in the 0th position").isEqualTo(innerResult);
    assertThat(innerResult).as("Should contain the inner record").isNotNull();
    assertEmptyAvroField(innerResult, "lon");
  }

  private void assertEmptyAvroField(GenericRecord record, String field) {
    assertThatThrownBy(() -> record.get(field))
        .isInstanceOf(AvroRuntimeException.class)
        .hasMessage("Not a valid schema field: " + field);
  }
}
