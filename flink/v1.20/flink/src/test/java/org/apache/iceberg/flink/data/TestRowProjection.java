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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withPrecision;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestRowProjection {

  @TempDir private Path temp;

  private RowData writeAndRead(String desc, Schema writeSchema, Schema readSchema, RowData row)
      throws IOException {
    File file = File.createTempFile("junit", desc + ".avro", temp.toFile());
    assertThat(file.delete()).isTrue();

    try (FileAppender<RowData> appender =
        Avro.write(Files.localOutput(file))
            .schema(writeSchema)
            .createWriterFunc(ignore -> new FlinkAvroWriter(FlinkSchemaUtil.convert(writeSchema)))
            .build()) {
      appender.add(row);
    }

    Iterable<RowData> records =
        Avro.read(Files.localInput(file))
            .project(readSchema)
            .createReaderFunc(FlinkAvroReader::new)
            .build();

    return Iterables.getOnlyElement(records);
  }

  @Test
  public void testFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    RowData projected = writeAndRead("full_projection", schema, schema, row);

    assertThat(projected.getLong(0)).isEqualTo(34);
    assertThat(projected.getString(1)).asString().isEqualTo("test");
  }

  @Test
  public void testSpecialCharacterProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "user id", Types.LongType.get()),
            Types.NestedField.optional(1, "data%0", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    RowData full = writeAndRead("special_chars", schema, schema, row);

    assertThat(full.getLong(0)).isEqualTo(34L);
    assertThat(full.getString(1)).asString().isEqualTo("test");

    RowData projected = writeAndRead("special_characters", schema, schema.select("data%0"), full);

    assertThat(projected.getArity()).isEqualTo(1);
    assertThat(projected.getString(0)).asString().isEqualTo("test");
  }

  @Test
  public void testReorderedFullProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    Schema reordered =
        new Schema(
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("full_projection", schema, reordered, row);

    assertThat(projected.getString(0)).asString().isEqualTo("test");
    assertThat(projected.getLong(1)).isEqualTo(34);
  }

  @Test
  public void testReorderedProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    Schema reordered =
        new Schema(
            Types.NestedField.optional(2, "missing_1", Types.StringType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "missing_2", Types.LongType.get()));

    RowData projected = writeAndRead("full_projection", schema, reordered, row);

    assertThat(projected.isNullAt(0)).isTrue();
    assertThat(projected.getString(1)).asString().isEqualTo("test");
    assertThat(projected.isNullAt(2)).isTrue();
  }

  @Test
  public void testRenamedAddedField() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(1, "a", Types.LongType.get()),
            Types.NestedField.required(2, "b", Types.LongType.get()),
            Types.NestedField.required(3, "d", Types.LongType.get()));

    RowData row = GenericRowData.of(100L, 200L, 300L);

    Schema renamedAdded =
        new Schema(
            Types.NestedField.optional(1, "a", Types.LongType.get()),
            Types.NestedField.optional(2, "b", Types.LongType.get()),
            Types.NestedField.optional(3, "c", Types.LongType.get()),
            Types.NestedField.optional(4, "d", Types.LongType.get()));

    RowData projected = writeAndRead("rename_and_add_column_projection", schema, renamedAdded, row);
    assertThat(projected.getLong(0))
        .as("Should contain the correct value in column 1")
        .isEqualTo(100L);
    assertThat(projected.getLong(1))
        .as("Should contain the correct value in column 2")
        .isEqualTo(200L);
    assertThat(projected.getLong(2))
        .as("Should contain the correct value in column 1")
        .isEqualTo(300L);
    assertThat(projected.isNullAt(3)).as("Should contain empty value on new column 4").isTrue();
  }

  @Test
  public void testEmptyProjection() throws Exception {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    RowData projected = writeAndRead("empty_projection", schema, schema.select(), row);

    assertThat(projected).isNotNull();
    assertThat(projected.getArity()).isEqualTo(0);
  }

  @Test
  public void testBasicProjection() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("basic_projection_id", writeSchema, idOnly, row);
    assertThat(projected.getArity()).as("Should not project data").isEqualTo(1);
    assertThat(projected.getLong(0)).isEqualTo(34L);

    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, row);

    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    int cmp = Comparators.charSequences().compare("test", projected.getString(0).toString());
    assertThat(projected.getString(0)).asString().isEqualTo("test");
  }

  @Test
  public void testRename() throws Exception {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowData row = GenericRowData.of(34L, StringData.fromString("test"));

    Schema readSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "renamed", Types.StringType.get()));

    RowData projected = writeAndRead("project_and_rename", writeSchema, readSchema, row);

    assertThat(projected.getLong(0)).isEqualTo(34L);
    assertThat(projected.getString(1))
        .as("Should contain the correct data/renamed value")
        .asString()
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

    RowData location = GenericRowData.of(52.995143f, -1.539054f);
    RowData record = GenericRowData.of(34L, location);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("id_only", writeSchema, idOnly, record);
    assertThat(projected.getArity()).isEqualTo(1);
    assertThat(projected.getLong(0)).as("Should contain the correct id value").isEqualTo(34L);

    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    RowData projectedLocation = projected.getRow(0, 1);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).as("Should project location").isFalse();
    assertThat(projectedLocation.getArity()).as("Should not project longitude").isEqualTo(1);
    assertThat(projectedLocation.getFloat(0))
        .as("Should project latitude")
        .isEqualTo(52.995143f, withPrecision(0.000001f));

    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = projected.getRow(0, 1);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).as("Should project location").isFalse();
    assertThat(projectedLocation.getArity()).as("Should not project latitutde").isEqualTo(1);
    assertThat(projectedLocation.getFloat(0))
        .as("Should project longitude")
        .isEqualTo(-1.539054f, withPrecision(0.000001f));

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = projected.getRow(0, 1);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).as("Should project location").isFalse();
    assertThat(projectedLocation.getFloat(0))
        .as("Should project latitude")
        .isEqualTo(52.995143f, withPrecision(0.000001f));
    assertThat(projectedLocation.getFloat(1))
        .as("Should project longitude")
        .isEqualTo(-1.539054f, withPrecision(0.000001f));
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

    GenericMapData properties =
        new GenericMapData(
            ImmutableMap.of(
                StringData.fromString("a"),
                StringData.fromString("A"),
                StringData.fromString("b"),
                StringData.fromString("B")));

    RowData row = GenericRowData.of(34L, properties);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("id_only", writeSchema, idOnly, row);
    assertThat(projected.getLong(0)).isEqualTo(34L);
    assertThat(projected.getArity()).as("Should not project properties map").isEqualTo(1);

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getMap(0)).isEqualTo(properties);

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getMap(0)).isEqualTo(properties);

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getMap(0)).isEqualTo(properties);
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

    RowData l1 = GenericRowData.of(53.992811f, -1.542616f);
    RowData l2 = GenericRowData.of(52.995143f, -1.539054f);
    GenericMapData map =
        new GenericMapData(
            ImmutableMap.of(StringData.fromString("L1"), l1, StringData.fromString("L2"), l2));
    RowData row = GenericRowData.of(34L, map);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("id_only", writeSchema, idOnly, row);
    assertThat(projected.getLong(0)).isEqualTo(34L);
    assertThat(projected.getArity()).as("Should not project locations map").isEqualTo(1);

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getMap(0)).isEqualTo(row.getMap(1));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), row);
    GenericMapData locations = (GenericMapData) projected.getMap(0);
    assertThat(locations).isNotNull();
    GenericArrayData l1l2Array =
        new GenericArrayData(
            new Object[] {StringData.fromString("L2"), StringData.fromString("L1")});
    assertThat(locations.keyArray()).isEqualTo(l1l2Array);
    RowData projectedL1 = (RowData) locations.get(StringData.fromString("L1"));
    assertThat(projectedL1).isNotNull();
    assertThat(projectedL1.getFloat(0))
        .as("L1 should contain lat")
        .isEqualTo(53.992811f, withPrecision(0.000001f));
    assertThat(projectedL1.getArity()).as("L1 should not contain long").isEqualTo(1);
    RowData projectedL2 = (RowData) locations.get(StringData.fromString("L2"));
    assertThat(projectedL2).isNotNull();
    assertThat(projectedL2.getFloat(0))
        .as("L2 should contain lat")
        .isEqualTo(52.995143f, withPrecision(0.000001f));
    assertThat(projectedL2.getArity()).as("L2 should not contain long").isEqualTo(1);

    projected = writeAndRead("long_only", writeSchema, writeSchema.select("locations.long"), row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    locations = (GenericMapData) projected.getMap(0);
    assertThat(locations).isNotNull();
    assertThat(locations.keyArray()).isEqualTo(l1l2Array);
    projectedL1 = (RowData) locations.get(StringData.fromString("L1"));
    assertThat(projectedL1).isNotNull();
    assertThat(projectedL1.getArity()).as("L1 should not contain lat").isEqualTo(1);
    assertThat(projectedL1.getFloat(0))
        .as("L1 should contain long")
        .isEqualTo(-1.542616f, withPrecision(0.000001f));
    projectedL2 = (RowData) locations.get(StringData.fromString("L2"));
    assertThat(projectedL2).isNotNull();
    assertThat(projectedL2.getArity()).as("L2 should not contain lat").isEqualTo(1);
    assertThat(projectedL2.getFloat(0))
        .as("L2 should contain long")
        .isEqualTo(-1.539054f, withPrecision(0.000001f));

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

    projected = writeAndRead("latitude_renamed", writeSchema, latitiudeRenamed, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    locations = (GenericMapData) projected.getMap(0);
    assertThat(locations).isNotNull();
    assertThat(locations.keyArray()).isEqualTo(l1l2Array);
    projectedL1 = (RowData) locations.get(StringData.fromString("L1"));
    assertThat(projectedL1).isNotNull();
    assertThat(projectedL1.getFloat(0))
        .as("L1 should contain latitude")
        .isEqualTo(53.992811f, withPrecision(0.000001f));
    projectedL2 = (RowData) locations.get(StringData.fromString("L2"));
    assertThat(projectedL2).isNotNull();
    assertThat(projectedL2.getFloat(0))
        .as("L2 should contain latitude")
        .isEqualTo(52.995143f, withPrecision(0.000001f));
  }

  @Test
  public void testListProjection() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                10, "values", Types.ListType.ofOptional(11, Types.LongType.get())));

    GenericArrayData values = new GenericArrayData(new Long[] {56L, 57L, 58L});

    RowData row = GenericRowData.of(34L, values);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("id_only", writeSchema, idOnly, row);
    assertThat(projected.getLong(0)).isEqualTo(34L);
    assertThat(projected.getArity()).as("Should not project values list").isEqualTo(1);

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getArray(0)).isEqualTo(values);

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getArray(0)).isEqualTo(values);
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

    RowData p1 = GenericRowData.of(1, 2);
    RowData p2 = GenericRowData.of(3, null);
    GenericArrayData arrayData = new GenericArrayData(new RowData[] {p1, p2});
    RowData row = GenericRowData.of(34L, arrayData);

    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));

    RowData projected = writeAndRead("id_only", writeSchema, idOnly, row);
    assertThat(projected.getLong(0)).isEqualTo(34L);
    assertThat(projected.getArity()).isEqualTo(1);

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.getArray(0)).isEqualTo(row.getArray(1));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).isFalse();
    ArrayData points = projected.getArray(0);
    assertThat(points.size()).isEqualTo(2);
    RowData projectedP1 = points.getRow(0, 2);
    assertThat(projectedP1.getInt(0)).as("Should project x").isEqualTo(1);
    assertThat(projectedP1.getArity()).as("Should not project y").isEqualTo(1);
    RowData projectedP2 = points.getRow(1, 2);
    assertThat(projectedP2.getArity()).as("Should not project y").isEqualTo(1);
    assertThat(projectedP2.getInt(0)).as("Should project x").isEqualTo(3);

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).isFalse();
    points = projected.getArray(0);
    assertThat(points.size()).isEqualTo(2);
    projectedP1 = points.getRow(0, 2);
    assertThat(projectedP1.getArity()).as("Should not project x").isEqualTo(1);
    assertThat(projectedP1.getInt(0)).as("Should project y").isEqualTo(2);
    projectedP2 = points.getRow(1, 2);
    assertThat(projectedP2.getArity()).as("Should not project x").isEqualTo(1);
    assertThat(projectedP2.isNullAt(0)).as("Should project null y").isTrue();

    Schema yRenamed =
        new Schema(
            Types.NestedField.optional(
                22,
                "points",
                Types.ListType.ofOptional(
                    21,
                    Types.StructType.of(
                        Types.NestedField.optional(18, "z", Types.IntegerType.get())))));

    projected = writeAndRead("y_renamed", writeSchema, yRenamed, row);
    assertThat(projected.getArity()).as("Should not project id").isEqualTo(1);
    assertThat(projected.isNullAt(0)).isFalse();
    points = projected.getArray(0);
    assertThat(points.size()).isEqualTo(2);
    projectedP1 = points.getRow(0, 2);
    assertThat(projectedP1.getArity()).as("Should not project x and y").isEqualTo(1);
    assertThat(projectedP1.getInt(0)).as("Should project z").isEqualTo(2);
    projectedP2 = points.getRow(1, 2);
    assertThat(projectedP2.getArity()).as("Should not project x and y").isEqualTo(1);
    assertThat(projectedP2.isNullAt(0)).as("Should project null z").isTrue();
  }

  @Test
  public void testAddedFieldsWithRequiredChildren() throws Exception {
    Schema schema = new Schema(Types.NestedField.required(1, "a", Types.LongType.get()));

    RowData row = GenericRowData.of(100L);

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

    RowData projected =
        writeAndRead("add_fields_with_required_children_projection", schema, addedFields, row);
    assertThat(projected.getLong(0))
        .as("Should contain the correct value in column 1")
        .isEqualTo(100L);
    assertThat(projected.isNullAt(1)).as("Should contain empty value in new column 2").isTrue();
    assertThat(projected.isNullAt(2)).as("Should contain empty value in new column 4").isTrue();
    assertThat(projected.isNullAt(3)).as("Should contain empty value in new column 6").isTrue();
  }
}
