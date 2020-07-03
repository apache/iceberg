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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.flink.types.Row;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRowProjection {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private Row writeAndRead(String desc, Schema writeSchema, Schema readSchema, Row row) throws IOException {
    File file = temp.newFile(desc + ".avro");
    Assert.assertTrue(file.delete());

    try (FileAppender<Row> appender = Avro.write(Files.localOutput(file))
        .schema(writeSchema)
        .createWriterFunc(FlinkAvroWriter::new)
        .build()) {
      appender.add(row);
    }

    Iterable<Row> records = Avro.read(Files.localInput(file))
        .project(readSchema)
        .createReaderFunc(FlinkAvroReader::new)
        .build();

    return Iterables.getOnlyElement(records);
  }

  @Test
  public void testFullProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Row projected = writeAndRead("full_projection", schema, schema, row);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));

    int cmp = Comparators.charSequences()
        .compare("test", (CharSequence) projected.getField(1));
    Assert.assertEquals("Should contain the correct data value", cmp, 0);
  }

  @Test
  public void testSpecialCharacterProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "user id", Types.LongType.get()),
        Types.NestedField.optional(1, "data%0", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Row full = writeAndRead("special_chars", schema, schema, row);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) row.getField(0));
    Assert.assertEquals("Should contain the correct data value",
        0,
        Comparators.charSequences().compare("test", (CharSequence) row.getField(1)));

    Row projected = writeAndRead("special_characters", schema, schema.select("data%0"), row);

    Assert.assertEquals("Should not contain id value", 1, projected.getArity());
    Assert.assertEquals("Should contain the correct data value",
        0,
        Comparators.charSequences().compare("test", (CharSequence) projected.getField(0)));
  }

  @Test
  public void testReorderedFullProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Schema reordered = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("full_projection", schema, reordered, row);

    Assert.assertEquals("Should contain the correct 0 value", "test", projected.getField(0).toString());
    Assert.assertEquals("Should contain the correct 1 value", 34L, projected.getField(1));
  }

  @Test
  public void testReorderedProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Schema reordered = new Schema(
        Types.NestedField.optional(2, "missing_1", Types.StringType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get()),
        Types.NestedField.optional(3, "missing_2", Types.LongType.get())
    );

    Row projected = writeAndRead("full_projection", schema, reordered, row);

    Assert.assertNull("Should contain the correct 0 value", projected.getField(0));
    Assert.assertEquals("Should contain the correct 1 value", "test", projected.getField(1).toString());
    Assert.assertNull("Should contain the correct 2 value", projected.getField(2));
  }

  @Test
  public void testRenamedAddedField() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "a", Types.LongType.get()),
        Types.NestedField.required(2, "b", Types.LongType.get()),
        Types.NestedField.required(3, "d", Types.LongType.get())
    );

    Row row = Row.of(100L, 200L, 300L);

    Schema renamedAdded = new Schema(
        Types.NestedField.optional(1, "a", Types.LongType.get()),
        Types.NestedField.optional(2, "b", Types.LongType.get()),
        Types.NestedField.optional(3, "c", Types.LongType.get()),
        Types.NestedField.optional(4, "d", Types.LongType.get())
    );

    Row projected = writeAndRead("rename_and_add_column_projection", schema, renamedAdded, row);
    Assert.assertEquals("Should contain the correct value in column 1", projected.getField(0), 100L);
    Assert.assertEquals("Should contain the correct value in column 2", projected.getField(1), 200L);
    Assert.assertEquals("Should contain the correct value in column 3", projected.getField(2), 300L);
    Assert.assertNull("Should contain empty value on new column 4", projected.getField(3));
  }

  @Test
  public void testEmptyProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Row projected = writeAndRead("empty_projection", schema, schema.select(), row);

    Assert.assertNotNull("Should read a non-null record", projected);
    try {
      projected.getField(0);
      Assert.fail("Should not retrieve value with ordinal 0");
    } catch (ArrayIndexOutOfBoundsException e) {
      // this is expected because there are no values
    }
  }

  @Test
  public void testBasicProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("basic_projection_id", writeSchema, idOnly, row);
    Assert.assertEquals("Should not project data", 1, projected.getArity());
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));

    Schema dataOnly = new Schema(
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    projected = writeAndRead("basic_projection_data", writeSchema, dataOnly, row);

    Assert.assertEquals("Should not project id", 1, projected.getArity());
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.getField(0));
    Assert.assertEquals("Should contain the correct data value", 0, cmp);
  }

  @Test
  public void testRename() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Schema readSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "renamed", Types.StringType.get())
    );

    Row projected = writeAndRead("project_and_rename", writeSchema, readSchema, row);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));
    int cmp = Comparators.charSequences().compare("test", (CharSequence) projected.getField(1));
    Assert.assertEquals("Should contain the correct data/renamed value", 0, cmp);
  }

  @Test
  public void testNestedStructProjection() throws Exception {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get()),
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    Row location = Row.of(52.995143f, -1.539054f);
    Row record = Row.of(34L, location);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("id_only", writeSchema, idOnly, record);
    Assert.assertEquals("Should not project location", 1, projected.getArity());
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));

    Schema latOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(1, "lat", Types.FloatType.get())
        ))
    );

    projected = writeAndRead("latitude_only", writeSchema, latOnly, record);
    Row projectedLocation = (Row) projected.getField(0);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project location", projected.getField(0));
    Assert.assertEquals("Should not project longitude", 1, projectedLocation.getArity());
    Assert.assertEquals("Should project latitude",
        52.995143f, (float) projectedLocation.getField(0), 0.000001f);

    Schema longOnly = new Schema(
        Types.NestedField.optional(3, "location", Types.StructType.of(
            Types.NestedField.required(2, "long", Types.FloatType.get())
        ))
    );

    projected = writeAndRead("longitude_only", writeSchema, longOnly, record);
    projectedLocation = (Row) projected.getField(0);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project location", projected.getField(0));
    Assert.assertEquals("Should not project latitutde", 1, projectedLocation.getArity());
    Assert.assertEquals("Should project longitude",
        -1.539054f, (float) projectedLocation.getField(0), 0.000001f);

    Schema locationOnly = writeSchema.select("location");
    projected = writeAndRead("location_only", writeSchema, locationOnly, record);
    projectedLocation = (Row) projected.getField(0);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project location", projected.getField(0));
    Assert.assertEquals("Should project latitude",
        52.995143f, (float) projectedLocation.getField(0), 0.000001f);
    Assert.assertEquals("Should project longitude",
        -1.539054f, (float) projectedLocation.getField(1), 0.000001f);
  }

  @Test
  public void testMapProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "properties",
            Types.MapType.ofOptional(6, 7, Types.StringType.get(), Types.StringType.get()))
    );

    Map<String, String> properties = ImmutableMap.of("a", "A", "b", "B");

    Row row = Row.of(34L, properties);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("id_only", writeSchema, idOnly, row);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));
    Assert.assertEquals("Should not project properties map", 1, projected.getArity());

    Schema keyOnly = writeSchema.select("properties.key");
    projected = writeAndRead("key_only", writeSchema, keyOnly, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project entire map", properties, toStringMap((Map) projected.getField(0)));

    Schema valueOnly = writeSchema.select("properties.value");
    projected = writeAndRead("value_only", writeSchema, valueOnly, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project entire map",
        properties, toStringMap((Map) projected.getField(0)));

    Schema mapOnly = writeSchema.select("properties");
    projected = writeAndRead("map_only", writeSchema, mapOnly, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project entire map", properties, toStringMap((Map) projected.getField(0)));
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
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "lat", Types.FloatType.get()),
                Types.NestedField.required(2, "long", Types.FloatType.get())
            )
        ))
    );

    Row l1 = Row.of(53.992811f, -1.542616f);
    Row l2 = Row.of(52.995143f, -1.539054f);
    Row row = Row.of(34L, ImmutableMap.of("L1", l1, "L2", l2));

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("id_only", writeSchema, idOnly, row);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));
    Assert.assertEquals("Should not project locations map", 1, projected.getArity());

    projected = writeAndRead("all_locations", writeSchema, writeSchema.select("locations"), row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project locations map",
        row.getField(1), toStringMap((Map) projected.getField(0)));

    projected = writeAndRead("lat_only", writeSchema, writeSchema.select("locations.lat"), row);
    Map<String, ?> locations = toStringMap((Map) projected.getField(0));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    Row projectedL1 = (Row) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals("L1 should contain lat",
        53.992811f, (float) projectedL1.getField(0), 0.000001);
    Assert.assertEquals("L1 should not contain long", 1, projectedL1.getArity());
    Row projectedL2 = (Row) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals("L2 should contain lat",
        52.995143f, (float) projectedL2.getField(0), 0.000001);
    Assert.assertEquals("L2 should not contain long", 1, projectedL2.getArity());

    projected = writeAndRead("long_only",
        writeSchema, writeSchema.select("locations.long"), row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    locations = toStringMap((Map) projected.getField(0));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Row) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals("L1 should not contain lat", 1, projectedL1.getArity());
    Assert.assertEquals("L1 should contain long",
        -1.542616f, (float) projectedL1.getField(0), 0.000001);
    projectedL2 = (Row) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals("L2 should not contain lat", 1, projectedL2.getArity());
    Assert.assertEquals("L2 should contain long",
        -1.539054f, (float) projectedL2.getField(0), 0.000001);

    Schema latitiudeRenamed = new Schema(
        Types.NestedField.optional(5, "locations", Types.MapType.ofOptional(6, 7,
            Types.StringType.get(),
            Types.StructType.of(
                Types.NestedField.required(1, "latitude", Types.FloatType.get())
            )
        ))
    );

    projected = writeAndRead("latitude_renamed", writeSchema, latitiudeRenamed, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    locations = toStringMap((Map) projected.getField(0));
    Assert.assertNotNull("Should project locations map", locations);
    Assert.assertEquals("Should contain L1 and L2",
        Sets.newHashSet("L1", "L2"), locations.keySet());
    projectedL1 = (Row) locations.get("L1");
    Assert.assertNotNull("L1 should not be null", projectedL1);
    Assert.assertEquals("L1 should contain latitude",
        53.992811f, (float) projectedL1.getField(0), 0.000001);
    projectedL2 = (Row) locations.get("L2");
    Assert.assertNotNull("L2 should not be null", projectedL2);
    Assert.assertEquals("L2 should contain latitude",
        52.995143f, (float) projectedL2.getField(0), 0.000001);
  }

  @Test
  public void testListProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(10, "values",
            Types.ListType.ofOptional(11, Types.LongType.get()))
    );

    List<Long> values = ImmutableList.of(56L, 57L, 58L);

    Row row = Row.of(34L, values);

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("id_only", writeSchema, idOnly, row);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));
    Assert.assertEquals("Should not project values list", 1, projected.getArity());

    Schema elementOnly = writeSchema.select("values.element");
    projected = writeAndRead("element_only", writeSchema, elementOnly, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project entire list", values, projected.getField(0));

    Schema listOnly = writeSchema.select("values");
    projected = writeAndRead("list_only", writeSchema, listOnly, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project entire list", values, projected.getField(0));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListOfStructsProjection() throws IOException {
    Schema writeSchema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.required(19, "x", Types.IntegerType.get()),
                Types.NestedField.optional(18, "y", Types.IntegerType.get())
            ))
        )
    );

    Row p1 = Row.of(1, 2);
    Row p2 = Row.of(3, null);
    Row row = Row.of(34L, ImmutableList.of(p1, p2));

    Schema idOnly = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get())
    );

    Row projected = writeAndRead("id_only", writeSchema, idOnly, row);
    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));
    Assert.assertEquals("Should not project points list", 1, projected.getArity());

    projected = writeAndRead("all_points", writeSchema, writeSchema.select("points"), row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertEquals("Should project points list", row.getField(1), projected.getField(0));

    projected = writeAndRead("x_only", writeSchema, writeSchema.select("points.x"), row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project points list", projected.getField(0));
    List<Row> points = (List<Row>) projected.getField(0);
    Assert.assertEquals("Should read 2 points", 2, points.size());
    Row projectedP1 = points.get(0);
    Assert.assertEquals("Should project x", 1, (int) projectedP1.getField(0));
    Assert.assertEquals("Should not project y", 1, projectedP1.getArity());
    Row projectedP2 = points.get(1);
    Assert.assertEquals("Should not project y", 1, projectedP2.getArity());
    Assert.assertEquals("Should project x", 3, (int) projectedP2.getField(0));

    projected = writeAndRead("y_only", writeSchema, writeSchema.select("points.y"), row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project points list", projected.getField(0));
    points = (List<Row>) projected.getField(0);
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertEquals("Should not project x", 1, projectedP1.getArity());
    Assert.assertEquals("Should project y", 2, (int) projectedP1.getField(0));
    projectedP2 = points.get(1);
    Assert.assertEquals("Should not project x", 1, projectedP2.getArity());
    Assert.assertNull("Should project null y", projectedP2.getField(0));

    Schema yRenamed = new Schema(
        Types.NestedField.optional(22, "points",
            Types.ListType.ofOptional(21, Types.StructType.of(
                Types.NestedField.optional(18, "z", Types.IntegerType.get())
            ))
        )
    );

    projected = writeAndRead("y_renamed", writeSchema, yRenamed, row);
    Assert.assertEquals("Should not project id", 1, projected.getArity());
    Assert.assertNotNull("Should project points list", projected.getField(0));
    points = (List<Row>) projected.getField(0);
    Assert.assertEquals("Should read 2 points", 2, points.size());
    projectedP1 = points.get(0);
    Assert.assertEquals("Should not project x and y", 1, projectedP1.getArity());
    Assert.assertEquals("Should project z", 2, (int) projectedP1.getField(0));
    projectedP2 = points.get(1);
    Assert.assertEquals("Should not project x and y", 1, projectedP2.getArity());
    Assert.assertNull("Should project null z", projectedP2.getField(0));
  }

  @Test
  public void testAddedFieldsWithRequiredChildren() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(1, "a", Types.LongType.get())
    );

    Row row = Row.of(100L);

    Schema addedFields = new Schema(
        Types.NestedField.optional(1, "a", Types.LongType.get()),
        Types.NestedField.optional(2, "b", Types.StructType.of(
            Types.NestedField.required(3, "c", Types.LongType.get())
        )),
        Types.NestedField.optional(4, "d", Types.ListType.ofRequired(5, Types.LongType.get())),
        Types.NestedField.optional(6, "e", Types.MapType.ofRequired(7, 8, Types.LongType.get(), Types.LongType.get()))
    );

    Row projected = writeAndRead("add_fields_with_required_children_projection", schema, addedFields, row);
    Assert.assertEquals("Should contain the correct value in column 1", projected.getField(0), 100L);
    Assert.assertNull("Should contain empty value in new column 2", projected.getField(1));
    Assert.assertNull("Should contain empty value in new column 4", projected.getField(2));
    Assert.assertNull("Should contain empty value in new column 6", projected.getField(3));
  }
}
