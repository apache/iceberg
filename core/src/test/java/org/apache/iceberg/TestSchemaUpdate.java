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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Test;

public class TestSchemaUpdate {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(
              3,
              "preferences",
              Types.StructType.of(
                  required(8, "feature1", Types.BooleanType.get()),
                  optional(9, "feature2", Types.BooleanType.get())),
              "struct of named boolean options"),
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
                      required(23, "zip", Types.IntegerType.get())),
                  Types.StructType.of(
                      required(12, "lat", Types.FloatType.get()),
                      required(13, "long", Types.FloatType.get()))),
              "map of address to coordinate"),
          optional(
              5,
              "points",
              Types.ListType.ofOptional(
                  14,
                  Types.StructType.of(
                      required(15, "x", Types.LongType.get()),
                      required(16, "y", Types.LongType.get()))),
              "2-D cartesian points"),
          required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
          optional(
              7,
              "properties",
              Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
              "string map of properties"));

  private static final Set<Integer> ALL_IDS = ImmutableSet.copyOf(TypeUtil.getProjectedIds(SCHEMA));

  private static final int SCHEMA_LAST_COLUMN_ID = 23;

  @Test
  public void testNoChanges() {
    Schema identical = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).apply();
    Assert.assertEquals("Should not include any changes", SCHEMA.asStruct(), identical.asStruct());
  }

  @Test
  public void testDeleteFields() {
    // use schema projection to test column deletes
    List<String> columns =
        Lists.newArrayList(
            "id",
            "data",
            "preferences",
            "preferences.feature1",
            "preferences.feature2",
            "locations",
            "locations.lat",
            "locations.long",
            "points",
            "points.x",
            "points.y",
            "doubles",
            "properties");
    for (String name : columns) {
      Set<Integer> selected = Sets.newHashSet(ALL_IDS);
      // remove the id and any nested fields from the projection
      Types.NestedField nested = SCHEMA.findField(name);
      selected.remove(nested.fieldId());
      selected.removeAll(TypeUtil.getProjectedIds(nested.type()));

      Schema del = new SchemaUpdate(SCHEMA, 19).deleteColumn(name).apply();

      Assert.assertEquals(
          "Should match projection with '" + name + "' removed",
          TypeUtil.project(SCHEMA, selected).asStruct(),
          del.asStruct());
    }
  }

  @Test
  public void testDeleteFieldsCaseSensitiveDisabled() {
    // use schema projection to test column deletes
    List<String> columns =
        Lists.newArrayList(
            "Id",
            "Data",
            "Preferences",
            "Preferences.feature1",
            "Preferences.feature2",
            "Locations",
            "Locations.lat",
            "Locations.long",
            "Points",
            "Points.x",
            "Points.y",
            "Doubles",
            "Properties");
    for (String name : columns) {
      Set<Integer> selected = Sets.newHashSet(ALL_IDS);
      // remove the id and any nested fields from the projection
      Types.NestedField nested = SCHEMA.caseInsensitiveFindField(name);
      selected.remove(nested.fieldId());
      selected.removeAll(TypeUtil.getProjectedIds(nested.type()));

      Schema del = new SchemaUpdate(SCHEMA, 19).caseSensitive(false).deleteColumn(name).apply();

      Assert.assertEquals(
          "Should match projection with '" + name + "' removed",
          TypeUtil.project(SCHEMA, selected).asStruct(),
          del.asStruct());
    }
  }

  @Test
  public void testUpdateTypes() {
    Types.StructType expected =
        Types.StructType.of(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "feature2", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "lat", Types.DoubleType.get()),
                        required(13, "long", Types.DoubleType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));

    Schema updated =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .updateColumn("id", Types.LongType.get())
            .updateColumn("locations.lat", Types.DoubleType.get())
            .updateColumn("locations.long", Types.DoubleType.get())
            .apply();

    Assert.assertEquals("Should convert types", expected, updated.asStruct());
  }

  @Test
  public void testUpdateTypesCaseInsensitive() {
    Types.StructType expected =
        Types.StructType.of(
            required(1, "id", Types.LongType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "feature2", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "lat", Types.DoubleType.get()),
                        required(13, "long", Types.DoubleType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));

    Schema updated =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .updateColumn("ID", Types.LongType.get())
            .updateColumn("Locations.Lat", Types.DoubleType.get())
            .updateColumn("Locations.Long", Types.DoubleType.get())
            .apply();

    Assert.assertEquals("Should convert types", expected, updated.asStruct());
  }

  @Test
  public void testUpdateFailure() {
    Set<Pair<Type.PrimitiveType, Type.PrimitiveType>> allowedUpdates =
        Sets.newHashSet(
            Pair.of(Types.IntegerType.get(), Types.LongType.get()),
            Pair.of(Types.FloatType.get(), Types.DoubleType.get()),
            Pair.of(Types.DecimalType.of(9, 2), Types.DecimalType.of(18, 2)));

    List<Type.PrimitiveType> primitives =
        Lists.newArrayList(
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
            Types.BinaryType.get(),
            Types.FixedType.ofLength(3),
            Types.FixedType.ofLength(4),
            Types.DecimalType.of(9, 2),
            Types.DecimalType.of(9, 3),
            Types.DecimalType.of(18, 2));

    for (Type.PrimitiveType fromType : primitives) {
      for (Type.PrimitiveType toType : primitives) {
        Schema fromSchema = new Schema(required(1, "col", fromType));

        if (fromType.equals(toType) || allowedUpdates.contains(Pair.of(fromType, toType))) {
          Schema expected = new Schema(required(1, "col", toType));
          Schema result = new SchemaUpdate(fromSchema, 1).updateColumn("col", toType).apply();
          Assert.assertEquals("Should allow update", expected.asStruct(), result.asStruct());
          continue;
        }

        String typeChange = fromType.toString() + " -> " + toType.toString();
        Assertions.assertThatThrownBy(
                () -> new SchemaUpdate(fromSchema, 1).updateColumn("col", toType))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot change column type: col: " + typeChange);
      }
    }
  }

  @Test
  public void testRename() {
    Types.StructType expected =
        Types.StructType.of(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "json", Types.StringType.get()),
            optional(
                3,
                "options",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "newfeature", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "latitude", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "X", Types.LongType.get()),
                        required(16, "y.y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));

    Schema renamed =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .renameColumn("data", "json")
            .renameColumn("preferences", "options")
            .renameColumn("preferences.feature2", "newfeature") // inside a renamed column
            .renameColumn("locations.lat", "latitude")
            .renameColumn("points.x", "X")
            .renameColumn("points.y", "y.y") // has a '.' in the field name
            .apply();

    Assert.assertEquals("Should rename all fields", expected, renamed.asStruct());
  }

  @Test
  public void testRenameCaseInsensitive() {
    Types.StructType expected =
        Types.StructType.of(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "json", Types.StringType.get()),
            optional(
                3,
                "options",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "newfeature", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "latitude", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "X", Types.LongType.get()),
                        required(16, "y.y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));

    Schema renamed =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .renameColumn("Data", "json")
            .renameColumn("Preferences", "options")
            .renameColumn("Preferences.Feature2", "newfeature") // inside a renamed column
            .renameColumn("Locations.Lat", "latitude")
            .renameColumn("Points.x", "X")
            .renameColumn("Points.y", "y.y") // has a '.' in the field name
            .apply();

    Assert.assertEquals("Should rename all fields", expected, renamed.asStruct());
  }

  @Test
  public void testAddFields() {
    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "feature2", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "lat", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get()),
                        optional(25, "alt", Types.FloatType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get()),
                        optional(26, "z", Types.LongType.get()),
                        optional(27, "t.t", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"),
            optional(24, "toplevel", Types.DecimalType.of(9, 2)));

    Schema added =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .addColumn("toplevel", Types.DecimalType.of(9, 2))
            .addColumn("locations", "alt", Types.FloatType.get()) // map of structs
            .addColumn("points", "z", Types.LongType.get()) // list of structs
            .addColumn("points", "t.t", Types.LongType.get()) // name with '.'
            .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), added.asStruct());
  }

  @Test
  public void testAddNestedStruct() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.StructType struct =
        Types.StructType.of(
            required(1, "lat", Types.IntegerType.get()), // conflicts with id
            optional(2, "long", Types.IntegerType.get()));

    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(
                2,
                "location",
                Types.StructType.of(
                    required(3, "lat", Types.IntegerType.get()),
                    optional(4, "long", Types.IntegerType.get()))));

    Schema result = new SchemaUpdate(schema, 1).addColumn("location", struct).apply();

    Assert.assertEquals(
        "Should add struct and reassign column IDs", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedMapOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.MapType map =
        Types.MapType.ofOptional(
            1,
            2,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())),
            Types.StructType.of(
                required(9, "lat", Types.IntegerType.get()),
                optional(8, "long", Types.IntegerType.get())));

    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(
                2,
                "locations",
                Types.MapType.ofOptional(
                    3,
                    4,
                    Types.StructType.of(
                        required(5, "address", Types.StringType.get()),
                        required(6, "city", Types.StringType.get()),
                        required(7, "state", Types.StringType.get()),
                        required(8, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(9, "lat", Types.IntegerType.get()),
                        optional(10, "long", Types.IntegerType.get())))));

    Schema result = new SchemaUpdate(schema, 1).addColumn("locations", map).apply();

    Assert.assertEquals(
        "Should add map and reassign column IDs", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedListOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.ListType list =
        Types.ListType.ofOptional(
            1,
            Types.StructType.of(
                required(9, "lat", Types.IntegerType.get()),
                optional(8, "long", Types.IntegerType.get())));

    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(
                2,
                "locations",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        required(4, "lat", Types.IntegerType.get()),
                        optional(5, "long", Types.IntegerType.get())))));

    Schema result = new SchemaUpdate(schema, 1).addColumn("locations", list).apply();

    Assert.assertEquals(
        "Should add map and reassign column IDs", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddRequiredColumn() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            required(2, "data", Types.StringType.get()));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 1).addRequiredColumn("data", Types.StringType.get()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Incompatible change: cannot add required column: data");

    Schema result =
        new SchemaUpdate(schema, 1)
            .allowIncompatibleChanges()
            .addRequiredColumn("data", Types.StringType.get())
            .apply();

    Assert.assertEquals("Should add required column", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddRequiredColumnCaseInsensitive() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 1)
                    .caseSensitive(false)
                    .allowIncompatibleChanges()
                    .addRequiredColumn("ID", Types.StringType.get())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add column, name already exists: ID");
  }

  @Test
  public void testMakeColumnOptional() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(optional(1, "id", Types.IntegerType.get()));

    Schema result = new SchemaUpdate(schema, 1).makeColumnOptional("id").apply();

    Assert.assertEquals(
        "Should update column to be optional", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testRequireColumn() {
    Schema schema = new Schema(optional(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(required(1, "id", Types.IntegerType.get()));

    Assertions.assertThatThrownBy(() -> new SchemaUpdate(schema, 1).requireColumn("id"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot change column nullability: id: optional -> required");

    // required to required is not an incompatible change
    new SchemaUpdate(expected, 1).requireColumn("id").apply();

    Schema result =
        new SchemaUpdate(schema, 1).allowIncompatibleChanges().requireColumn("id").apply();

    Assert.assertEquals(
        "Should update column to be required", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testRequireColumnCaseInsensitive() {
    Schema schema = new Schema(optional(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(required(1, "id", Types.IntegerType.get()));

    Schema result =
        new SchemaUpdate(schema, 1)
            .caseSensitive(false)
            .allowIncompatibleChanges()
            .requireColumn("ID")
            .apply();

    Assert.assertEquals(
        "Should update column to be required", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testMixedChanges() {
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get(), "unique id"),
            required(2, "json", Types.StringType.get()),
            optional(
                3,
                "options",
                Types.StructType.of(
                    required(8, "feature1", Types.BooleanType.get()),
                    optional(9, "newfeature", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "latitude", Types.DoubleType.get(), "latitude"),
                        optional(25, "alt", Types.FloatType.get()),
                        required(
                            28, "description", Types.StringType.get(), "Location description"))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        optional(15, "X", Types.LongType.get()),
                        required(16, "y.y", Types.LongType.get()),
                        optional(26, "z", Types.LongType.get()),
                        optional(27, "t.t", Types.LongType.get(), "name with '.'"))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(24, "toplevel", Types.DecimalType.of(9, 2)));

    Schema updated =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .addColumn("toplevel", Types.DecimalType.of(9, 2))
            .addColumn("locations", "alt", Types.FloatType.get()) // map of structs
            .addColumn("points", "z", Types.LongType.get()) // list of structs
            .addColumn("points", "t.t", Types.LongType.get(), "name with '.'")
            .renameColumn("data", "json")
            .renameColumn("preferences", "options")
            .renameColumn("preferences.feature2", "newfeature") // inside a renamed column
            .renameColumn("locations.lat", "latitude")
            .renameColumn("points.x", "X")
            .renameColumn("points.y", "y.y") // has a '.' in the field name
            .updateColumn("id", Types.LongType.get(), "unique id")
            .updateColumn("locations.lat", Types.DoubleType.get()) // use the original name
            .updateColumnDoc("locations.lat", "latitude")
            .deleteColumn("locations.long")
            .deleteColumn("properties")
            .makeColumnOptional("points.x")
            .allowIncompatibleChanges()
            .requireColumn("data")
            .addRequiredColumn(
                "locations", "description", Types.StringType.get(), "Location description")
            .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), updated.asStruct());
  }

  @Test
  public void testAmbiguousAdd() {
    // preferences.booleans could be top-level or a field of preferences
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.addColumn("preferences.booleans", Types.BooleanType.get());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add column with ambiguous name: preferences.booleans");
  }

  @Test
  public void testAddAlreadyExists() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.addColumn("preferences", "feature1", Types.BooleanType.get());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add column, name already exists: preferences.feature1");

    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.addColumn("preferences", Types.BooleanType.get());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add column, name already exists: preferences");
  }

  @Test
  public void testDeleteThenAdd() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(optional(2, "id", Types.IntegerType.get()));

    Schema updated =
        new SchemaUpdate(schema, 1)
            .deleteColumn("id")
            .addColumn("id", optional(2, "id", Types.IntegerType.get()).type())
            .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), updated.asStruct());
  }

  @Test
  public void testDeleteThenAddNested() {
    Schema expectedNested =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "data", Types.StringType.get()),
            optional(
                3,
                "preferences",
                Types.StructType.of(
                    optional(9, "feature2", Types.BooleanType.get()),
                    optional(24, "feature1", Types.BooleanType.get())),
                "struct of named boolean options"),
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
                        required(23, "zip", Types.IntegerType.get())),
                    Types.StructType.of(
                        required(12, "lat", Types.FloatType.get()),
                        required(13, "long", Types.FloatType.get()))),
                "map of address to coordinate"),
            optional(
                5,
                "points",
                Types.ListType.ofOptional(
                    14,
                    Types.StructType.of(
                        required(15, "x", Types.LongType.get()),
                        required(16, "y", Types.LongType.get()))),
                "2-D cartesian points"),
            required(6, "doubles", Types.ListType.ofRequired(17, Types.DoubleType.get())),
            optional(
                7,
                "properties",
                Types.MapType.ofOptional(18, 19, Types.StringType.get(), Types.StringType.get()),
                "string map of properties"));

    Schema updatedNested =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .deleteColumn("preferences.feature1")
            .addColumn("preferences", "feature1", Types.BooleanType.get())
            .apply();

    Assert.assertEquals(
        "Should match with added fields", expectedNested.asStruct(), updatedNested.asStruct());
  }

  @Test
  public void testDeleteMissingColumn() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.deleteColumn("col");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete missing column: col");
  }

  @Test
  public void testAddDeleteConflict() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.addColumn("col", Types.IntegerType.get()).deleteColumn("col");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete missing column: col");

    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update
                  .addColumn("preferences", "feature3", Types.IntegerType.get())
                  .deleteColumn("preferences");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete a column that has additions: preferences");
  }

  @Test
  public void testRenameMissingColumn() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.renameColumn("col", "fail");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename missing column: col");
  }

  @Test
  public void testRenameDeleteConflict() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.renameColumn("id", "col").deleteColumn("id");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete a column that has updates: id");

    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.renameColumn("id", "col").deleteColumn("col");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete missing column: col");
  }

  @Test
  public void testDeleteRenameConflict() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.deleteColumn("id").renameColumn("id", "identifier");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot rename a column that will be deleted: id");
  }

  @Test
  public void testUpdateMissingColumn() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.updateColumn("col", Types.DateType.get());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot update missing column: col");
  }

  @Test
  public void testUpdateDeleteConflict() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.updateColumn("id", Types.LongType.get()).deleteColumn("id");
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot delete a column that has updates: id");
  }

  @Test
  public void testDeleteUpdateConflict() {
    Assertions.assertThatThrownBy(
            () -> {
              UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
              update.deleteColumn("id").updateColumn("id", Types.LongType.get());
            })
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot update a column that will be deleted: id");
  }

  @Test
  public void testDeleteMapKey() {
    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .deleteColumn("locations.key")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot delete map keys");
  }

  @Test
  public void testAddFieldToMapKey() {
    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .addColumn("locations.key", "address_line_2", Types.StringType.get())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot add fields to map keys");
  }

  @Test
  public void testAlterMapKey() {
    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .updateColumn("locations.key.zip", Types.LongType.get())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot alter map keys");
  }

  @Test
  public void testUpdateMapKey() {
    Schema schema =
        new Schema(
            required(
                1,
                "m",
                Types.MapType.ofOptional(2, 3, Types.IntegerType.get(), Types.DoubleType.get())));
    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 3).updateColumn("m.key", Types.LongType.get()).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot update map keys: map<int, double>");
  }

  @Test
  public void testUpdateAddedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 3)
                    .addColumn("value", Types.LongType.get())
                    .updateColumnDoc("value", "a value")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot update missing column: value");
  }

  @Test
  public void testUpdateDeletedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 3)
                    .deleteColumn("i")
                    .updateColumnDoc("i", "a value")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot update a column that will be deleted: i");
  }

  @Test
  public void testMultipleMoves() {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.IntegerType.get()),
            required(3, "c", Types.IntegerType.get()),
            required(4, "d", Types.IntegerType.get()));

    Schema expected =
        new Schema(
            required(3, "c", Types.IntegerType.get()),
            required(2, "b", Types.IntegerType.get()),
            required(4, "d", Types.IntegerType.get()),
            required(1, "a", Types.IntegerType.get()));

    // moves are applied in order
    Schema actual =
        new SchemaUpdate(schema, 4)
            .moveFirst("d")
            .moveFirst("c")
            .moveAfter("b", "d")
            .moveBefore("d", "a")
            .apply();

    Assert.assertEquals("Schema should match", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnFirst() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(2, "data", Types.StringType.get()), required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2).moveFirst("data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnBeforeFirst() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(2, "data", Types.StringType.get()), required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2).moveBefore("data", "id").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnAfterLast() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(2, "data", Types.StringType.get()), required(1, "id", Types.LongType.get()));

    Schema actual = new SchemaUpdate(schema, 2).moveAfter("id", "data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnAfter() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "ts", Types.TimestampType.withZone()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "ts", Types.TimestampType.withZone()),
            required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 3).moveAfter("ts", "id").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveTopLevelColumnBefore() {
    Schema schema =
        new Schema(
            optional(3, "ts", Types.TimestampType.withZone()),
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "ts", Types.TimestampType.withZone()),
            required(2, "data", Types.StringType.get()));

    Schema actual = new SchemaUpdate(schema, 3).moveBefore("ts", "data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldFirst() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveFirst("struct.data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldBeforeFirst() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveBefore("struct.data", "struct.count").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldAfterLast() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(4, "data", Types.StringType.get()),
                    required(3, "count", Types.LongType.get()))));

    Schema actual = new SchemaUpdate(schema, 4).moveAfter("struct.count", "struct.data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldAfter() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5).moveAfter("struct.ts", "struct.count").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveNestedFieldBefore() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual = new SchemaUpdate(schema, 5).moveBefore("struct.ts", "struct.data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveListElementField() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "list",
                Types.ListType.ofOptional(
                    6,
                    Types.StructType.of(
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(3, "count", Types.LongType.get()),
                        required(4, "data", Types.StringType.get())))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "list",
                Types.ListType.ofOptional(
                    6,
                    Types.StructType.of(
                        required(3, "count", Types.LongType.get()),
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 6).moveBefore("list.ts", "list.data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveMapValueStructField() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "map",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(3, "count", Types.LongType.get()),
                        required(4, "data", Types.StringType.get())))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "map",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.StringType.get(),
                    Types.StructType.of(
                        required(3, "count", Types.LongType.get()),
                        optional(5, "ts", Types.TimestampType.withZone()),
                        required(4, "data", Types.StringType.get())))));

    Schema actual = new SchemaUpdate(schema, 7).moveBefore("map.ts", "map.data").apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedTopLevelColumn() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "ts", Types.TimestampType.withZone()),
            required(2, "data", Types.StringType.get()));

    Schema actual =
        new SchemaUpdate(schema, 2)
            .addColumn("ts", Types.TimestampType.withZone())
            .moveAfter("ts", "id")
            .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedTopLevelColumnAfterAddedColumn() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(3, "ts", Types.TimestampType.withZone()),
            optional(4, "count", Types.LongType.get()),
            required(2, "data", Types.StringType.get()));

    Schema actual =
        new SchemaUpdate(schema, 2)
            .addColumn("ts", Types.TimestampType.withZone())
            .addColumn("count", Types.LongType.get())
            .moveAfter("ts", "id")
            .moveAfter("count", "ts")
            .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedNestedStructField() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 4)
            .addColumn("struct", "ts", Types.TimestampType.withZone())
            .moveBefore("struct.ts", "struct.count")
            .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveAddedNestedStructFieldBeforeAddedColumn() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));
    Schema expected =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(
                2,
                "struct",
                Types.StructType.of(
                    optional(6, "size", Types.LongType.get()),
                    optional(5, "ts", Types.TimestampType.withZone()),
                    required(3, "count", Types.LongType.get()),
                    required(4, "data", Types.StringType.get()))));

    Schema actual =
        new SchemaUpdate(schema, 4)
            .addColumn("struct", "ts", Types.TimestampType.withZone())
            .addColumn("struct", "size", Types.LongType.get())
            .moveBefore("struct.ts", "struct.count")
            .moveBefore("struct.size", "struct.ts")
            .apply();

    Assert.assertEquals("Should move data first", expected.asStruct(), actual.asStruct());
  }

  @Test
  public void testMoveSelfReferenceFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    Assertions.assertThatThrownBy(() -> new SchemaUpdate(schema, 2).moveBefore("id", "id").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move id before itself");

    Assertions.assertThatThrownBy(() -> new SchemaUpdate(schema, 2).moveAfter("id", "id").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move id after itself");
  }

  @Test
  public void testMoveMissingColumnFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    Assertions.assertThatThrownBy(() -> new SchemaUpdate(schema, 2).moveFirst("items").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: items");

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 2).moveBefore("items", "id").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: items");

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 2).moveAfter("items", "data").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: items");
  }

  @Test
  public void testMoveBeforeAddFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 2)
                    .moveFirst("ts")
                    .addColumn("ts", Types.TimestampType.withZone())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: ts");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 2)
                    .moveBefore("ts", "id")
                    .addColumn("ts", Types.TimestampType.withZone())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: ts");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schema, 2)
                    .moveAfter("ts", "data")
                    .addColumn("ts", Types.TimestampType.withZone())
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move missing column: ts");
  }

  @Test
  public void testMoveMissingReferenceColumnFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), required(2, "data", Types.StringType.get()));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 2).moveBefore("id", "items").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move id before missing column: items");

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 2).moveAfter("data", "items").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move data after missing column: items");
  }

  @Test
  public void testMovePrimitiveMapKeyFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(
                3,
                "map",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StringType.get())));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 5).moveBefore("map.key", "map.value").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: map<string, string>");
  }

  @Test
  public void testMovePrimitiveMapValueFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(
                3,
                "map",
                Types.MapType.ofRequired(4, 5, Types.StringType.get(), Types.StructType.of())));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 5).moveBefore("map.value", "map.key").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: map<string, struct<>>");
  }

  @Test
  public void testMovePrimitiveListElementFails() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            required(2, "data", Types.StringType.get()),
            optional(3, "list", Types.ListType.ofRequired(4, Types.StringType.get())));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 4).moveBefore("list.element", "list").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move fields in non-struct type: list<string>");
  }

  @Test
  public void testMoveTopLevelBetweenStructsFails() {
    Schema schema =
        new Schema(
            required(1, "a", Types.IntegerType.get()),
            required(2, "b", Types.IntegerType.get()),
            required(
                3,
                "struct",
                Types.StructType.of(
                    required(4, "x", Types.IntegerType.get()),
                    required(5, "y", Types.IntegerType.get()))));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 5).moveBefore("a", "struct.x").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move field a to a different struct");
  }

  @Test
  public void testMoveBetweenStructsFails() {
    Schema schema =
        new Schema(
            required(
                1,
                "s1",
                Types.StructType.of(
                    required(3, "a", Types.IntegerType.get()),
                    required(4, "b", Types.IntegerType.get()))),
            required(
                2,
                "s2",
                Types.StructType.of(
                    required(5, "x", Types.IntegerType.get()),
                    required(6, "y", Types.IntegerType.get()))));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(schema, 6).moveBefore("s2.x", "s1.a").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot move field s2.x to a different struct");
  }

  @Test
  public void testAddExistingIdentifierFields() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Assert.assertEquals(
        "add an existing field as identifier field should succeed",
        Sets.newHashSet(newSchema.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddNewIdentifierFieldColumns() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("new_field", Types.StringType.get())
            .setIdentifierFields("id", "new_field")
            .apply();

    Assert.assertEquals(
        "add column then set as identifier should succeed",
        Sets.newHashSet(
            newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .setIdentifierFields("id", "new_field")
            .addRequiredColumn("new_field", Types.StringType.get())
            .apply();

    Assert.assertEquals(
        "set identifier then add column should succeed",
        Sets.newHashSet(
            newSchema.findField("id").fieldId(), newSchema.findField("new_field").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddNestedIdentifierFieldColumns() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "required_struct",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "field", Types.StringType.get())))
            .apply();

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID + 2)
            .setIdentifierFields("required_struct.field")
            .apply();

    Assert.assertEquals(
        "set existing nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("required_struct.field").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "field", Types.StringType.get())))
            .setIdentifierFields("new.field")
            .apply();

    Assert.assertEquals(
        "set newly added nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("new.field").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2,
                        "field",
                        Types.StructType.of(
                            Types.NestedField.required(
                                SCHEMA_LAST_COLUMN_ID + 3, "nested", Types.StringType.get())))))
            .setIdentifierFields("new.field.nested")
            .apply();

    Assert.assertEquals(
        "set newly added multi-layer nested field as identifier should succeed",
        Sets.newHashSet(newSchema.findField("new.field.nested").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testAddDottedIdentifierFieldColumns() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(null, "dot.field", Types.StringType.get())
            .setIdentifierFields("id", "dot.field")
            .apply();

    Assert.assertEquals(
        "add a field with dot as identifier should succeed",
        Sets.newHashSet(
            newSchema.findField("id").fieldId(), newSchema.findField("dot.field").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testRemoveIdentifierFields() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("new_field", Types.StringType.get())
            .addRequiredColumn("new_field2", Types.StringType.get())
            .setIdentifierFields("id", "new_field", "new_field2")
            .apply();

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields("new_field", "new_field2")
            .apply();

    Assert.assertEquals(
        "remove an identifier field should succeed",
        Sets.newHashSet(
            newSchema.findField("new_field").fieldId(),
            newSchema.findField("new_field2").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields(Sets.newHashSet())
            .apply();

    Assert.assertEquals(
        "remove all identifier fields should succeed",
        Sets.newHashSet(),
        newSchema.identifierFieldIds());
  }

  @SuppressWarnings("MethodLength")
  @Test
  public void testSetIdentifierFieldsFails() {
    Schema testSchema =
        new Schema(
            optional(1, "id", Types.IntegerType.get()),
            required(2, "float", Types.FloatType.get()),
            required(3, "double", Types.DoubleType.get()));

    Assertions.assertThatThrownBy(
            () -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(999)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add fieldId 999 as an identifier field: field does not exist");

    Assertions.assertThatThrownBy(
            () -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add field id as an identifier field: not a required field");

    Assertions.assertThatThrownBy(
            () -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(2)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field float as an identifier field: must not be float or double field");

    Assertions.assertThatThrownBy(
            () -> new Schema(testSchema.asStruct().fields(), ImmutableSet.of(3)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field double as an identifier field: must not be float or double field");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("unknown")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field unknown as an identifier field: not found in current schema or added columns");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("locations")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field locations as an identifier field: not a primitive type field");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("data").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot add field data as an identifier field: not a required field");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("locations.key.zip")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field zip as an identifier field: must not be nested in "
                + SCHEMA.findField("locations"));

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
                    .setIdentifierFields("points.element.x")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field x as an identifier field: must not be nested in "
                + SCHEMA.findField("points"));

    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn("col_float", Types.FloatType.get())
            .addRequiredColumn("col_double", Types.DoubleType.get())
            .addRequiredColumn(
                "new",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 4,
                        "fields",
                        Types.ListType.ofRequired(
                            SCHEMA_LAST_COLUMN_ID + 5,
                            Types.StructType.of(
                                Types.NestedField.required(
                                    SCHEMA_LAST_COLUMN_ID + 6,
                                    "nested",
                                    Types.StringType.get()))))))
            .addRequiredColumn(
                "new_map",
                Types.MapType.ofRequired(
                    SCHEMA_LAST_COLUMN_ID + 8,
                    SCHEMA_LAST_COLUMN_ID + 9,
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 10, "key_col", Types.StringType.get())),
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 11, "val_col", Types.StringType.get()))),
                "map of address to coordinate")
            .addRequiredColumn(
                "required_list",
                Types.ListType.ofRequired(
                    SCHEMA_LAST_COLUMN_ID + 13,
                    Types.StructType.of(
                        required(SCHEMA_LAST_COLUMN_ID + 14, "x", Types.LongType.get()),
                        required(SCHEMA_LAST_COLUMN_ID + 15, "y", Types.LongType.get()))))
            .apply();

    int lastColId = SCHEMA_LAST_COLUMN_ID + 15;

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("required_list.element.x")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field x as an identifier field: must not be nested in "
                + newSchema.findField("required_list"));

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(newSchema, lastColId).setIdentifierFields("col_double").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field col_double as an identifier field: must not be float or double field");

    Assertions.assertThatThrownBy(
            () -> new SchemaUpdate(newSchema, lastColId).setIdentifierFields("col_float").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field col_float as an identifier field: must not be float or double field");

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("new_map.value.val_col")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field val_col as an identifier field: must not be nested in "
                + newSchema.findField("new_map"));

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("new.fields.element.nested")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith(
            "Cannot add field nested as an identifier field: must not be nested in "
                + newSchema.findField("new.fields"));

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, lastColId)
                    .setIdentifierFields("preferences.feature1")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot add field feature1 as an identifier field: must not be nested in an optional field "
                + newSchema.findField("preferences"));
  }

  @Test
  public void testDeleteIdentifierFieldColumns() {
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Assert.assertEquals(
        "delete column and then reset identifier field should succeed",
        Sets.newHashSet(),
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .deleteColumn("id")
            .setIdentifierFields(Sets.newHashSet())
            .apply()
            .identifierFieldIds());

    Assert.assertEquals(
        "delete reset identifier field and then delete column should succeed",
        Sets.newHashSet(),
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .setIdentifierFields(Sets.newHashSet())
            .deleteColumn("id")
            .apply()
            .identifierFieldIds());
  }

  @Test
  public void testDeleteIdentifierFieldColumnsFails() {
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
                    .deleteColumn("id")
                    .apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot delete identifier field 1: id: required int. To force deletion, also call setIdentifierFields to update identifier fields.");
  }

  @Test
  public void testDeleteContainingNestedIdentifierFieldColumnsFails() {
    Schema newSchema =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
            .allowIncompatibleChanges()
            .addRequiredColumn(
                "out",
                Types.StructType.of(
                    Types.NestedField.required(
                        SCHEMA_LAST_COLUMN_ID + 2, "nested", Types.StringType.get())))
            .setIdentifierFields("out.nested")
            .apply();

    Assertions.assertThatThrownBy(
            () ->
                new SchemaUpdate(newSchema, SCHEMA_LAST_COLUMN_ID + 2).deleteColumn("out").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Cannot delete field 24: out: required struct<25: nested: required string> "
                + "as it will delete nested identifier field 25: nested: required string");
  }

  @Test
  public void testRenameIdentifierFields() {
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .renameColumn("id", "id2")
            .apply();

    Assert.assertEquals(
        "rename should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testMoveIdentifierFields() {
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .moveAfter("id", "locations")
            .apply();

    Assert.assertEquals(
        "move after should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .moveBefore("id", "locations")
            .apply();

    Assert.assertEquals(
        "move before should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID).moveFirst("id").apply();

    Assert.assertEquals(
        "move first should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }

  @Test
  public void testMoveIdentifierFieldsCaseInsensitive() {
    Schema schemaWithIdentifierFields =
        new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).setIdentifierFields("id").apply();

    Schema newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveAfter("iD", "locations")
            .apply();

    Assert.assertEquals(
        "move after should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveBefore("ID", "locations")
            .apply();

    Assert.assertEquals(
        "move before should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());

    newSchema =
        new SchemaUpdate(schemaWithIdentifierFields, SCHEMA_LAST_COLUMN_ID)
            .caseSensitive(false)
            .moveFirst("ID")
            .apply();

    Assert.assertEquals(
        "move first should not affect identifier fields",
        Sets.newHashSet(SCHEMA.findField("id").fieldId()),
        newSchema.identifierFieldIds());
  }
}
