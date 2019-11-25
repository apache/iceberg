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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSchemaUpdate {
  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()),
      optional(3, "preferences", Types.StructType.of(
          required(8, "feature1", Types.BooleanType.get()),
          optional(9, "feature2", Types.BooleanType.get())
      ), "struct of named boolean options"),
      required(4, "locations", Types.MapType.ofRequired(10, 11,
          Types.StructType.of(
              required(20, "address", Types.StringType.get()),
              required(21, "city", Types.StringType.get()),
              required(22, "state", Types.StringType.get()),
              required(23, "zip", Types.IntegerType.get())
          ),
          Types.StructType.of(
              required(12, "lat", Types.FloatType.get()),
              required(13, "long", Types.FloatType.get())
          )), "map of address to coordinate"),
      optional(5, "points", Types.ListType.ofOptional(14,
          Types.StructType.of(
              required(15, "x", Types.LongType.get()),
              required(16, "y", Types.LongType.get())
          )), "2-D cartesian points"),
      required(6, "doubles", Types.ListType.ofRequired(17,
          Types.DoubleType.get()
      )),
      optional(7, "properties", Types.MapType.ofOptional(18, 19,
          Types.StringType.get(),
          Types.StringType.get()
      ), "string map of properties")
  );

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
    List<String> columns = Lists.newArrayList("id", "data", "preferences", "preferences.feature1",
        "preferences.feature2", "locations", "locations.lat", "locations.long", "points",
        "points.x", "points.y", "doubles", "properties");
    for (String name : columns) {
      Set<Integer> selected = Sets.newHashSet(ALL_IDS);
      // remove the id and any nested fields from the projection
      Types.NestedField nested = SCHEMA.findField(name);
      selected.remove(nested.fieldId());
      selected.removeAll(TypeUtil.getProjectedIds(nested.type()));

      Schema del = new SchemaUpdate(SCHEMA, 19).deleteColumn(name).apply();

      Assert.assertEquals("Should match projection with '" + name + "' removed",
          TypeUtil.select(SCHEMA, selected).asStruct(), del.asStruct());
    }
  }

  @Test
  public void testUpdateTypes() {
    Types.StructType expected = Types.StructType.of(
        required(1, "id", Types.LongType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "feature2", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "lat", Types.DoubleType.get()),
                required(13, "long", Types.DoubleType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "x", Types.LongType.get()),
                required(16, "y", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );

    Schema updated = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
        .updateColumn("id", Types.LongType.get())
        .updateColumn("locations.lat", Types.DoubleType.get())
        .updateColumn("locations.long", Types.DoubleType.get())
        .apply();

    Assert.assertEquals("Should convert types", expected, updated.asStruct());
  }

  @Test
  public void testUpdateFailure() {
    Set<Pair<Type.PrimitiveType, Type.PrimitiveType>> allowedUpdates = Sets.newHashSet(
        Pair.of(Types.IntegerType.get(), Types.LongType.get()),
        Pair.of(Types.FloatType.get(), Types.DoubleType.get()),
        Pair.of(Types.DecimalType.of(9, 2), Types.DecimalType.of(18, 2))
    );

    List<Type.PrimitiveType> primitives = Lists.newArrayList(
        Types.BooleanType.get(), Types.IntegerType.get(), Types.LongType.get(),
        Types.FloatType.get(), Types.DoubleType.get(), Types.DateType.get(), Types.TimeType.get(),
        Types.TimestampType.withZone(), Types.TimestampType.withoutZone(),
        Types.StringType.get(), Types.UUIDType.get(), Types.BinaryType.get(),
        Types.FixedType.ofLength(3), Types.FixedType.ofLength(4),
        Types.DecimalType.of(9, 2), Types.DecimalType.of(9, 3),
        Types.DecimalType.of(18, 2)
    );

    for (Type.PrimitiveType fromType : primitives) {
      for (Type.PrimitiveType toType : primitives) {
        Schema fromSchema = new Schema(required(1, "col", fromType));

        if (fromType.equals(toType) ||
            allowedUpdates.contains(Pair.of(fromType, toType))) {
          Schema expected = new Schema(required(1, "col", toType));
          Schema result = new SchemaUpdate(fromSchema, 1).updateColumn("col", toType).apply();
          Assert.assertEquals("Should allow update", expected.asStruct(), result.asStruct());
          continue;
        }

        String typeChange = fromType.toString() + " -> " + toType.toString();
        AssertHelpers.assertThrows("Should reject update: " + typeChange,
            IllegalArgumentException.class, "change column type: col: " + typeChange,
            () -> new SchemaUpdate(fromSchema, 1).updateColumn("col", toType));
      }
    }
  }

  @Test
  public void testRename() {
    Types.StructType expected = Types.StructType.of(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "json", Types.StringType.get()),
        optional(3, "options", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "newfeature", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "latitude", Types.FloatType.get()),
                required(13, "long", Types.FloatType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "X", Types.LongType.get()),
                required(16, "y.y", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties")
    );

    Schema renamed = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
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
  public void testAddFields() {
    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "preferences", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "feature2", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "lat", Types.FloatType.get()),
                required(13, "long", Types.FloatType.get()),
                optional(25, "alt", Types.FloatType.get())
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                required(15, "x", Types.LongType.get()),
                required(16, "y", Types.LongType.get()),
                optional(26, "z", Types.LongType.get()),
                optional(27, "t.t", Types.LongType.get())
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(7, "properties", Types.MapType.ofOptional(18, 19,
            Types.StringType.get(),
            Types.StringType.get()
        ), "string map of properties"),
        optional(24, "toplevel", Types.DecimalType.of(9, 2))
    );

    Schema added = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
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
    Types.StructType struct = Types.StructType.of(
        required(1, "lat", Types.IntegerType.get()), // conflicts with id
        optional(2, "long", Types.IntegerType.get())
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "location", Types.StructType.of(
            required(3, "lat", Types.IntegerType.get()),
            optional(4, "long", Types.IntegerType.get())
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("location", struct)
        .apply();

    Assert.assertEquals("Should add struct and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedMapOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.MapType map = Types.MapType.ofOptional(1, 2,
        Types.StructType.of(
            required(20, "address", Types.StringType.get()),
            required(21, "city", Types.StringType.get()),
            required(22, "state", Types.StringType.get()),
            required(23, "zip", Types.IntegerType.get())
        ),
        Types.StructType.of(
            required(9, "lat", Types.IntegerType.get()),
            optional(8, "long", Types.IntegerType.get())
        )
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "locations", Types.MapType.ofOptional(3, 4,
            Types.StructType.of(
                required(5, "address", Types.StringType.get()),
                required(6, "city", Types.StringType.get()),
                required(7, "state", Types.StringType.get()),
                required(8, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(9, "lat", Types.IntegerType.get()),
                optional(10, "long", Types.IntegerType.get())
            )
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("locations", map)
        .apply();

    Assert.assertEquals("Should add map and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddNestedListOfStructs() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Types.ListType list = Types.ListType.ofOptional(1,
        Types.StructType.of(
            required(9, "lat", Types.IntegerType.get()),
            optional(8, "long", Types.IntegerType.get())
        )
    );

    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "locations", Types.ListType.ofOptional(3,
            Types.StructType.of(
                required(4, "lat", Types.IntegerType.get()),
                optional(5, "long", Types.IntegerType.get())
            )
        ))
    );

    Schema result = new SchemaUpdate(schema, 1)
        .addColumn("locations", list)
        .apply();

    Assert.assertEquals("Should add map and reassign column IDs",
        expected.asStruct(), result.asStruct());
  }

  @Test
  public void testAddRequiredColumn() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(
        required(1, "id", Types.IntegerType.get()),
        required(2, "data", Types.StringType.get()));

    AssertHelpers.assertThrows("Should reject add required column if incompatible changes are not allowed",
        IllegalArgumentException.class, "Incompatible change: cannot add required column: data",
        () -> new SchemaUpdate(schema, 1).addRequiredColumn("data", Types.StringType.get()));

    Schema result = new SchemaUpdate(schema, 1)
        .allowIncompatibleChanges()
        .addRequiredColumn("data", Types.StringType.get())
        .apply();

    Assert.assertEquals("Should add required column", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testMakeColumnOptional() {
    Schema schema = new Schema(required(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(optional(1, "id", Types.IntegerType.get()));

    Schema result = new SchemaUpdate(schema, 1)
        .makeColumnOptional("id")
        .apply();

    Assert.assertEquals("Should update column to be optional", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testRequireColumn() {
    Schema schema = new Schema(optional(1, "id", Types.IntegerType.get()));
    Schema expected = new Schema(required(1, "id", Types.IntegerType.get()));

    AssertHelpers.assertThrows("Should reject change to required if incompatible changes are not allowed",
        IllegalArgumentException.class, "Cannot change column nullability: id: optional -> required",
        () -> new SchemaUpdate(schema, 1).requireColumn("id"));

    // required to required is not an incompatible change
    new SchemaUpdate(expected, 1).requireColumn("id").apply();

    Schema result = new SchemaUpdate(schema, 1)
        .allowIncompatibleChanges()
        .requireColumn("id")
        .apply();

    Assert.assertEquals("Should update column to be required", expected.asStruct(), result.asStruct());
  }

  @Test
  public void testMixedChanges() {
    Schema expected = new Schema(
        required(1, "id", Types.LongType.get(), "unique id"),
        required(2, "json", Types.StringType.get()),
        optional(3, "options", Types.StructType.of(
            required(8, "feature1", Types.BooleanType.get()),
            optional(9, "newfeature", Types.BooleanType.get())
        ), "struct of named boolean options"),
        required(4, "locations", Types.MapType.ofRequired(10, 11,
            Types.StructType.of(
                required(20, "address", Types.StringType.get()),
                required(21, "city", Types.StringType.get()),
                required(22, "state", Types.StringType.get()),
                required(23, "zip", Types.IntegerType.get())
            ),
            Types.StructType.of(
                required(12, "latitude", Types.DoubleType.get(), "latitude"),
                optional(25, "alt", Types.FloatType.get()),
                required(28, "description", Types.StringType.get(), "Location description")
            )), "map of address to coordinate"),
        optional(5, "points", Types.ListType.ofOptional(14,
            Types.StructType.of(
                optional(15, "X", Types.LongType.get()),
                required(16, "y.y", Types.LongType.get()),
                optional(26, "z", Types.LongType.get()),
                optional(27, "t.t", Types.LongType.get(), "name with '.'")
            )), "2-D cartesian points"),
        required(6, "doubles", Types.ListType.ofRequired(17,
            Types.DoubleType.get()
        )),
        optional(24, "toplevel", Types.DecimalType.of(9, 2))
    );

    Schema updated = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
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
        .addRequiredColumn("locations", "description", Types.StringType.get(), "Location description")
        .apply();

    Assert.assertEquals("Should match with added fields", expected.asStruct(), updated.asStruct());
  }

  @Test
  public void testAmbiguousAdd() {
    // preferences.booleans could be top-level or a field of preferences
    AssertHelpers.assertThrows("Should reject ambiguous column name",
        IllegalArgumentException.class, "ambiguous name: preferences.booleans", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences.booleans", Types.BooleanType.get());
        }
    );
  }

  @Test
  public void testAddAlreadyExists() {
    AssertHelpers.assertThrows("Should reject column name that already exists",
        IllegalArgumentException.class, "already exists: preferences.feature1", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", "feature1", Types.BooleanType.get());
        }
    );
    AssertHelpers.assertThrows("Should reject column name that already exists",
        IllegalArgumentException.class, "already exists: preferences", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", Types.BooleanType.get());
        }
    );
  }

  @Test
  public void testDeleteMissingColumn() {
    AssertHelpers.assertThrows("Should reject delete missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("col");
        }
    );
  }

  @Test
  public void testAddDeleteConflict() {
    AssertHelpers.assertThrows("Should reject add then delete",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("col", Types.IntegerType.get()).deleteColumn("col");
        }
    );
    AssertHelpers.assertThrows("Should reject add then delete",
        IllegalArgumentException.class, "column that has additions: preferences", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.addColumn("preferences", "feature3", Types.IntegerType.get())
              .deleteColumn("preferences");
        }
    );
  }

  @Test
  public void testRenameMissingColumn() {
    AssertHelpers.assertThrows("Should reject rename missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("col", "fail");
        }
    );
  }

  @Test
  public void testRenameDeleteConflict() {
    AssertHelpers.assertThrows("Should reject rename then delete",
        IllegalArgumentException.class, "column that has updates: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("id", "col").deleteColumn("id");
        }
    );
    AssertHelpers.assertThrows("Should reject rename then delete",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.renameColumn("id", "col").deleteColumn("col");
        }
    );
  }

  @Test
  public void testDeleteRenameConflict() {
    AssertHelpers.assertThrows("Should reject delete then rename",
        IllegalArgumentException.class, "column that will be deleted: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("id").renameColumn("id", "identifier");
        }
    );
  }

  @Test
  public void testUpdateMissingColumn() {
    AssertHelpers.assertThrows("Should reject rename missing column",
        IllegalArgumentException.class, "missing column: col", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.updateColumn("col", Types.DateType.get());
        }
    );
  }

  @Test
  public void testUpdateDeleteConflict() {
    AssertHelpers.assertThrows("Should reject update then delete",
        IllegalArgumentException.class, "column that has updates: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.updateColumn("id", Types.LongType.get()).deleteColumn("id");
        }
    );
  }

  @Test
  public void testDeleteUpdateConflict() {
    AssertHelpers.assertThrows("Should reject delete then update",
        IllegalArgumentException.class, "column that will be deleted: id", () -> {
          UpdateSchema update = new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID);
          update.deleteColumn("id").updateColumn("id", Types.LongType.get());
        }
    );
  }

  @Test
  public void testDeleteMapKey() {
    AssertHelpers.assertThrows("Should reject delete map key",
        IllegalArgumentException.class, "Cannot delete map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID).deleteColumn("locations.key").apply();
        }
    );
  }

  @Test
  public void testAddFieldToMapKey() {
    AssertHelpers.assertThrows("Should reject add sub-field to map key",
        IllegalArgumentException.class, "Cannot add fields to map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
              .addColumn("locations.key", "address_line_2", Types.StringType.get()).apply();
        }
    );
  }

  @Test
  public void testAlterMapKey() {
    AssertHelpers.assertThrows("Should reject alter sub-field of map key",
        IllegalArgumentException.class, "Cannot alter map keys", () -> {
          new SchemaUpdate(SCHEMA, SCHEMA_LAST_COLUMN_ID)
              .updateColumn("locations.key.zip", Types.LongType.get()).apply();
        }
    );
  }

  @Test
  public void testUpdateMapKey() {
    Schema schema = new Schema(required(1, "m", Types.MapType.ofOptional(2, 3,
        Types.IntegerType.get(), Types.DoubleType.get())));
    AssertHelpers.assertThrows("Should reject update map key",
        IllegalArgumentException.class, "Cannot update map keys", () -> {
          new SchemaUpdate(schema, 3).updateColumn("m.key", Types.LongType.get()).apply();
        }
    );
  }

  @Test
  public void testUpdateAddedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    AssertHelpers.assertThrows("Should reject add and update doc",
        IllegalArgumentException.class, "Cannot update missing column", () -> {
          new SchemaUpdate(schema, 3)
              .addColumn("value", Types.LongType.get())
              .updateColumnDoc("value", "a value")
              .apply();
        }
    );
  }

  @Test
  public void testUpdateDeletedColumnDoc() {
    Schema schema = new Schema(required(1, "i", Types.IntegerType.get()));
    AssertHelpers.assertThrows("Should reject add and update doc",
        IllegalArgumentException.class, "Cannot update a column that will be deleted", () -> {
          new SchemaUpdate(schema, 3)
              .deleteColumn("i")
              .updateColumnDoc("i", "a value")
              .apply();
        }
    );
  }
}
