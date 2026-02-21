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
package org.apache.iceberg.hadoop;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestSchemaUndelete extends HadoopTableTestBase {

  @Test
  public void testUndeleteTopLevelColumn() {
    // Add a column, then delete it, then undelete it
    table.updateSchema().addColumn("count", Types.LongType.get(), "a count column").commit();

    int originalFieldId = table.schema().findField("count").fieldId();
    assertThat(table.schema().findField("count")).isNotNull();

    // Delete the column
    table.updateSchema().deleteColumn("count").commit();
    assertThat(table.schema().findField("count")).isNull();

    // Undelete the column
    table.updateSchema().undeleteColumn("count").commit();

    Types.NestedField restoredField = table.schema().findField("count");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.type()).isEqualTo(Types.LongType.get());
    assertThat(restoredField.doc()).isEqualTo("a count column");
  }

  @Test
  public void testUndeleteNestedField() {
    // Add a struct with nested fields
    table
        .updateSchema()
        .addColumn(
            "location",
            Types.StructType.of(
                Types.NestedField.optional(100, "lat", Types.DoubleType.get()),
                Types.NestedField.optional(101, "long", Types.DoubleType.get())))
        .commit();

    int latFieldId = table.schema().findField("location.lat").fieldId();
    assertThat(table.schema().findField("location.lat")).isNotNull();

    // Delete the nested field
    table.updateSchema().deleteColumn("location.lat").commit();
    assertThat(table.schema().findField("location.lat")).isNull();

    // Undelete the nested field
    table.updateSchema().undeleteColumn("location.lat").commit();

    Types.NestedField restoredField = table.schema().findField("location.lat");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(latFieldId);
    assertThat(restoredField.type()).isEqualTo(Types.DoubleType.get());
  }

  @Test
  public void testUndeleteColumnAlreadyExists() {
    // Try to undelete a column that already exists (id is part of SCHEMA)
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists in the current schema");
  }

  @Test
  public void testUndeleteColumnNotFound() {
    // Try to undelete a column that was never in the schema
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("nonexistent_column").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not found in any historical schema");
  }

  @Test
  public void testUndeletePreservesFieldId() {
    // This test explicitly verifies that the field ID is preserved (not a new ID)
    table.updateSchema().addColumn("temp_col", Types.StringType.get()).commit();

    int originalId = table.schema().findField("temp_col").fieldId();

    // Add another column to increment the lastColumnId
    table.updateSchema().addColumn("another_col", Types.IntegerType.get()).commit();
    int lastIdAfterAdd = table.schema().findField("another_col").fieldId();

    // Delete temp_col
    table.updateSchema().deleteColumn("temp_col").commit();

    // Undelete temp_col
    table.updateSchema().undeleteColumn("temp_col").commit();

    Types.NestedField restored = table.schema().findField("temp_col");
    assertThat(restored.fieldId())
        .as("Restored field should have original ID, not a new one")
        .isEqualTo(originalId);
    assertThat(restored.fieldId())
        .as("Restored field ID should be less than the last assigned ID")
        .isLessThan(lastIdAfterAdd);
  }

  @Test
  public void testUndeleteNestedFieldParentMissing() {
    // Add a struct, delete the whole struct, then try to undelete a nested field
    table
        .updateSchema()
        .addColumn(
            "prefs",
            Types.StructType.of(
                Types.NestedField.optional(200, "setting1", Types.BooleanType.get()),
                Types.NestedField.optional(201, "setting2", Types.BooleanType.get())))
        .commit();

    // Delete the entire parent struct
    table.updateSchema().deleteColumn("prefs").commit();
    assertThat(table.schema().findField("prefs")).isNull();

    // Try to undelete nested field when parent doesn't exist
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("prefs.setting1").commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("parent struct")
        .hasMessageContaining("does not exist")
        .hasMessageContaining("Undelete the parent first");
  }

  @Test
  public void testUndeleteParentThenNestedField() {
    // Add a struct, delete the whole struct, then undelete parent, then undelete nested field
    table
        .updateSchema()
        .addColumn(
            "config",
            Types.StructType.of(
                Types.NestedField.optional(300, "enabled", Types.BooleanType.get()),
                Types.NestedField.optional(301, "value", Types.StringType.get())))
        .commit();

    int enabledId = table.schema().findField("config.enabled").fieldId();
    int configId = table.schema().findField("config").fieldId();

    // Delete both nested fields to empty the struct, then delete the struct
    table.updateSchema().deleteColumn("config.enabled").deleteColumn("config.value").commit();
    table.updateSchema().deleteColumn("config").commit();

    assertThat(table.schema().findField("config")).isNull();

    // Undelete the parent struct first
    table.updateSchema().undeleteColumn("config").commit();

    Types.NestedField restoredConfig = table.schema().findField("config");
    assertThat(restoredConfig).isNotNull();
    assertThat(restoredConfig.fieldId()).isEqualTo(configId);

    // Now undelete the nested field
    table.updateSchema().undeleteColumn("config.enabled").commit();

    Types.NestedField restoredEnabled = table.schema().findField("config.enabled");
    assertThat(restoredEnabled).isNotNull();
    assertThat(restoredEnabled.fieldId()).isEqualTo(enabledId);
  }

  @Test
  public void testUndeleteRequiredColumnBecomesOptional() {
    // Add a required column, delete it, then undelete it
    // The undeleted column should be optional because new data may have been written
    // without this column after it was deleted
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();

    Types.NestedField originalField = table.schema().findField("required_col");
    assertThat(originalField.isRequired()).isTrue();
    int originalFieldId = originalField.fieldId();

    // Delete the required column
    table.updateSchema().deleteColumn("required_col").commit();
    assertThat(table.schema().findField("required_col")).isNull();

    // Undelete the column - it should now be optional
    table.updateSchema().undeleteColumn("required_col").commit();

    Types.NestedField restoredField = table.schema().findField("required_col");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.isOptional())
        .as(
            "Undeleted column must be optional (not required) because new data may have been "
                + "written without this column")
        .isTrue();
  }

  @Test
  public void testUndeleteCaseInsensitive() {
    // Add and delete a column
    table.updateSchema().addColumn("MixedCase", Types.StringType.get()).commit();
    int originalId = table.schema().findField("MixedCase").fieldId();
    table.updateSchema().deleteColumn("MixedCase").commit();

    // Undelete with different case (case insensitive mode)
    table.updateSchema().caseSensitive(false).undeleteColumn("mixedcase").commit();

    Types.NestedField restored = table.schema().findField("MixedCase");
    assertThat(restored).isNotNull();
    assertThat(restored.fieldId()).isEqualTo(originalId);
  }

  @Test
  public void testUndeleteRestoresMostRecentlyDeletedField() {
    // Add a column, delete it, add it again (new ID), delete it again
    // Undelete should restore the most recently deleted field (second one)
    table.updateSchema().addColumn("reused_name", Types.StringType.get()).commit();
    int firstFieldId = table.schema().findField("reused_name").fieldId();

    // Delete the first field
    table.updateSchema().deleteColumn("reused_name").commit();
    assertThat(table.schema().findField("reused_name")).isNull();

    // Add a new field with the same name (will get a new ID)
    table.updateSchema().addColumn("reused_name", Types.IntegerType.get()).commit();
    int secondFieldId = table.schema().findField("reused_name").fieldId();
    assertThat(secondFieldId)
        .as("Second field should have a different ID than the first")
        .isNotEqualTo(firstFieldId);

    // Delete the second field
    table.updateSchema().deleteColumn("reused_name").commit();
    assertThat(table.schema().findField("reused_name")).isNull();

    // Undelete - should restore the most recently deleted field (the second one)
    table.updateSchema().undeleteColumn("reused_name").commit();

    Types.NestedField restored = table.schema().findField("reused_name");
    assertThat(restored).isNotNull();
    assertThat(restored.fieldId())
        .as("Undelete should restore the most recently deleted field, not the first")
        .isEqualTo(secondFieldId);
    assertThat(restored.type())
        .as("Restored field should have the type of the second field")
        .isEqualTo(Types.IntegerType.get());
  }
}
