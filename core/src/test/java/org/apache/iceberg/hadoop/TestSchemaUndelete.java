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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.expressions.Literal;
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
    table.updateSchema().undeleteColumn("count", false).commit();

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
    table.updateSchema().undeleteColumn("location.lat", false).commit();

    Types.NestedField restoredField = table.schema().findField("location.lat");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(latFieldId);
    assertThat(restoredField.type()).isEqualTo(Types.DoubleType.get());
  }

  @Test
  public void testUndeleteColumnAlreadyExists() {
    // Try to undelete a column that already exists (id is part of SCHEMA)
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id", false).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("already exists in the current schema");
  }

  @Test
  public void testUndeleteColumnNotFound() {
    // Try to undelete a column that was never in the schema
    assertThatThrownBy(
            () -> table.updateSchema().undeleteColumn("nonexistent_column", false).commit())
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
    table.updateSchema().undeleteColumn("temp_col", false).commit();

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
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("prefs.setting1", false).commit())
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
    table.updateSchema().undeleteColumn("config", false).commit();

    Types.NestedField restoredConfig = table.schema().findField("config");
    assertThat(restoredConfig).isNotNull();
    assertThat(restoredConfig.fieldId()).isEqualTo(configId);

    // Now undelete the nested field
    table.updateSchema().undeleteColumn("config.enabled", false).commit();

    Types.NestedField restoredEnabled = table.schema().findField("config.enabled");
    assertThat(restoredEnabled).isNotNull();
    assertThat(restoredEnabled.fieldId()).isEqualTo(enabledId);
  }

  @Test
  public void testUndeleteRequiredColumnPreservedWhenNoData() {
    // Add a required column, delete it, then undelete with setNullable=false.
    // No data has been written, so the column should be restored as required.
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();

    Types.NestedField originalField = table.schema().findField("required_col");
    assertThat(originalField.isRequired()).isTrue();
    int originalFieldId = originalField.fieldId();

    table.updateSchema().deleteColumn("required_col").commit();
    assertThat(table.schema().findField("required_col")).isNull();

    table.updateSchema().undeleteColumn("required_col", false).commit();

    Types.NestedField restoredField = table.schema().findField("required_col");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.isRequired())
        .as("Required column must stay required when no data was written since deletion")
        .isTrue();
  }

  @Test
  public void testUndeleteRequiredColumnPreservedAcrossAppends() {
    // Data appended *while the column is present* is fine. After deletion, no more data is
    // written, so undelete should succeed as required.
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();
    int originalFieldId = table.schema().findField("required_col").fieldId();

    // Append data while the column exists — not a blocker.
    table.newFastAppend().appendFile(FILE_A).commit();

    table.updateSchema().deleteColumn("required_col").commit();
    assertThat(table.schema().findField("required_col")).isNull();

    table.updateSchema().undeleteColumn("required_col", false).commit();

    Types.NestedField restoredField = table.schema().findField("required_col");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.isRequired()).isTrue();
  }

  @Test
  public void testUndeleteRequiredColumnFailsWhenDataWritten() {
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSchema().deleteColumn("required_col").commit();
    // Data written while the column is absent — blocks the undelete.
    table.newFastAppend().appendFile(FILE_B).commit();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("required_col", false).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data was written after the column was deleted")
        .hasMessageContaining("setNullable=true");
  }

  @Test
  public void testUndeleteRequiredColumnAsNullableWithDataWritten() {
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();
    int originalFieldId = table.schema().findField("required_col").fieldId();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSchema().deleteColumn("required_col").commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    // setNullable=true bypasses the safety check and restores the column as optional.
    table.updateSchema().undeleteColumn("required_col", true).commit();

    Types.NestedField restoredField = table.schema().findField("required_col");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.isOptional()).isTrue();
  }

  @Test
  public void testUndeleteOptionalColumnWithDataWritten() {
    // Optional columns skip the data-since-deletion check entirely.
    table.updateSchema().addColumn("opt_col", Types.StringType.get()).commit();
    int originalFieldId = table.schema().findField("opt_col").fieldId();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSchema().deleteColumn("opt_col").commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    table.updateSchema().undeleteColumn("opt_col", false).commit();

    Types.NestedField restoredField = table.schema().findField("opt_col");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.fieldId()).isEqualTo(originalFieldId);
    assertThat(restoredField.isOptional()).isTrue();
  }

  @Test
  public void testUndeleteRequiredColumnFailsWithLegacySnapshotNoSchemaId() {
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("required_col", Types.StringType.get())
        .commit();

    table.updateSchema().deleteColumn("required_col").commit();

    // Inject a synthetic snapshot with null schemaId (simulating a pre-schema-id Iceberg
    // snapshot) that claims to have added data. Constructed via SnapshotParser.fromJson so
    // we don't need package-private BaseSnapshot access.
    BaseTable baseTable = (BaseTable) table;
    TableMetadata current = baseTable.operations().current();
    long nextSeq = current.lastSequenceNumber() + 1;
    long snapshotId = nextSeq + 1000L;
    String legacySnapshotJson =
        String.format(
            "{\"snapshot-id\":%d,\"sequence-number\":%d,\"timestamp-ms\":%d,"
                + "\"summary\":{\"operation\":\"append\",\"added-data-files\":\"1\"},"
                + "\"manifest-list\":\"no-such-manifest-list.avro\"}",
            snapshotId, nextSeq, System.currentTimeMillis());
    Snapshot legacySnapshot = SnapshotParser.fromJson(legacySnapshotJson);
    TableMetadata withLegacySnapshot =
        TableMetadata.buildFrom(current).addSnapshot(legacySnapshot).build();
    baseTable.operations().commit(current, withLegacySnapshot);
    table.refresh();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("required_col", false).commit())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("data was written after the column was deleted");

    // setNullable=true should bypass the check even with a legacy snapshot present.
    table.updateSchema().undeleteColumn("required_col", true).commit();
    assertThat(table.schema().findField("required_col")).isNotNull();
    assertThat(table.schema().findField("required_col").isOptional()).isTrue();
  }

  @Test
  public void testUndeleteCaseInsensitive() {
    // Add and delete a column
    table.updateSchema().addColumn("MixedCase", Types.StringType.get()).commit();
    int originalId = table.schema().findField("MixedCase").fieldId();
    table.updateSchema().deleteColumn("MixedCase").commit();

    // Undelete with different case (case insensitive mode)
    table.updateSchema().caseSensitive(false).undeleteColumn("mixedcase", false).commit();

    Types.NestedField restored = table.schema().findField("MixedCase");
    assertThat(restored).isNotNull();
    assertThat(restored.fieldId()).isEqualTo(originalId);
  }

  @Test
  public void testUndeletePreservesDefaults() {
    // Upgrade to v3 to support non-null defaults
    table.updateProperties().set(TableProperties.FORMAT_VERSION, "3").commit();

    // Add a column with initialDefault and writeDefault
    table
        .updateSchema()
        .addColumn("count", Types.IntegerType.get(), "a count column", Literal.of(42))
        .commit();

    Types.NestedField originalField = table.schema().findField("count");
    assertThat(originalField.initialDefault()).isEqualTo(42);
    assertThat(originalField.writeDefault()).isEqualTo(42);

    // Delete the column
    table.updateSchema().deleteColumn("count").commit();
    assertThat(table.schema().findField("count")).isNull();

    // Undelete the column
    table.updateSchema().undeleteColumn("count", false).commit();

    Types.NestedField restoredField = table.schema().findField("count");
    assertThat(restoredField).isNotNull();
    assertThat(restoredField.initialDefault())
        .as("initialDefault should be preserved after undelete")
        .isEqualTo(42);
    assertThat(restoredField.writeDefault())
        .as("writeDefault should be preserved after undelete")
        .isEqualTo(42);
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
    table.updateSchema().undeleteColumn("reused_name", false).commit();

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
