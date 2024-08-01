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

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSchemaAndMappingUpdate extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2, 3);
  }

  @TestTemplate
  public void testAddPrimitiveColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    table.updateSchema().addColumn("count", Types.LongType.get()).commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    validateUnchanged(mapping, updated);

    MappedField newMapping = updated.find("count");
    assertThat(newMapping).isNotNull();
    assertThat(updated.find("count").id()).isEqualTo(table.schema().findField("count").fieldId());
    assertThat(updated.find("count").nestedMapping()).isNull();
  }

  @TestTemplate
  public void testAddStructColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    table
        .updateSchema()
        .addColumn(
            "location",
            Types.StructType.of(
                Types.NestedField.optional(1, "lat", Types.DoubleType.get()),
                Types.NestedField.optional(2, "long", Types.DoubleType.get())))
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    validateUnchanged(mapping, updated);

    MappedField newMapping = updated.find("location");
    assertThat(newMapping).isNotNull();

    assertThat(updated.find("location").id())
        .isEqualTo(table.schema().findField("location").fieldId());
    assertThat(updated.find("location").nestedMapping()).isNotNull();

    assertThat(updated.find("location.lat").id())
        .isEqualTo(table.schema().findField("location.lat").fieldId());
    assertThat(updated.find("location.lat").nestedMapping()).isNull();

    assertThat(updated.find("location.long").id())
        .isEqualTo(table.schema().findField("location.long").fieldId());
    assertThat(updated.find("location.long").nestedMapping()).isNull();
  }

  @TestTemplate
  public void testRenameColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    table.updateSchema().renameColumn("id", "object_id").commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("object_id").fieldId();
    validateUnchanged(
        Iterables.filter(
            mapping.asMappedFields().fields(), field -> !Objects.equals(idColumnId, field.id())),
        updated);

    MappedField updatedMapping = updated.find(idColumnId);
    assertThat(updatedMapping)
        .isNotNull()
        .isEqualTo(MappedField.of(idColumnId, ImmutableList.of("id", "object_id")));
  }

  @TestTemplate
  public void testDeleteColumn() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    table.updateSchema().deleteColumn("id").commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    // should not change the mapping
    validateUnchanged(mapping, updated);
  }

  @TestTemplate
  public void testModificationWithMetricsMetrics() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
        .set("write.metadata.metrics.column.id", "full")
        .commit();

    assertThatThrownBy(
            () ->
                table
                    .updateProperties()
                    .set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson)
                    .set("write.metadata.metrics.column.ids", "full")
                    .commit())
        .isInstanceOf(ValidationException.class)
        .hasMessageStartingWith(
            "Invalid metrics config, could not find column ids from table prop write.metadata.metrics.column.ids in schema table");

    // Re-naming a column with metrics succeeds;
    table.updateSchema().renameColumn("id", "bloop").commit();
    assertThat(table.properties())
        .containsEntry(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "bloop", "full")
        .doesNotContainKey(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id");

    // Deleting a column with metrics succeeds
    table.updateSchema().deleteColumn("bloop").commit();
    // Make sure no more reference to bloop in the metrics config
    assertThat(table.properties())
        .doesNotContainKey(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "id")
        .doesNotContainKey(TableProperties.METRICS_MODE_COLUMN_CONF_PREFIX + "bloop");
  }

  @TestTemplate
  public void testModificationWithParquetBloomConfig() {
    table
        .updateProperties()
        .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
        .commit();

    table.updateSchema().renameColumn("id", "ID").commit();
    assertThat(table.properties())
        .containsEntry(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "ID", "true")
        .doesNotContainKey(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id");

    table.updateSchema().deleteColumn("ID").commit();
    assertThat(table.properties())
        .doesNotContainKey(
            table.properties().get(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "ID"));
  }

  @TestTemplate
  public void testDeleteAndAddColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema().deleteColumn("id").commit();

    // add the same column name back to the table with a different field ID
    table.updateSchema().addColumn("id", Types.StringType.get()).commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(
            mapping.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    assertThat(newMapping).isNotNull();
    assertThat(newMapping.id()).isEqualTo(idColumnId);
    assertThat(newMapping.nestedMapping()).isNull();

    MappedField updatedMapping = updated.find(startIdColumnId);
    assertThat(updatedMapping).isNotNull();
    assertThat(updatedMapping.id()).isEqualTo(startIdColumnId);
    assertThat(updatedMapping.names()).doesNotContain("id");
    assertThat(updatedMapping.nestedMapping()).isNull();
  }

  @TestTemplate
  public void testDeleteAndRenameColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema().deleteColumn("id").commit();

    // rename the data column to id
    table.updateSchema().renameColumn("data", "id").commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(
            mapping.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    assertThat(newMapping).isNotNull();
    assertThat(newMapping.id()).isEqualTo(idColumnId);
    assertThat(newMapping.names()).containsExactly("data", "id");
    assertThat(newMapping.nestedMapping()).isNull();

    MappedField updatedMapping = updated.find(startIdColumnId);
    assertThat(updatedMapping).isNotNull();
    assertThat(updatedMapping.id()).isEqualTo(startIdColumnId);
    assertThat(updatedMapping.names()).doesNotContain("id");
    assertThat(updatedMapping.nestedMapping()).isNull();
  }

  @TestTemplate
  public void testRenameAndAddColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema().renameColumn("id", "object_id").commit();

    NameMapping afterRename =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    assertThat(afterRename.find(startIdColumnId).names()).containsExactly("id", "object_id");

    // add a new column with the renamed column's old name
    // also, rename the original column again to ensure its names are handled correctly
    table
        .updateSchema()
        .renameColumn("object_id", "oid")
        .addColumn("id", Types.StringType.get())
        .commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(
            afterRename.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    assertThat(newMapping).isNotNull();
    assertThat(newMapping.id()).isEqualTo(idColumnId);
    assertThat(newMapping.nestedMapping()).isNull();

    MappedField updatedMapping = updated.find(startIdColumnId);
    assertThat(updatedMapping).isNotNull();
    assertThat(updatedMapping.id()).isEqualTo(startIdColumnId);
    assertThat(updatedMapping.names()).containsExactly("oid", "object_id");
    assertThat(updatedMapping.nestedMapping()).isNull();
  }

  @TestTemplate
  public void testRenameAndRenameColumnReassign() {
    NameMapping mapping = MappingUtil.create(table.schema());
    String mappingJson = NameMappingParser.toJson(mapping);

    table.updateProperties().set(TableProperties.DEFAULT_NAME_MAPPING, mappingJson).commit();

    int startIdColumnId = table.schema().findField("id").fieldId(); // the original field ID

    table.updateSchema().renameColumn("id", "object_id").commit();

    NameMapping afterRename =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    assertThat(afterRename.find(startIdColumnId).names()).containsExactly("id", "object_id");

    // rename the data column to the renamed column's old name
    // also, rename the original column again to ensure its names are handled correctly
    table.updateSchema().renameColumn("object_id", "oid").renameColumn("data", "id").commit();

    String updatedJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(updatedJson);

    int idColumnId = table.schema().findField("id").fieldId(); // the new field ID
    Set<Integer> changedIds = Sets.newHashSet(startIdColumnId, idColumnId);
    validateUnchanged(
        Iterables.filter(
            afterRename.asMappedFields().fields(), field -> !changedIds.contains(field.id())),
        updated);

    MappedField newMapping = updated.find("id");
    assertThat(newMapping).isNotNull();
    assertThat(newMapping.names()).containsExactly("data", "id");
    assertThat(newMapping.id()).isEqualTo(idColumnId);
    assertThat(newMapping.nestedMapping()).isNull();

    MappedField updatedMapping = updated.find(startIdColumnId);
    assertThat(updatedMapping).isNotNull();
    assertThat(updatedMapping.id()).isEqualTo(startIdColumnId);
    assertThat(updatedMapping.names()).containsExactly("oid", "object_id");
    assertThat(updatedMapping.nestedMapping()).isNull();
  }

  /** Asserts that the fields in the original mapping are unchanged in the updated mapping. */
  private void validateUnchanged(NameMapping original, NameMapping updated) {
    MappedFields updatedFields = updated.asMappedFields();
    for (MappedField field : original.asMappedFields().fields()) {
      assertThat(updatedFields.field(field.id())).isEqualTo(field);
    }
  }

  /** Asserts that the fields in the original mapping are unchanged in the updated mapping. */
  private void validateUnchanged(Iterable<MappedField> fields, NameMapping updated) {
    MappedFields updatedFields = updated.asMappedFields();
    for (MappedField field : fields) {
      assertThat(updatedFields.field(field.id())).isEqualTo(field);
    }
  }
}
