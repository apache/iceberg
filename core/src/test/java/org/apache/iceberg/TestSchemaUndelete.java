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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.List;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSchemaUndelete extends TestBase {

  @TestTemplate
  public void undeleteExistingNameRefuses() {
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id"))
        .hasMessage("Cannot undelete column: name already exists: id");
  }

  @TestTemplate
  public void undeleteUnknownNameRefuses() {
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("ghost"))
        .hasMessage("Cannot undelete column: no deleted column with that name: ghost");
  }

  @TestTemplate
  public void undeleteRestoresNewestIncarnation() {
    table.updateSchema().addColumn("flag", Types.BooleanType.get()).commit();
    table.updateSchema().deleteColumn("flag").commit();

    table.updateSchema().addColumn("flag", Types.StringType.get()).commit();
    int stringIncarnationId = table.schema().findField("flag").fieldId();
    table.updateSchema().deleteColumn("flag").commit();

    table.updateSchema().undeleteColumn("flag").commit();

    Types.NestedField restored = table.schema().findField("flag");
    assertThat(restored.fieldId()).isEqualTo(stringIncarnationId);
    assertThat(restored.type()).isEqualTo(Types.StringType.get());
    assertThat(restored.isOptional()).isTrue();
  }

  @TestTemplate
  public void undeleteCaseCollisionWithLiveColumnRefuses() {
    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("ID"))
        .hasMessage(
            "Cannot undelete column: case-insensitive collision between ID and existing "
                + "column: id");
  }

  @TestTemplate
  public void undeleteCaseCollisionWithPendingAddRefuses() {
    UpdateSchema update = table.updateSchema();
    update.addColumn("X", Types.IntegerType.get());

    assertThatThrownBy(() -> update.undeleteColumn("x"))
        .hasMessage(
            "Cannot undelete column: case-insensitive collision between x and existing "
                + "column: X");
  }

  @TestTemplate
  public void undeleteNestedChildUnderLiveParentKeepsIds() {
    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lat", Types.DoubleType.get())))
        .commit();

    int locId = table.schema().findField("loc").fieldId();
    int latId = table.schema().findField("loc.lat").fieldId();

    table.updateSchema().deleteColumn("loc.lat").commit();
    table.updateSchema().undeleteColumn("loc.lat").commit();

    Types.NestedField loc = table.schema().findField("loc");
    assertThat(loc.fieldId()).isEqualTo(locId);
    assertThat(loc.type().asStructType().field("lat").fieldId()).isEqualTo(latId);
    assertThat(table.schema().findColumnName(latId)).isEqualTo("loc.lat");
  }

  @TestTemplate
  public void undeleteChildAfterParentDroppedAndReaddedLandsUnderCurrentParent() {
    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lat", Types.DoubleType.get())))
        .commit();

    int latId = table.schema().findField("loc.lat").fieldId();

    // drop the whole struct, then re-add a fresh parent carrying a different child
    table.updateSchema().deleteColumn("loc").commit();
    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lon", Types.DoubleType.get())))
        .commit();
    int freshLocId = table.schema().findField("loc").fieldId();

    // the historical child must come back under the CURRENT parent, not the dead one (#15084)
    table.updateSchema().undeleteColumn("loc.lat").commit();

    Types.NestedField loc = table.schema().findField("loc");
    assertThat(loc.fieldId()).isEqualTo(freshLocId);
    assertThat(loc.type().asStructType().field("lat").fieldId()).isEqualTo(latId);
    assertThat(loc.type().asStructType().field("lon")).isNotNull();
    assertThat(table.schema().findColumnName(latId)).isEqualTo("loc.lat");
  }

  @TestTemplate
  public void undeleteMissingAncestorRefusesWithPath() {
    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lat", Types.DoubleType.get())))
        .commit();
    table.updateSchema().deleteColumn("loc.lat").commit();
    table.updateSchema().deleteColumn("loc").commit();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("loc.lat"))
        .hasMessage("Cannot find parent struct: loc");
  }

  @TestTemplate
  public void undeleteIntoPendingDeletedParentRefuses() {
    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lat", Types.DoubleType.get())))
        .commit();
    table.updateSchema().deleteColumn("loc.lat").commit();

    UpdateSchema update = table.updateSchema();
    update.deleteColumn("loc");

    assertThatThrownBy(() -> update.undeleteColumn("loc.lat"))
        .hasMessage("Cannot undelete into a column that will be deleted: loc");
  }

  @TestTemplate
  public void undeleteRequiredNoSnapshotsThrows() {
    table.updateSchema().deleteColumn("id").commit();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id").commit())
        .hasMessage(
            "Cannot undelete required column id: table has no snapshots. Only nullable columns "
                + "or tables unchanged since the drop can be undeleted");
  }

  @TestTemplate
  public void undeleteRequiredUnchangedSinceDropStaysRequired() {
    table.newFastAppend().appendFile(FILE_A).commit();

    int idBefore = table.schema().findField("id").fieldId();

    table.updateSchema().deleteColumn("id").commit();
    table.updateSchema().undeleteColumn("id").commit();

    Types.NestedField restored = table.schema().findField("id");
    assertThat(restored.fieldId()).isEqualTo(idBefore);
    assertThat(restored.isRequired()).isTrue();
  }

  @TestTemplate
  public void undeleteOptionalWithWritesRestoresOptional() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();

    int payloadBefore = table.schema().findField("payload").fieldId();

    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSchema().deleteColumn("payload").commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    table.updateSchema().undeleteColumn("payload").commit();

    Types.NestedField restored = table.schema().findField("payload");
    assertThat(restored.fieldId()).isEqualTo(payloadBefore);
    assertThat(restored.isOptional()).isTrue();
  }

  @TestTemplate
  public void undeleteRequiredWithWritesThrowsQuotingLastSeenSnapshot() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table.updateSchema().deleteColumn("id").commit();
    table.newFastAppend().appendFile(FILE_B).commit();

    long lastSeenSnapshotId = table.currentSnapshot().snapshotId();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("id").commit())
        .hasMessage(
            "Cannot undelete required column id: rows were written while the column was absent "
                + "(last seen at snapshot "
                + lastSeenSnapshotId
                + "). Only nullable columns or tables unchanged since the drop can be undeleted");
  }

  @TestTemplate
  public void undeleteRequiredPrunedLineageThrows() {
    table.newFastAppend().appendFile(FILE_A).commit();
    table
        .updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("extra", Types.LongType.get())
        .commit();
    table.updateSchema().deleteColumn("extra").commit();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("extra").commit())
        .hasMessage(
            "Cannot undelete required column extra: snapshot lineage could not be verified. "
                + "Only nullable columns or tables unchanged since the drop can be undeleted");
  }

  @TestTemplate
  public void deleteThenUndeleteSameBatchReplacesInPlace() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();

    int payloadId = table.schema().findField("payload").fieldId();
    int versionBefore = version();

    UpdateSchema update = table.updateSchema();
    update.deleteColumn("payload");
    update.undeleteColumn("payload");
    update.commit();

    List<Types.NestedField> columns = table.schema().columns();
    assertThat(columns.stream().filter(field -> field.name().equals("payload")).count())
        .isEqualTo(1);

    Types.NestedField restored = table.schema().findField("payload");
    assertThat(restored.fieldId()).isEqualTo(payloadId);
    assertThat(restored.isOptional()).isTrue();
    assertThat(version()).isEqualTo(versionBefore + 1);
  }

  @TestTemplate
  public void undeleteTwiceInOneUpdateRefuses() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();
    table.updateSchema().deleteColumn("payload").commit();

    UpdateSchema update = table.updateSchema();
    update.undeleteColumn("payload");

    // the pending-add collision guard runs before the double-undelete guard and rejects first
    assertThatThrownBy(() -> update.undeleteColumn("payload"))
        .hasMessage(
            "Cannot undelete column: case-insensitive collision between payload and existing "
                + "column: payload");
  }

  @TestTemplate
  public void undeleteThenAddSameNameInOneUpdateFailsAtApply() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();
    table.updateSchema().deleteColumn("payload").commit();

    UpdateSchema update = table.updateSchema().undeleteColumn("payload");
    update.addColumn("payload", Types.IntegerType.get());

    // mirrors plain double-addColumn on main: the duplicate surfaces at apply, not at call time
    assertThatThrownBy(update::apply)
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("multiple fields for name payload");
  }

  @TestTemplate
  public void addThenUndeleteSameNameInOneUpdateRefuses() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();
    table.updateSchema().deleteColumn("payload").commit();

    UpdateSchema update = table.updateSchema();
    update.addColumn("payload", Types.IntegerType.get());

    assertThatThrownBy(() -> update.undeleteColumn("payload"))
        .hasMessage(
            "Cannot undelete column: case-insensitive collision between payload and existing "
                + "column: payload");
  }

  @TestTemplate
  public void undeleteLiveIdUnderRenamedNameRefuses() {
    int dataId = table.schema().findField("data").fieldId();
    table.updateSchema().renameColumn("data", "payload").commit();

    assertThatThrownBy(() -> table.updateSchema().undeleteColumn("data"))
        .hasMessage("Cannot undelete column: field ID " + dataId + " is still present as payload");
  }

  @TestTemplate
  public void undeletePreservesDocAndDefaults() {
    assumeThat(formatVersion).isGreaterThanOrEqualTo(3);

    table
        .updateSchema()
        .addColumn("meas", Types.LongType.get(), "sensor measurement", Literal.of(42L))
        .commit();

    int measId = table.schema().findField("meas").fieldId();

    table.updateSchema().deleteColumn("meas").commit();
    table.updateSchema().undeleteColumn("meas").commit();

    Types.NestedField restored = table.schema().findField("meas");
    assertThat(restored.fieldId()).isEqualTo(measId);
    assertThat(restored.doc()).isEqualTo("sensor measurement");
    assertThat(restored.initialDefaultLiteral()).isEqualTo(Literal.of(42L));
    assertThat(restored.writeDefaultLiteral()).isEqualTo(Literal.of(42L));
  }

  @TestTemplate
  public void undeleteKeepsLastColumnIdAndNextAddIncrements() {
    int baseLastColumnId = readMetadata().lastColumnId();

    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();
    int afterAddLastColumnId = readMetadata().lastColumnId();
    assertThat(afterAddLastColumnId).isEqualTo(baseLastColumnId + 1);

    table.updateSchema().deleteColumn("payload").commit();
    table.updateSchema().undeleteColumn("payload").commit();

    assertThat(readMetadata().lastColumnId()).isEqualTo(afterAddLastColumnId);

    table.updateSchema().addColumn("extra", Types.LongType.get()).commit();

    assertThat(readMetadata().lastColumnId()).isEqualTo(afterAddLastColumnId + 1);
    assertThat(table.schema().findField("extra").fieldId()).isEqualTo(afterAddLastColumnId + 1);
  }

  @TestTemplate
  public void committedDropThenUndeleteReusesHistoricalFieldId() {
    table.updateSchema().addColumn("payload", Types.StringType.get()).commit();

    int payloadBefore = table.schema().findField("payload").fieldId();

    table.updateSchema().deleteColumn("payload").commit();
    table.updateSchema().undeleteColumn("payload").commit();

    TableMetadata metadata = readMetadata();
    assertThat(metadata.currentSchemaId()).isNotEqualTo(metadata.schemas().get(0).schemaId());
    Types.NestedField restored = table.schema().findField("payload");
    assertThat(restored.fieldId()).isEqualTo(payloadBefore);
    assertThat(restored.type()).isEqualTo(Types.StringType.get());
    assertThat(restored.isOptional()).isTrue();
  }

  @TestTemplate
  public void undeleteUpdatesMappingWithoutDuplicateIds() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table.updateSchema().addColumn("cnt", Types.LongType.get()).commit();
    int cntId = table.schema().findField("cnt").fieldId();
    table.updateSchema().deleteColumn("cnt").commit();
    table.updateSchema().undeleteColumn("cnt").commit();

    String mappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(mappingJson);

    MappedField cntMapping = updated.find(cntId);
    assertThat(cntMapping).isNotNull();
    assertThat(cntMapping.names()).containsExactly("cnt");
    assertThat(countFieldsWithId(updated.asMappedFields(), cntId)).isEqualTo(1);

    int idColId = table.schema().findField("id").fieldId();
    int dataColId = table.schema().findField("data").fieldId();
    assertThat(updated.find(idColId).names()).containsExactly("id");
    assertThat(updated.find(dataColId).names()).containsExactly("data");
  }

  @TestTemplate
  public void undeleteMergesStaleNestedAlias() {
    NameMapping mapping = MappingUtil.create(table.schema());
    table
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();

    table
        .updateSchema()
        .addColumn(
            "loc",
            Types.StructType.of(Types.NestedField.optional(1, "lat", Types.DoubleType.get())))
        .commit();
    int locId = table.schema().findField("loc").fieldId();
    int latId = table.schema().findField("loc.lat").fieldId();

    table.updateSchema().renameColumn("loc.lat", "latitude").commit();
    table.updateSchema().deleteColumn("loc.latitude").commit();
    table.updateSchema().undeleteColumn("loc.lat").commit();

    String mappingJson = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    NameMapping updated = NameMappingParser.fromJson(mappingJson);

    assertThat(table.schema().findField("loc.lat").fieldId()).isEqualTo(latId);

    MappedField locMapping = updated.find(locId);
    assertThat(locMapping).isNotNull();
    MappedField latMapping = locMapping.nestedMapping().field(latId);
    assertThat(latMapping).isNotNull();
    assertThat(latMapping.names()).containsExactlyInAnyOrder("lat", "latitude");
    assertThat(countFieldsWithId(updated.asMappedFields(), latId)).isEqualTo(1);
  }

  private int countFieldsWithId(MappedFields fields, int fieldId) {
    int count = 0;
    for (MappedField field : fields.fields()) {
      if (field.id() != null && field.id() == fieldId) {
        count += 1;
      }
      if (field.nestedMapping() != null) {
        count += countFieldsWithId(field.nestedMapping(), fieldId);
      }
    }

    return count;
  }
}
