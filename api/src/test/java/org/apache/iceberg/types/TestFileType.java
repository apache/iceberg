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
package org.apache.iceberg.types;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestFileType {
  private static final Types.FileType FILE = Types.FileType.of(5);

  @Test
  void nestedFieldsAreDerivedFromTheEnclosingId() {
    assertThat(FILE.fields())
        .containsExactly(
            optional(6, "uri", Types.StringType.get()),
            optional(7, "offset", Types.LongType.get()),
            optional(8, "size", Types.LongType.get()),
            optional(9, "content_type", Types.StringType.get()),
            optional(10, "checksum", Types.StringType.get()),
            optional(11, "inline", Types.BinaryType.get()));
    assertThat(FILE.enclosingId()).isEqualTo(5);
    assertThat(Types.FileType.NUM_NESTED_FIELDS).isEqualTo(FILE.fields().size());
  }

  @Test
  void rejectsANegativeEnclosingId() {
    assertThatThrownBy(() -> Types.FileType.of(-1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid enclosing field ID: -1 < 0");
  }

  @Test
  void rejectsAnEnclosingIdThatCannotReserveNestedIds() {
    int lastEnclosingId = Integer.MAX_VALUE - Types.FileType.NUM_NESTED_FIELDS;

    assertThat(Types.FileType.of(lastEnclosingId).fields())
        .last()
        .extracting(Types.NestedField::fieldId)
        .isEqualTo(Integer.MAX_VALUE);

    assertThatThrownBy(() -> Types.FileType.of(lastEnclosingId + 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid enclosing field ID: %s > %s (cannot reserve %s nested field IDs)",
            lastEnclosingId + 1, lastEnclosingId, Types.FileType.NUM_NESTED_FIELDS);
  }

  @Test
  void rejectsASchemaWhereAnotherColumnHoldsADerivedId() {
    assertThatThrownBy(
            () ->
                new Schema(
                    required(1, "id", Types.LongType.get()),
                    optional(2, "photo", Types.FileType.of(2)),
                    optional(3, "data", Types.StringType.get())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file column photo: derived field ID 3 is already used by data");
  }

  @Test
  void rejectsASchemaWhereTheLastDerivedIdIsHeldByANestedColumn() {
    int lastDerivedId = 2 + Types.FileType.NUM_NESTED_FIELDS;

    assertThatThrownBy(
            () ->
                new Schema(
                    optional(2, "photo", Types.FileType.of(2)),
                    optional(
                        20,
                        "media",
                        Types.StructType.of(
                            optional(lastDerivedId, "caption", Types.StringType.get())))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid file column photo: derived field ID %s is already used by media.caption",
            lastDerivedId);
  }

  @Test
  void rejectsASchemaWhereAListElementFileOverlapsAnotherColumn() {
    assertThatThrownBy(
            () ->
                new Schema(
                    optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(2))),
                    optional(4, "data", Types.StringType.get())))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "Invalid file column photos.element: derived field ID 4 is already used by data");
  }

  @Test
  void rejectsASchemaWhereTwoFileColumnsOverlap() {
    assertThatThrownBy(
            () ->
                new Schema(
                    optional(1, "photo", Types.FileType.of(1)),
                    optional(2, "thumbnail", Types.FileType.of(2))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file column photo: derived field ID 2 is already used by thumbnail");
  }

  @Test
  void rejectsASchemaWithUnderivedNestedIds() {
    assertThatThrownBy(() -> new Schema(optional(5, "photo", Types.FileType.of(9))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file column photo: nested field IDs are derived from 5, not 9");
  }

  @Test
  void rejectsASchemaWithUnderivedNestedIdsInAMap() {
    assertThatThrownBy(
            () ->
                new Schema(
                    optional(
                        1,
                        "byName",
                        Types.MapType.ofOptional(
                            2, 3, Types.StringType.get(), Types.FileType.of(9)))))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file column byName.value: nested field IDs are derived from 3, not 9");
  }

  @Test
  void isHandledAsAStruct() {
    assertThat(FILE.typeId()).isEqualTo(Type.TypeID.STRUCT);
    assertThat(FILE.isStructType()).isTrue();
    assertThat(FILE.isNestedType()).isTrue();
    assertThat(FILE.asStructType()).isSameAs(FILE);
  }

  @Test
  void isDistinguishableFromAStruct() {
    assertThat(FILE.isFileType()).isTrue();
    assertThat(FILE.asFileType()).isSameAs(FILE);

    Types.StructType struct = Types.StructType.of(FILE.fields());
    assertThat(struct.isFileType()).isFalse();
    assertThatThrownBy(struct::asFileType)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Not a file type:");
  }

  @Test
  void persistsAsASingleTypeName() {
    assertThat(FILE.toString()).isEqualTo(Types.FileType.NAME).isEqualTo("file");
    assertThat(optional(5, "photo", FILE)).hasToString("5: photo: optional file");
  }

  @Test
  void isNotEqualToAStructWithTheSameFields() {
    Types.StructType struct = Types.StructType.of(FILE.fields());

    assertThat(FILE).isNotEqualTo(struct);
    assertThat(struct).isNotEqualTo(FILE);
    assertThat(FILE.hashCode()).isNotEqualTo(struct.hashCode());
  }

  @Test
  void isNotEqualToAFileHeldByADifferentField() {
    assertThat(FILE).isEqualTo(Types.FileType.of(5)).isNotEqualTo(Types.FileType.of(12));
    assertThat(FILE.hashCode()).isNotEqualTo(Types.FileType.of(12).hashCode());
  }

  @Test
  void freshIdsReserveTheNestedIdBlock() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "photo", Types.FileType.of(2)),
            optional(9, "data", Types.StringType.get()));

    Schema assigned = TypeUtil.assignFreshIds(schema, new AtomicInteger(0)::incrementAndGet);

    assertThat(assigned.findField("id").fieldId()).isEqualTo(1);
    assertThat(assigned.findField("photo").fieldId()).isEqualTo(2);
    assertThat(assigned.findField("photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(assigned.findField("photo.uri").fieldId()).isEqualTo(3);
    assertThat(assigned.findField("photo.inline").fieldId()).isEqualTo(8);
    assertThat(assigned.findField("data").fieldId()).isEqualTo(9);
    assertThat(assigned.highestFieldId()).isEqualTo(9);
  }

  @Test
  void freshIdsHandleAdjacentFileColumns() {
    Schema schema =
        new Schema(
            optional(1, "photo", Types.FileType.of(1)),
            optional(8, "thumbnail", Types.FileType.of(8)));

    Schema assigned = TypeUtil.assignFreshIds(schema, new AtomicInteger(0)::incrementAndGet);

    assertThat(assigned.findField("photo").type()).isEqualTo(Types.FileType.of(1));
    assertThat(assigned.findField("thumbnail").type()).isEqualTo(Types.FileType.of(8));
    assertThat(assigned.highestFieldId()).isEqualTo(14);
    assertThat(TypeUtil.indexById(assigned.asStruct())).hasSize(14);
  }

  @Test
  void freshIdsReuseBaseSchemaIdsWhenTheBaseColumnIsAlsoAFile() {
    Schema base =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));
    Schema updated =
        new Schema(
            required(11, "id", Types.LongType.get()),
            optional(12, "photo", Types.FileType.of(12)),
            optional(19, "data", Types.StringType.get()));

    Schema assigned = TypeUtil.assignFreshIds(updated, base, new AtomicInteger(8)::incrementAndGet);

    assertThat(assigned.findField("id").fieldId()).isEqualTo(1);
    assertThat(assigned.findField("photo").fieldId()).isEqualTo(2);
    assertThat(assigned.findField("photo.uri").fieldId()).isEqualTo(3);
    assertThat(assigned.findField("photo.inline").fieldId()).isEqualTo(8);
    assertThat(assigned.findField("data").fieldId()).isEqualTo(9);
  }

  @Test
  void freshIdsReserveANewBlockWhenABaseColumnBecomesAFile() {
    Schema base =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(2, "photo", Types.StringType.get()),
            optional(3, "data", Types.StringType.get()));
    Schema updated =
        new Schema(
            required(11, "id", Types.LongType.get()),
            optional(12, "photo", Types.FileType.of(12)),
            optional(19, "data", Types.StringType.get()));

    Schema assigned = TypeUtil.assignFreshIds(updated, base, new AtomicInteger(3)::incrementAndGet);

    assertThat(assigned.findField("id").fieldId()).isEqualTo(1);
    assertThat(assigned.findField("data").fieldId()).isEqualTo(3);

    Types.NestedField photo = assigned.findField("photo");
    assertThat(photo.type()).isEqualTo(Types.FileType.of(photo.fieldId()));
    assertThat(assigned.findField("photo.uri").fieldId()).isEqualTo(photo.fieldId() + 1);
    assertThat(assigned.highestFieldId())
        .isEqualTo(photo.fieldId() + Types.FileType.NUM_NESTED_FIELDS);
    assertThat(TypeUtil.indexById(assigned.asStruct()))
        .hasSize(updated.columns().size() + Types.FileType.NUM_NESTED_FIELDS);
  }

  @Test
  void freshIdsReserveForFilesInListsAndMaps() {
    Schema schema =
        new Schema(
            optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(2))),
            optional(
                9,
                "byName",
                Types.MapType.ofOptional(10, 11, Types.StringType.get(), Types.FileType.of(11))));

    Schema assigned = TypeUtil.assignFreshIds(schema, new AtomicInteger(0)::incrementAndGet);

    assertThat(assigned.findField("photos.element").type()).isEqualTo(Types.FileType.of(3));
    assertThat(assigned.findField("photos.element.uri").fieldId()).isEqualTo(4);
    assertThat(assigned.findField("byName.value").type()).isEqualTo(Types.FileType.of(11));
    assertThat(assigned.findField("byName.value.uri").fieldId()).isEqualTo(12);
    assertThat(assigned.highestFieldId()).isEqualTo(17);
    assertThat(TypeUtil.indexById(assigned.asStruct())).hasSize(17);
  }

  @Test
  void freshIdsRejectAnAssignerThatSkipsTheReservedIds() {
    Schema schema = new Schema(optional(1, "photo", Types.FileType.of(1)));
    AtomicInteger counter = new AtomicInteger(0);

    assertThatThrownBy(() -> TypeUtil.assignFreshIds(schema, () -> counter.addAndGet(10)))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot reserve 6 IDs after 10: assigned 20");
  }

  @Test
  void assignedIdsRejectAnAssignerThatCannotReserve() {
    Schema schema = new Schema(optional(1, "photo", Types.FileType.of(1)));

    assertThatThrownBy(() -> TypeUtil.assignIds(schema.asStruct(), oldId -> oldId + 10))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot reserve 6 IDs after 1: reserving IDs is not supported");
  }

  @Test
  void reassignedConflictingIdsReserveTheNestedIdBlock() {
    List<Types.NestedField> columns =
        ImmutableList.of(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

    Schema schema =
        new Schema(
            columns,
            TypeUtil.reassignConflictingIds(
                ImmutableSet.of(2), ImmutableSet.of(1, 2, 3, 4, 5, 6, 7, 8)));

    Types.NestedField photo = schema.findField("photo");
    assertThat(photo.fieldId()).isEqualTo(9);
    assertThat(photo.type()).isEqualTo(Types.FileType.of(9));
    assertThat(schema.findField("photo.uri").fieldId()).isEqualTo(10);
    assertThat(schema.findField("photo.inline").fieldId()).isEqualTo(15);
    assertThat(schema.idsToReassigned()).containsEntry(2, 9).doesNotContainKey(3);
    assertThat(schema.idsToOriginal()).containsEntry(9, 2).doesNotContainKey(10);
  }

  @Test
  void reassignedConflictingIdsMoveAFileWhenTheNestedIdsAreInUse() {
    List<Types.NestedField> columns =
        ImmutableList.of(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

    // 5 falls inside the derived block 3-8 even though the file's own id is not conflicting
    Schema schema =
        new Schema(columns, TypeUtil.reassignConflictingIds(ImmutableSet.of(), ImmutableSet.of(5)));

    assertThat(schema.findField("id").fieldId()).isEqualTo(1);
    assertThat(schema.findField("photo").type()).isEqualTo(Types.FileType.of(6));
    assertThat(schema.findField("photo.uri").fieldId()).isEqualTo(7);
    assertThat(schema.findField("photo.inline").fieldId()).isEqualTo(12);
  }

  @Test
  void reassignedConflictingIdsKeepAFileWhenOnlyItsOwnIdIsInUse() {
    List<Types.NestedField> columns = ImmutableList.of(optional(2, "photo", Types.FileType.of(2)));

    Schema schema =
        new Schema(columns, TypeUtil.reassignConflictingIds(ImmutableSet.of(), ImmutableSet.of(2)));

    assertThat(schema.findField("photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(schema.findField("photo.uri").fieldId()).isEqualTo(3);
    assertThat(schema.findField("photo.inline").fieldId()).isEqualTo(8);
  }

  @Test
  void reassignedConflictingIdsSkipBlocksThatOverlapUsedIds() {
    List<Types.NestedField> columns = ImmutableList.of(optional(2, "photo", Types.FileType.of(2)));

    Schema schema =
        new Schema(
            columns, TypeUtil.reassignConflictingIds(ImmutableSet.of(), ImmutableSet.of(3, 9)));

    assertThat(schema.findField("photo").type()).isEqualTo(Types.FileType.of(10));
    assertThat(schema.findField("photo.uri").fieldId()).isEqualTo(11);
    assertThat(schema.findField("photo.inline").fieldId()).isEqualTo(16);
    assertThat(TypeUtil.indexById(schema.asStruct()).keySet()).doesNotContain(3, 9);
  }

  @Test
  void reassignedConflictingIdsDoNotHandOutAPreservedId() {
    TypeUtil.GetID getId =
        TypeUtil.reassignConflictingIds(ImmutableSet.of(9), ImmutableSet.of(1, 9));

    int preservedId = getId.get(2);
    int fileId = getId.get(9, Types.FileType.NUM_NESTED_FIELDS);

    assertThat(preservedId).isEqualTo(2);
    assertThat(fileId).isNotEqualTo(preservedId);
    assertThat(Types.FileType.of(fileId).fields())
        .extracting(Types.NestedField::fieldId)
        .doesNotContain(preservedId);
  }

  @Test
  void reassignedConflictingIdsKeepAPreservedIdOutOfANewFileBlock() {
    List<Types.NestedField> columns =
        ImmutableList.of(
            optional(2, "data", Types.StringType.get()),
            optional(9, "photo", Types.FileType.of(9)));

    // the caller does not report the preserved id 2 as used, so only the assigner knows it is taken
    Schema schema =
        new Schema(columns, TypeUtil.reassignConflictingIds(ImmutableSet.of(9), ImmutableSet.of()));

    Types.NestedField data = schema.findField("data");
    Types.NestedField photo = schema.findField("photo");
    assertThat(data.fieldId()).isEqualTo(2);
    assertThat(photo.type()).isEqualTo(Types.FileType.of(photo.fieldId()));
    assertThat(photo.type().asStructType().fields())
        .extracting(Types.NestedField::fieldId)
        .doesNotContain(data.fieldId());
    assertThat(TypeUtil.indexById(schema.asStruct()))
        .hasSize(columns.size() + Types.FileType.NUM_NESTED_FIELDS);
  }

  @Test
  void reassignedIdsComeFromTheSourceSchema() {
    Schema source =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));
    Schema unassigned =
        new Schema(
            required(11, "id", Types.LongType.get()), optional(12, "photo", Types.FileType.of(12)));

    Schema reassigned = TypeUtil.reassignIds(unassigned, source);

    assertThat(reassigned.asStruct()).isEqualTo(source.asStruct());
    assertThat(reassigned.findField("photo").type()).isEqualTo(Types.FileType.of(2));
  }

  @Test
  void refreshedIdsReserveTheNestedIdBlockForNewFileColumns() {
    Schema source = new Schema(required(1, "id", Types.LongType.get()));
    Schema unassigned =
        new Schema(
            required(11, "id", Types.LongType.get()),
            optional(12, "photo", Types.FileType.of(12)),
            optional(19, "data", Types.StringType.get()));

    Schema reassigned = TypeUtil.reassignOrRefreshIds(unassigned, source);

    assertThat(reassigned.findField("id").fieldId()).isEqualTo(1);
    Types.NestedField photo = reassigned.findField("photo");
    assertThat(photo.type()).isEqualTo(Types.FileType.of(photo.fieldId()));
    assertThat(reassigned.findField("photo.uri").fieldId()).isEqualTo(photo.fieldId() + 1);
    assertThat(reassigned.findField("data").fieldId())
        .isEqualTo(photo.fieldId() + Types.FileType.NUM_NESTED_FIELDS + 1);
    assertThat(TypeUtil.indexById(reassigned.asStruct())).hasSize(9);
  }
}
