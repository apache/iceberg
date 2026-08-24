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
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class TestFileType {
  private static final Types.FileType FILE = Types.FileType.of(5);

  @Test
  void nestedFieldsAreDerivedFromTheHoldingId() {
    assertThat(FILE.fields())
        .containsExactly(
            optional(6, "uri", Types.StringType.get()),
            optional(7, "offset", Types.LongType.get()),
            optional(8, "size", Types.LongType.get()),
            optional(9, "content_type", Types.StringType.get()),
            optional(10, "checksum", Types.StringType.get()),
            optional(11, "inline", Types.BinaryType.get()));
    assertThat(FILE.fieldId()).isEqualTo(5);
    assertThat(Types.FileType.NUM_NESTED_FIELDS).isEqualTo(FILE.fields().size());
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
  void isNotResolvedByName() {
    assertThatThrownBy(() -> Types.fromTypeName("file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse type string to primitive: file");
    assertThatThrownBy(() -> Types.fromPrimitiveString("file"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse type string to primitive: file");
  }

  @Test
  void survivesJavaSerialization() throws Exception {
    Type copy = TestHelpers.roundTripSerialize(FILE);

    assertThat(copy).isEqualTo(FILE);
    assertThat(copy.isFileType()).isTrue();
    assertThat(copy.asFileType().fieldId()).isEqualTo(5);
  }

  @Test
  void rejectsDefaultValues() {
    assertThatThrownBy(
            () ->
                Types.NestedField.optional("photo")
                    .withId(5)
                    .ofType(FILE)
                    .withWriteDefault(Expressions.lit("s3://bucket/key"))
                    .build())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Invalid default value for file:");
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
  void freshIdsReuseBaseSchemaIdsWithoutReserving() {
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
    assertThat(assigned.findField("data").fieldId()).isEqualTo(9);
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

  @Test
  void isRejectedBeforeFormatVersion4() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

    for (int version = 1; version < 4; version += 1) {
      int formatVersion = version;
      assertThatThrownBy(() -> Schema.checkCompatibility(schema, formatVersion))
          .isInstanceOf(IllegalStateException.class)
          .hasMessage(
              "Invalid schema for v"
                  + formatVersion
                  + ":\n- Invalid type for photo: file is not supported until v4");
    }

    Schema.checkCompatibility(schema, 4);
  }

  @Test
  void cannotBeReadAsAStruct() {
    Schema fileSchema = new Schema(optional(1, "photo", Types.FileType.of(1)));
    Schema structSchema = new Schema(optional(1, "photo", Types.StructType.of(FILE.fields())));

    List<String> asFile = CheckCompatibility.readCompatibilityErrors(fileSchema, structSchema);
    assertThat(asFile).hasSize(1);
    assertThat(asFile.get(0)).contains("cannot be read as a file");

    List<String> asStruct = CheckCompatibility.readCompatibilityErrors(structSchema, fileSchema);
    assertThat(asStruct).hasSize(1);
    assertThat(asStruct.get(0)).contains("file cannot be read as a struct");

    assertThat(CheckCompatibility.readCompatibilityErrors(fileSchema, fileSchema)).isEmpty();
  }

  @Test
  void reassignDocKeepsTheFileType() {
    Schema schema = new Schema(optional(2, "photo", Types.FileType.of(2)));
    Schema docs = new Schema(optional(2, "photo", Types.FileType.of(2), "image"));

    Schema actual = TypeUtil.reassignDoc(schema, docs);

    assertThat(actual.findField("photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(actual.findField("photo").doc()).isEqualTo("image");
  }

  @Test
  void projectKeepsTheFileTypeWhenAllNestedFieldsRemain() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

    Schema projected = TypeUtil.project(schema, ImmutableSet.of(3, 4, 5, 6, 7, 8));

    assertThat(projected.findField("photo").type()).isEqualTo(Types.FileType.of(2));
  }

  @Test
  void projectDropsTheFileTypeWhenNestedFieldsArePruned() {
    Schema schema = new Schema(optional(2, "photo", Types.FileType.of(2)));

    Schema projected = TypeUtil.project(schema, ImmutableSet.of(3));

    assertThat(projected.findField("photo").type().isFileType()).isFalse();
    assertThat(projected.findField("photo").type().asStructType().fields())
        .containsExactly(optional(3, "uri", Types.StringType.get()));
  }

  @Test
  void replacingANestedFieldTypeDropsTheFileType() {
    Schema schema = new Schema(optional(2, "photo", Types.FileType.of(2)));

    Schema replaced =
        TypeUtil.replaceFieldTypes(schema, ImmutableMap.of(3, Types.BinaryType.get()));

    assertThat(replaced.findField("photo").type().isFileType()).isFalse();
    assertThat(replaced.findField("photo.uri").type()).isEqualTo(Types.BinaryType.get());
  }
}
