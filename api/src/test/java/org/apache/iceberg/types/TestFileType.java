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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.junit.jupiter.api.Test;

class TestFileType {
  private static final Types.FileType FILE = Types.FileType.of(5);

  @Test
  void nestedFieldsAreDerivedFromTheBaseId() {
    assertThat(FILE.fields())
        .containsExactly(
            optional(6, "uri", Types.StringType.get()),
            optional(7, "offset", Types.LongType.get()),
            optional(8, "size", Types.LongType.get()),
            optional(9, "content_type", Types.StringType.get()),
            optional(10, "checksum", Types.StringType.get()),
            optional(11, "inline", Types.BinaryType.get()));
    assertThat(FILE.baseId()).isEqualTo(5);
    assertThat(Types.FileType.NUM_NESTED_FIELDS).isEqualTo(FILE.fields().size());
  }

  @Test
  void findsNestedFieldsById() {
    assertThat(FILE.field(6)).isEqualTo(FILE.fields().get(0));
    assertThat(FILE.field(11)).isEqualTo(FILE.fields().get(5));

    assertThat(FILE.field(5)).isNull();
    assertThat(FILE.field(12)).isNull();
  }

  @Test
  void findsNestedFieldsByName() {
    assertThat(FILE.field("content_type")).isEqualTo(FILE.fields().get(3));
    assertThat(FILE.caseInsensitiveField("Content_Type")).isEqualTo(FILE.fields().get(3));
    assertThat(FILE.fieldType("content_type")).isEqualTo(Types.StringType.get());

    assertThat(FILE.field("missing")).isNull();
    assertThat(FILE.caseInsensitiveField("missing")).isNull();
    assertThat(FILE.fieldType("missing")).isNull();
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
  void toStringIsFile() {
    assertThat(FILE).hasToString("file");
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
  void isNotEqualToAFileWithDifferentBaseId() {
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
        .hasMessage("Failed to assign consecutive IDs 11-16, requested 11 and got 20");
  }

  @Test
  void assignedIdsRejectAnAssignerThatCannotReserve() {
    Schema schema = new Schema(optional(1, "photo", Types.FileType.of(1)));

    assertThatThrownBy(() -> TypeUtil.assignIds(schema.asStruct(), oldId -> oldId + 10))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot reserve 6 IDs for field 1: reserving IDs is not supported");
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
