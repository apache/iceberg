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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class TestNestedVectorType {
  private static final Types.VectorType VECTOR =
      Types.VectorType.of(Types.VectorType.ElementType.FLOAT, 3);

  private static Schema schema() {
    return new Schema(
        required(1, "id", Types.LongType.get()),
        optional(2, "emb", VECTOR),
        optional(3, "s", Types.StructType.of(optional(4, "v", VECTOR))),
        optional(5, "arr", Types.ListType.ofRequired(6, VECTOR)),
        optional(7, "m", Types.MapType.ofRequired(8, 9, Types.StringType.get(), VECTOR)));
  }

  @Test
  void elementsHaveNoSchemaFields() {
    Schema schema = schema();

    assertThat(VECTOR.asNestedType().fields()).isEmpty();
    assertThat(VECTOR.fieldType("element")).isNull();
    assertThat(VECTOR.field(10)).isNull();
    assertThat(schema.findField("emb.element")).isNull();
    assertThat(TypeUtil.indexById(schema.asStruct())).hasSize(9);
    assertThat(TypeUtil.indexByName(schema.asStruct()))
        .doesNotContainKeys("emb.element", "s.v.element", "arr.element.element");
    assertThat(TypeUtil.indexParents(schema.asStruct()))
        .containsExactlyInAnyOrderEntriesOf(Map.of(4, 3, 6, 5, 8, 7, 9, 7));
    assertThat(schema.highestFieldId()).isEqualTo(9);
    assertThat(TypeUtil.getProjectedIds(VECTOR)).isEmpty();
  }

  @Test
  void freshIdsAreAllocatedOnlyForSchemaFields() {
    AtomicInteger nextId = new AtomicInteger(100);
    Schema fresh = TypeUtil.assignFreshIds(schema(), nextId::incrementAndGet);

    assertThat(nextId.get()).isEqualTo(109);
    assertThat(TypeUtil.indexById(fresh.asStruct())).hasSize(9);
    assertThat(fresh.findType("emb")).isEqualTo(VECTOR);
    assertThat(fresh.findType("s.v")).isEqualTo(VECTOR);
    assertThat(fresh.findType("arr.element")).isEqualTo(VECTOR);
    assertThat(fresh.findType("m.value")).isEqualTo(VECTOR);
  }

  @Test
  void explicitIdAssignmentPreservesVectors() {
    Types.StructType assigned =
        TypeUtil.assignIds(schema().asStruct(), id -> id + 100).asStructType();

    assertThat(assigned.asSchema().findField("emb").fieldId()).isEqualTo(102);
    assertThat(assigned.asSchema().findType("arr.element")).isEqualTo(VECTOR);
    assertThat(assigned.asSchema().highestFieldId()).isEqualTo(109);
  }

  @Test
  void idsCanBeReassignedFromAnotherSchema() {
    Schema source =
        TypeUtil.assignIds(schema().asStruct(), id -> id + 100).asStructType().asSchema();

    assertThat(TypeUtil.reassignIds(schema(), source).asStruct()).isEqualTo(source.asStruct());
  }

  @Test
  void documentationCanBeReassignedWithoutChangingTheVector() {
    Schema original = new Schema(optional(1, "emb", VECTOR));
    Schema documented = new Schema(optional(1, "emb", VECTOR, "embedding"));

    assertThat(TypeUtil.reassignDoc(original, documented).asStruct())
        .isEqualTo(documented.asStruct());
  }

  @Test
  void projectionKeepsSelectedVectorsOnly() {
    Schema projected = TypeUtil.project(schema(), Set.of(2, 4));

    assertThat(projected.asStruct())
        .isEqualTo(
            new Schema(
                    optional(2, "emb", VECTOR),
                    optional(3, "s", Types.StructType.of(optional(4, "v", VECTOR))))
                .asStruct());
    assertThat(TypeUtil.getProjectedIds(projected)).containsExactlyInAnyOrder(2, 3, 4);
    assertThat(TypeUtil.project(schema(), Set.of(1)).findField("emb")).isNull();
    assertThat(TypeUtil.project(schema(), Set.of(6)).findType("arr.element")).isEqualTo(VECTOR);
    assertThat(TypeUtil.project(schema(), Set.of(9)).findType("m.value")).isEqualTo(VECTOR);
  }

  @Test
  void projectionAndAccessorsReturnTheCompleteVectorValue() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()), optional(2, "emb", VECTOR));
    List<Float> value = List.of(1.0F, 2.0F, 3.0F);
    TestHelpers.Row row = TestHelpers.Row.of(7L, value);
    Schema projected = TypeUtil.project(schema, Set.of(2));

    assertThat(schema.accessorForField(2).get(row)).isSameAs(value);
    assertThat(StructProjection.create(schema, projected).wrap(row).get(0, List.class))
        .isSameAs(value);
  }

  @Test
  void replacingAnotherFieldPreservesVectors() {
    Schema replaced = TypeUtil.replaceFieldTypes(schema(), Map.of(1, Types.StringType.get()));

    assertThat(replaced.findType("id")).isEqualTo(Types.StringType.get());
    assertThat(replaced.findType("emb")).isEqualTo(VECTOR);
    assertThat(replaced.findType("m.value")).isEqualTo(VECTOR);
  }

  @Test
  void typeSearchFindsVectorsWithoutInventingAnElementField() {
    assertThat(TypeUtil.find(schema(), Type::isVectorType)).isEqualTo(VECTOR);
    assertThat(TypeUtil.find(VECTOR, type -> type.typeId() == Type.TypeID.FLOAT)).isNull();
  }

  @ParameterizedTest
  @EnumSource(Types.VectorType.ElementType.class)
  void identicalVectorTypesAreCompatible(Types.VectorType.ElementType elementType) {
    Schema schema = new Schema(optional(1, "emb", Types.VectorType.of(elementType, 3)));

    assertThat(CheckCompatibility.readCompatibilityErrors(schema, schema)).isEmpty();
    assertThat(CheckCompatibility.writeCompatibilityErrors(schema, schema)).isEmpty();
  }

  @Test
  void dimensionChangesAreIncompatible() {
    Schema original = new Schema(optional(1, "emb", VECTOR));
    Schema changed = new Schema(optional(1, "emb", Types.VectorType.of(VECTOR.elementType(), 4)));

    assertThat(CheckCompatibility.readCompatibilityErrors(changed, original)).isNotEmpty();
  }

  @Test
  void elementTypeChangesAreIncompatible() {
    Schema original = new Schema(optional(1, "emb", VECTOR));
    Schema changed =
        new Schema(optional(1, "emb", Types.VectorType.of(Types.VectorType.ElementType.DOUBLE, 3)));

    assertThat(CheckCompatibility.readCompatibilityErrors(changed, original)).isNotEmpty();
  }

  @Test
  void listsAndVectorsAreIncompatible() {
    Schema vector = new Schema(optional(1, "emb", VECTOR));
    Schema list =
        new Schema(optional(1, "emb", Types.ListType.ofRequired(2, Types.FloatType.get())));

    assertThat(CheckCompatibility.readCompatibilityErrors(vector, list)).isNotEmpty();
    assertThat(CheckCompatibility.readCompatibilityErrors(list, vector)).isNotEmpty();
  }

  @Test
  void vectorCannotBeAnIdentifierField() {
    assertThatThrownBy(() -> new Schema(List.of(required(1, "emb", VECTOR)), Set.of(1)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("not a primitive type field");
  }
}
