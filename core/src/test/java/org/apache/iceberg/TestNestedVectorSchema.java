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
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.RestoreColumns;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestNestedVectorSchema {
  private static final Types.VectorType VECTOR =
      Types.VectorType.of(Types.VectorType.ElementType.FLOAT, 3);

  @Test
  void schemaJsonUsesCompactVectorNamesWithoutElementIds() {
    Schema schema =
        new Schema(
            optional(1, "a", VECTOR),
            optional(2, "b", VECTOR),
            optional(3, "s", Types.StructType.of(optional(4, "v", VECTOR))));

    String json = SchemaParser.toJson(schema);

    assertThat(json).contains("\"type\":\"vector(float, 3)\"").doesNotContain("element-id");
    assertThat(SchemaParser.fromJson(json).asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  void schemaUpdatesPreserveVectorsAndAllocateNoElementIds() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()), optional(2, "emb", VECTOR));

    Schema updated =
        new SchemaUpdate(schema, schema.highestFieldId())
            .addColumn("other", VECTOR)
            .updateColumnDoc("emb", "existing vector")
            .renameColumn("emb", "embedding")
            .apply();

    assertThat(updated.findType("embedding")).isEqualTo(VECTOR);
    assertThat(updated.findField("embedding").doc()).isEqualTo("existing vector");
    assertThat(updated.findType("other")).isEqualTo(VECTOR);
    assertThat(updated.highestFieldId()).isEqualTo(3);
    assertThat(TypeUtil.indexById(updated.asStruct())).hasSize(3);
  }

  @Test
  void vectorColumnsCanBeDeleted() {
    Schema schema = new Schema(required(1, "id", Types.LongType.get()), optional(2, "emb", VECTOR));

    Schema updated = new SchemaUpdate(schema, 2).deleteColumn("emb").apply();

    assertThat(updated.asStruct())
        .isEqualTo(new Schema(required(1, "id", Types.LongType.get())).asStruct());
  }

  @Test
  void unionByNamePreservesVectorsAndAddsVectorColumnsWithoutElementIds() {
    Schema original = new Schema(optional(1, "emb", VECTOR));
    Schema incoming = new Schema(optional(10, "emb", VECTOR), optional(11, "other", VECTOR));

    Schema merged = new SchemaUpdate(original, 1).unionByNameWith(incoming).apply();

    assertThat(merged.findType("emb")).isEqualTo(VECTOR);
    assertThat(merged.findType("other")).isEqualTo(VECTOR);
    assertThat(merged.highestFieldId()).isEqualTo(2);
  }

  @Test
  void unionByNameRejectsVectorDimensionChanges() {
    Schema original = new Schema(optional(1, "emb", VECTOR));
    Schema incoming = new Schema(optional(1, "emb", Types.VectorType.of(VECTOR.elementType(), 4)));

    assertThatThrownBy(() -> new SchemaUpdate(original, 1).unionByNameWith(incoming).apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot change vector type");
  }

  @Test
  void vectorElementsAreNotIndependentlyAddressable() {
    Schema schema = new Schema(optional(1, "emb", VECTOR));

    assertThatThrownBy(
            () -> new SchemaUpdate(schema, 1).renameColumn("emb.element", "coordinate").apply())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot rename missing column: emb.element");
  }

  @Test
  void metricsEligibilityUsesVectorColumnIds() {
    Schema schema =
        new Schema(
            optional(1, "emb", VECTOR),
            optional(2, "s", Types.StructType.of(optional(3, "v", VECTOR))),
            optional(4, "arr", Types.ListType.ofRequired(5, VECTOR)));

    assertThat(MetricsConfig.limitFieldIds(schema, 1)).containsExactly(1);
    assertThat(MetricsConfig.limitFieldIds(schema, 10)).containsExactlyInAnyOrder(1, 3, 5);
  }

  @Test
  void nameMappingContainsNoSyntheticElement() {
    NameMapping mapping = MappingUtil.create(new Schema(optional(1, "emb", VECTOR)));

    assertThat(mapping.find("emb").id()).isEqualTo(1);
    assertThat(mapping.find("emb", "element")).isNull();
    assertThat(mapping.find(2)).isNull();
  }

  @Test
  void restoringAnotherColumnPreservesProjectedVectors() {
    Schema original =
        new Schema(required(1, "id", Types.LongType.get()), optional(2, "emb", VECTOR));
    Schema projected = TypeUtil.project(original, Set.of(2));

    assertThat(RestoreColumns.restore(original, projected, Set.of(1)).asStruct())
        .isEqualTo(original.asStruct());
  }

  @Test
  void avroConversionRejectsUnsupportedVectorsExplicitly() {
    assertThatThrownBy(() -> AvroSchemaUtil.convert(new Schema(optional(1, "emb", VECTOR)), "row"))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessageContaining("vector(float, 3)");
  }
}
