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
package org.apache.iceberg.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Set;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

class TestStructProjection {

  // projected schema asks for the optional "middle" field nested inside "person"
  private static final StructType PROJECTED_STRUCT =
      StructType.of(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.required(
              2,
              "person",
              StructType.of(
                  NestedField.required(3, "first", Types.StringType.get()),
                  NestedField.optional(4, "middle", Types.StringType.get()),
                  NestedField.required(5, "last", Types.StringType.get()))));

  // data schema is missing the optional "middle" field inside the nested "person" struct
  private static final StructType DATA_STRUCT_MISSING_NESTED_FIELD =
      TypeUtil.selectNot(PROJECTED_STRUCT, Set.of(4));

  private static final StructType FILE_STRUCT =
      StructType.of(
          NestedField.required(1, "id", Types.LongType.get()),
          NestedField.optional(2, "photo", Types.FileType.of(2)));

  @Test
  void createAllowMissingAllowsMissingOptionalFieldInNestedStruct() {
    Row row = Row.of(1L, Row.of("John", "Doe"));

    StructProjection projection =
        StructProjection.createAllowMissing(DATA_STRUCT_MISSING_NESTED_FIELD, PROJECTED_STRUCT);
    projection.wrap(row);

    StructLike projectedPerson = projection.get(1, StructLike.class);
    assertThat(projectedPerson.get(0, String.class)).isEqualTo("John");
    assertThat(projectedPerson.get(1, String.class)).isNull();
    assertThat(projectedPerson.get(2, String.class)).isEqualTo("Doe");
  }

  @Test
  void createStillThrowsForMissingOptionalFieldInNestedStruct() {
    assertThatThrownBy(
            () -> StructProjection.create(DATA_STRUCT_MISSING_NESTED_FIELD, PROJECTED_STRUCT))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot find field");
  }

  @Test
  void projectsAWholeFileColumn() {
    StructType projected = TypeUtil.select(FILE_STRUCT, Set.of(2));

    assertThat(projected.field("photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(StructProjection.create(FILE_STRUCT, projected).projectedFields()).isEqualTo(1);
  }

  @Test
  void projectsASingleNestedFieldOfAFileColumn() {
    StructType projected = TypeUtil.select(FILE_STRUCT, Set.of(3));

    assertThat(projected.field("photo").type())
        .isEqualTo(StructType.of(NestedField.optional(3, "uri", Types.StringType.get())));
    assertThat(StructProjection.create(FILE_STRUCT, projected).projectedFields()).isEqualTo(1);
  }
}
