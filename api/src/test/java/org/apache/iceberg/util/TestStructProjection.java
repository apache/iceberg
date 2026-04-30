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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestStructProjection {

  @Test
  public void testAllowMissingNestedField() {
    StructType dataType =
        StructType.of(
            required(1, "id", Types.IntegerType.get()),
            required(2, "nested", StructType.of(required(3, "x", Types.StringType.get()))));

    StructType projectedType =
        StructType.of(
            required(1, "id", Types.IntegerType.get()),
            required(
                2,
                "nested",
                StructType.of(
                    required(3, "x", Types.StringType.get()),
                    optional(4, "y", Types.IntegerType.get()))));

    StructProjection projection = StructProjection.createAllowMissing(dataType, projectedType);

    Row nestedRow = Row.of("hello");
    Row row = Row.of(42, nestedRow);

    projection.wrap(row);

    assertThat(projection.get(0, Integer.class)).isEqualTo(42);

    StructLike projectedNested = projection.get(1, StructLike.class);
    assertThat(projectedNested).isNotNull();
    assertThat(projectedNested.get(0, String.class)).isEqualTo("hello");
    assertThat(projectedNested.get(1, Integer.class)).isNull();
  }

  @Test
  public void testNestedFieldThrowsWithoutAllowMissing() {
    StructType dataType =
        StructType.of(
            required(1, "id", Types.IntegerType.get()),
            required(2, "nested", StructType.of(required(3, "x", Types.StringType.get()))));

    StructType projectedType =
        StructType.of(
            required(1, "id", Types.IntegerType.get()),
            required(
                2,
                "nested",
                StructType.of(
                    required(3, "x", Types.StringType.get()),
                    optional(4, "y", Types.IntegerType.get()))));

    assertThatThrownBy(() -> StructProjection.create(dataType, projectedType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot find field");
  }

  @Test
  public void testAllowMissingDeeplyNested() {
    StructType dataType =
        StructType.of(
            required(
                1,
                "outer",
                StructType.of(
                    required(
                        2, "middle", StructType.of(required(3, "a", Types.StringType.get()))))));

    StructType projectedType =
        StructType.of(
            required(
                1,
                "outer",
                StructType.of(
                    required(
                        2,
                        "middle",
                        StructType.of(
                            required(3, "a", Types.StringType.get()),
                            optional(4, "b", Types.LongType.get()))))));

    StructProjection projection = StructProjection.createAllowMissing(dataType, projectedType);

    Row innerRow = Row.of("value");
    Row middleRow = Row.of(innerRow);
    Row outerRow = Row.of(middleRow);

    projection.wrap(outerRow);

    StructLike outer = projection.get(0, StructLike.class);
    assertThat(outer).isNotNull();
    StructLike middle = outer.get(0, StructLike.class);
    assertThat(middle).isNotNull();
    assertThat(middle.get(0, String.class)).isEqualTo("value");
    assertThat(middle.get(1, Long.class)).isNull();
  }

  @Test
  public void testNestedFieldsPresentWithAllowMissing() {
    StructType structType =
        StructType.of(
            required(1, "id", Types.IntegerType.get()),
            required(
                2,
                "nested",
                StructType.of(
                    required(3, "x", Types.StringType.get()),
                    optional(4, "y", Types.IntegerType.get()))));

    StructProjection projection = StructProjection.createAllowMissing(structType, structType);

    Row nestedRow = Row.of("hello", 100);
    Row row = Row.of(42, nestedRow);

    projection.wrap(row);

    assertThat(projection.get(0, Integer.class)).isEqualTo(42);

    StructLike projectedNested = projection.get(1, StructLike.class);
    assertThat(projectedNested).isNotNull();
    assertThat(projectedNested.get(0, String.class)).isEqualTo("hello");
    assertThat(projectedNested.get(1, Integer.class)).isEqualTo(100);
  }
}
