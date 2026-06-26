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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.types.Types.DoubleType;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StringType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.jupiter.api.Test;

public class TestStructProjection {
  @Test
  public void projectSubsetOfSchemaFields() {
    Schema dataSchema =
        new Schema(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    Schema projectedSchema = new Schema(required(30, "value", IntegerType.get()));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    assertThat(projection.get(0, Integer.class)).isNull();

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);
    projection.wrap(row);

    assertThat(projection.size()).isEqualTo(1);
    assertThat(projection.projectedFields()).isEqualTo(1);
    assertThat(projection.get(0, Integer.class)).isEqualTo(42);
  }

  @Test
  public void getThrowsClassCastExceptionForWrongType() {
    Schema dataSchema =
        new Schema(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    Schema projectedSchema = new Schema(required(30, "value", IntegerType.get()));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    assertThat(projection.get(0, Integer.class)).isNull();

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);
    projection.wrap(row);

    assertThatThrownBy(() -> projection.get(0, String.class))
        .isInstanceOf(ClassCastException.class)
        .hasMessage("Cannot cast java.lang.Integer to java.lang.String");
  }

  @Test
  public void getThrowsForOutOfBoundsIndex() {
    Schema dataSchema =
        new Schema(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    Schema projectedSchema = new Schema(required(30, "value", IntegerType.get()));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);
    projection.wrap(row);

    assertThatThrownBy(() -> projection.get(1, Integer.class))
        .isInstanceOf(ArrayIndexOutOfBoundsException.class)
        .hasMessage("Index 1 out of bounds for length 1");
  }

  @Test
  public void setOnProjectionThrows() {
    Schema dataSchema =
        new Schema(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    Schema projectedSchema = new Schema(required(30, "value", IntegerType.get()));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    assertThat(projection.get(0, Integer.class)).isNull();

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);
    projection.wrap(row);

    assertThatThrownBy(() -> projection.set(0, 5))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot set fields in a TypeProjection");
  }

  @Test
  public void projectNestedStructSubfields() {
    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(
                2,
                "address",
                StructType.of(
                    required(10, "street", StringType.get()),
                    required(
                        20,
                        "coordinates",
                        StructType.of(
                            required(100, "latitude", DoubleType.get()),
                            required(200, "longitude", DoubleType.get()))))));

    Schema projectedSchema =
        new Schema(
            required(
                2,
                "address",
                StructType.of(
                    required(
                        20,
                        "coordinates",
                        StructType.of(required(200, "longitude", DoubleType.get()))))));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row coordinates = TestHelpers.Row.of(42.00, 24.00);
    TestHelpers.Row address = TestHelpers.Row.of("221B Baker Street", coordinates);
    TestHelpers.Row row = TestHelpers.Row.of(1L, address);

    projection.wrap(row);

    StructLike addressProjection = projection.get(0, StructLike.class);
    assertThat(addressProjection).isNotNull();
    assertThat(addressProjection.size()).isEqualTo(1);

    StructLike coordinatesProjection = addressProjection.get(0, StructLike.class);
    assertThat(coordinatesProjection).isNotNull();
    assertThat(coordinatesProjection.size()).isEqualTo(1);
    assertThat(projection.projectedFields()).isEqualTo(1);
    assertThat(coordinatesProjection.get(0, Double.class)).isEqualTo(24.00);
  }

  @Test
  public void createAllowMissingWithAbsentOptionalFieldReturnsNull() {
    Schema dataSchema = new Schema(required(10, "id", LongType.get()));

    StructType projectedStructType =
        StructType.of(
            required(10, "id", LongType.get()), NestedField.optional(20, "name", StringType.get()));

    StructProjection projection =
        StructProjection.createAllowMissing(dataSchema.asStruct(), projectedStructType);

    TestHelpers.Row row = TestHelpers.Row.of(1L);
    projection.wrap(row);

    assertThat(projection.size()).isEqualTo(2);
    assertThat(projection.projectedFields()).isEqualTo(1);
    assertThat(projection.get(0, Long.class)).isEqualTo(1L);
    assertThat(projection.get(1, String.class)).isNull();
  }

  @Test
  public void createThrowsWhenOptionalFieldAbsent() {
    Schema dataSchema = new Schema(required(10, "id", LongType.get()));

    StructType projectedStructType =
        StructType.of(
            required(10, "id", LongType.get()), NestedField.optional(20, "name", StringType.get()));

    assertThatThrownBy(() -> StructProjection.create(dataSchema.asStruct(), projectedStructType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field 20: name: optional string in struct<10: id: required long>");
  }

  @Test
  public void createAllowMissingThrowsWhenRequiredFieldAbsent() {
    Schema dataSchema = new Schema(required(10, "id", LongType.get()));

    StructType projectedStructType =
        StructType.of(required(10, "id", LongType.get()), required(20, "name", StringType.get()));

    assertThatThrownBy(
            () -> StructProjection.createAllowMissing(dataSchema.asStruct(), projectedStructType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field 20: name: required string in struct<10: id: required long>");
  }

  @Test
  public void createThrowsWhenRequiredFieldAbsent() {
    Schema dataSchema = new Schema(required(10, "id", LongType.get()));

    StructType projectedStructType =
        StructType.of(required(10, "id", LongType.get()), required(20, "name", StringType.get()));

    assertThatThrownBy(() -> StructProjection.create(dataSchema.asStruct(), projectedStructType))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot find field 20: name: required string in struct<10: id: required long>");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void projectMapWithNestedStructAndPrimitiveValues() {
    StructType coordinatesStruct =
        StructType.of(
            required(100, "latitude", DoubleType.get()),
            required(200, "longitude", DoubleType.get()));

    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(2, "address", MapType.ofRequired(3, 4, StringType.get(), coordinatesStruct)),
            required(5, "metadata", MapType.ofRequired(6, 7, StringType.get(), StringType.get())));
    Schema projectedSchema =
        new Schema(
            required(2, "address", MapType.ofRequired(3, 4, StringType.get(), coordinatesStruct)),
            required(5, "metadata", MapType.ofRequired(6, 7, StringType.get(), StringType.get())));
    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row coordinates = TestHelpers.Row.of(42.00, 24.00);
    Map<String, TestHelpers.Row> location = Map.of("home", coordinates);
    Map<String, String> userDetail = Map.of("type", "administrator");
    TestHelpers.Row row = TestHelpers.Row.of(1L, location, userDetail);

    projection.wrap(row);

    assertThat(projection.size()).isEqualTo(2);
    assertThat(projection.projectedFields()).isEqualTo(2);

    assertThat(projection.get(1, Map.class)).isEqualTo(Map.of("type", "administrator"));

    Map<String, TestHelpers.Row> projectedLocations = projection.get(0, Map.class);
    assertThat(projectedLocations).isNotNull().containsKey("home");

    TestHelpers.Row projectedCoordinatesRow = projectedLocations.get("home");
    assertThat(projectedCoordinatesRow.get(0, Double.class)).isNotNull().isEqualTo(42.00);
    assertThat(projectedCoordinatesRow.get(1, Double.class)).isNotNull().isEqualTo(24.00);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void projectListWithPrimitiveAndStructElements() {
    StructType elementStruct = StructType.of(required(200, "point", DoubleType.get()));
    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(2, "numbers", ListType.ofRequired(3, StringType.get())),
            required(4, "points", ListType.ofRequired(5, elementStruct)));
    Schema projectedSchema =
        new Schema(
            required(2, "numbers", ListType.ofRequired(3, StringType.get())),
            required(4, "points", ListType.ofRequired(5, elementStruct)));
    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row points = TestHelpers.Row.of(1.0, 2.0);
    TestHelpers.Row row = TestHelpers.Row.of(1L, List.of("a", "b", "c"), List.of(points));
    projection.wrap(row);

    assertThat(projection.size()).isEqualTo(2);
    assertThat(projection.projectedFields()).isEqualTo(2);
    assertThat(projection.get(0, List.class)).containsExactly("a", "b", "c");
    assertThat(projection.get(1, List.class)).containsExactly(points);
  }

  @Test
  public void createProjectionFromFieldIds() {
    Schema dataSchema =
        new Schema(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    StructProjection projection = StructProjection.create(dataSchema, Set.of(30, 20));

    TestHelpers.Row row = TestHelpers.Row.of(1L, "Batman", 42);
    projection.wrap(row);

    assertThat(projection.size()).isEqualTo(2);
    assertThat(projection.projectedFields()).isEqualTo(2);
    assertThat(projection.get(0, String.class)).isEqualTo("Batman");
    assertThat(projection.get(1, Integer.class)).isEqualTo(42);
  }

  @Test
  public void copyForCreatesIndependentProjection() {
    StructType dataSchema =
        StructType.of(
            required(10, "id", LongType.get()),
            required(20, "name", StringType.get()),
            required(30, "value", IntegerType.get()));

    StructType projectedSchema = StructType.of(required(30, "value", IntegerType.get()));
    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);

    TestHelpers.Row row1 = TestHelpers.Row.of(1L, "Batman", 42);
    TestHelpers.Row row2 = TestHelpers.Row.of(1L, "Ironman", 3000);
    TestHelpers.Row row3 = TestHelpers.Row.of(1L, "Spiderman", 8);

    projection.wrap(row1);
    StructProjection copyProjection = projection.copyFor(row2);
    projection.wrap(row3);

    assertThat(projection.size()).isEqualTo(1);
    assertThat(projection.projectedFields()).isEqualTo(1);
    assertThat(copyProjection.get(0, Integer.class)).isEqualTo(3000);
  }

  @Test
  public void createThrowsForPartialMapValueStructProjection() {
    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(
                2,
                "address",
                MapType.ofRequired(
                    3,
                    4,
                    StringType.get(),
                    StructType.of(
                        required(100, "latitude", DoubleType.get()),
                        required(200, "longitude", DoubleType.get())))));
    Schema projectedSchema =
        new Schema(
            required(
                2,
                "address",
                MapType.ofRequired(
                    3,
                    4,
                    StringType.get(),
                    StructType.of(required(200, "longitude", DoubleType.get())))));

    assertThatThrownBy(() -> StructProjection.create(dataSchema, projectedSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial map key or value struct");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void projectMapWithNestedStructKey() {
    StructType keyStruct = StructType.of(required(100, "region", StringType.get()));

    Schema dataSchema =
        new Schema(required(2, "data", MapType.ofRequired(3, 4, keyStruct, StringType.get())));

    StructProjection projection = StructProjection.create(dataSchema, dataSchema);
    TestHelpers.Row key = TestHelpers.Row.of("US");
    projection.wrap(TestHelpers.Row.of(Map.of(key, "A Country")));

    assertThat(projection.size()).isEqualTo(1);
    assertThat(projection.projectedFields()).isEqualTo(1);
    assertThat(projection.get(0, Map.class)).isEqualTo(Map.of(key, "A Country"));
  }

  @Test
  public void createThrowsForPartialMapKeyStructProjection() {
    Schema dataSchema =
        new Schema(
            required(
                1,
                "delivery_map",
                MapType.ofRequired(
                    2,
                    3,
                    StructType.of(
                        required(100, "city", StringType.get()),
                        required(101, "zip", IntegerType.get())),
                    StringType.get())));

    Schema projectedSchema =
        new Schema(
            required(
                1,
                "delivery_map",
                MapType.ofRequired(
                    2,
                    3,
                    StructType.of(required(100, "city", StringType.get())),
                    StringType.get())));

    assertThatThrownBy(() -> StructProjection.create(dataSchema, projectedSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial map key or value struct");
  }

  @Test
  public void createThrowsForPartialListElementStructProjection() {
    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(
                2,
                "address",
                ListType.ofRequired(
                    3,
                    StructType.of(
                        required(100, "latitude", DoubleType.get()),
                        required(200, "longitude", DoubleType.get())))));
    Schema projectedSchema =
        new Schema(
            required(
                2,
                "address",
                ListType.ofRequired(
                    3, StructType.of(required(200, "longitude", DoubleType.get())))));

    assertThatThrownBy(() -> StructProjection.create(dataSchema, projectedSchema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial list element struct");
  }

  @Test
  public void getReturnsNullForNullNestedStruct() {
    Schema dataSchema =
        new Schema(
            required(1, "id", LongType.get()),
            required(2, "address", StructType.of(required(10, "street", StringType.get()))));
    Schema projectedSchema =
        new Schema(required(2, "address", StructType.of(required(10, "street", StringType.get()))));

    StructProjection projection = StructProjection.create(dataSchema, projectedSchema);
    TestHelpers.Row row = TestHelpers.Row.of(1L, null);
    projection.wrap(row);

    assertThat(projection.get(0, StructLike.class)).isNull();
  }

  @Test
  public void createAllowMissingPropagatesAllowMissingToNestedStructs() {
    StructType dataAddressType = StructType.of(required(10, "street", StringType.get()));

    StructType projectedAddressType =
        StructType.of(
            required(10, "street", StringType.get()),
            NestedField.optional(20, "city", StringType.get()));

    StructType dataStructType =
        StructType.of(required(1, "id", LongType.get()), required(2, "address", dataAddressType));

    StructType projectedStructType =
        StructType.of(
            required(1, "id", LongType.get()), required(2, "address", projectedAddressType));

    StructProjection projection =
        StructProjection.createAllowMissing(dataStructType, projectedStructType);

    TestHelpers.Row nestedRow = TestHelpers.Row.of("123 Main St");
    TestHelpers.Row row = TestHelpers.Row.of(42L, nestedRow);
    projection.wrap(row);

    assertThat(projection.get(0, Long.class)).isEqualTo(42L);
    StructLike addressProjection = projection.get(1, StructLike.class);
    assertThat(addressProjection.get(0, String.class)).isEqualTo("123 Main St");
    assertThat(addressProjection.get(1, String.class)).isNull();
  }
}
