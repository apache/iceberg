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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.DataGenerator;
import org.apache.iceberg.flink.DataGenerators;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructProjection;
import org.junit.jupiter.api.Test;

public class TestRowDataProjection {
  @Test
  public void testNullRootRowData() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    RowDataProjection projection = RowDataProjection.create(schema, schema.select("id"));

    assertThatThrownBy(() -> projection.wrap(null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid row data: null");
  }

  @Test
  public void testFullProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    generateAndValidate(schema, schema);

    GenericRowData rowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData copyRowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData otherRowData = GenericRowData.of(2L, StringData.fromString("b"));
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);
  }

  @Test
  public void testReorderedFullProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Schema reordered =
        new Schema(
            Types.NestedField.optional(1, "data", Types.StringType.get()),
            Types.NestedField.required(0, "id", Types.LongType.get()));

    generateAndValidate(schema, reordered);

    GenericRowData rowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData copyRowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData otherRowData = GenericRowData.of(2L, StringData.fromString("b"));
    testEqualsAndHashCode(schema, reordered, rowData, copyRowData, otherRowData);
  }

  @Test
  public void testBasicProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));
    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));
    Schema dataOnly = new Schema(Types.NestedField.optional(1, "data", Types.StringType.get()));
    generateAndValidate(schema, idOnly);
    generateAndValidate(schema, dataOnly);

    GenericRowData rowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData copyRowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData otherRowData = GenericRowData.of(2L, StringData.fromString("b"));
    testEqualsAndHashCode(schema, idOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, dataOnly, rowData, copyRowData, otherRowData);
  }

  @Test
  public void testEmptyProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));
    generateAndValidate(schema, schema.select());

    GenericRowData rowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData copyRowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData otherRowData = GenericRowData.of(2L, StringData.fromString("b"));
    testEqualsAndHashCode(schema, schema.select(), rowData, copyRowData, otherRowData, true);
  }

  @Test
  public void testRename() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "data", Types.StringType.get()));

    Schema renamed =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(1, "renamed", Types.StringType.get()));
    generateAndValidate(schema, renamed);

    GenericRowData rowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData copyRowData = GenericRowData.of(1L, StringData.fromString("a"));
    GenericRowData otherRowData = GenericRowData.of(2L, StringData.fromString("b"));
    testEqualsAndHashCode(schema, renamed, rowData, copyRowData, otherRowData);
  }

  @Test
  public void testNestedProjection() {
    Schema schema =
        new Schema(
            Types.NestedField.required(0, "id", Types.LongType.get()),
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(
                    Types.NestedField.required(1, "lat", Types.FloatType.get()),
                    Types.NestedField.required(2, "long", Types.FloatType.get()))));

    GenericRowData rowData = GenericRowData.of(1L, GenericRowData.of(1.0f, 1.0f));
    GenericRowData copyRowData = GenericRowData.of(1L, GenericRowData.of(1.0f, 1.0f));
    GenericRowData otherRowData = GenericRowData.of(2L, GenericRowData.of(2.0f, 2.0f));

    GenericRowData rowDataNullStruct = GenericRowData.of(1L, null);
    GenericRowData copyRowDataNullStruct = GenericRowData.of(1L, null);
    GenericRowData otherRowDataNullStruct = GenericRowData.of(2L, null);

    // Project id only.
    Schema idOnly = new Schema(Types.NestedField.required(0, "id", Types.LongType.get()));
    assertThat(idOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, idOnly);
    testEqualsAndHashCode(schema, idOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(
        schema, idOnly, rowDataNullStruct, copyRowDataNullStruct, otherRowDataNullStruct);

    // Project lat only.
    Schema latOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(1, "lat", Types.FloatType.get()))));
    assertThat(latOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, latOnly);
    testEqualsAndHashCode(schema, latOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(
        schema, latOnly, rowDataNullStruct, copyRowDataNullStruct, otherRowDataNullStruct, true);

    // Project long only.
    Schema longOnly =
        new Schema(
            Types.NestedField.optional(
                3,
                "location",
                Types.StructType.of(Types.NestedField.required(2, "long", Types.FloatType.get()))));
    assertThat(longOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, longOnly);
    testEqualsAndHashCode(schema, longOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(
        schema, longOnly, rowDataNullStruct, copyRowDataNullStruct, otherRowDataNullStruct, true);

    // Project location.
    Schema locationOnly = schema.select("location");
    assertThat(locationOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, locationOnly);
    testEqualsAndHashCode(schema, locationOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(
        schema,
        locationOnly,
        rowDataNullStruct,
        copyRowDataNullStruct,
        otherRowDataNullStruct,
        true);
  }

  @Test
  public void testPrimitivesFullProjection() {
    DataGenerator dataGenerator = new DataGenerators.Primitives();
    Schema schema = dataGenerator.icebergSchema();
    generateAndValidate(schema, schema);

    GenericRowData rowData = dataGenerator.generateFlinkRowData();
    GenericRowData copyRowData = dataGenerator.generateFlinkRowData();
    GenericRowData otherRowData = dataGenerator.generateFlinkRowData();
    // modify the string field value (position 6)
    otherRowData.setField(6, StringData.fromString("foo_bar"));
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);

    GenericRowData rowDataNullOptionalFields = dataGenerator.generateFlinkRowData();
    setOptionalFieldsNullForPrimitives(rowDataNullOptionalFields);
    GenericRowData copyRowDataNullOptionalFields = dataGenerator.generateFlinkRowData();
    setOptionalFieldsNullForPrimitives(copyRowDataNullOptionalFields);
    GenericRowData otherRowDataNullOptionalFields = dataGenerator.generateFlinkRowData();
    // modify the string field value (position 6)
    otherRowDataNullOptionalFields.setField(6, StringData.fromString("foo_bar"));
    setOptionalFieldsNullForPrimitives(otherRowData);
    testEqualsAndHashCode(
        schema,
        schema,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
  }

  private void setOptionalFieldsNullForPrimitives(GenericRowData rowData) {
    // fields from [1, 5] range are optional
    for (int pos = 1; pos <= 5; ++pos) {
      rowData.setField(pos, null);
    }
  }

  @Test
  public void testMapOfPrimitivesProjection() {
    DataGenerator dataGenerator = new DataGenerators.MapOfPrimitives();
    Schema schema = dataGenerator.icebergSchema();

    // Project id only.
    Schema idOnly = schema.select("row_id");
    assertThat(idOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, idOnly);

    // Project map only.
    Schema mapOnly = schema.select("map_of_primitives");
    assertThat(mapOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);

    GenericRowData rowData = dataGenerator.generateFlinkRowData();
    GenericRowData copyRowData = dataGenerator.generateFlinkRowData();
    // modify the map field value
    GenericRowData otherRowData =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericMapData(
                ImmutableMap.of(StringData.fromString("foo"), 1, StringData.fromString("bar"), 2)));
    testEqualsAndHashCode(schema, idOnly, rowData, copyRowData, otherRowData, true);
    testEqualsAndHashCode(schema, mapOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);

    GenericRowData rowDataNullOptionalFields =
        GenericRowData.of(StringData.fromString("row_id_value"), null);
    GenericRowData copyRowDataNullOptionalFields =
        GenericRowData.of(StringData.fromString("row_id_value"), null);
    // modify the map field value
    GenericRowData otherRowDataNullOptionalFields =
        GenericRowData.of(StringData.fromString("other_row_id_value"), null);
    testEqualsAndHashCode(
        schema,
        idOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
    testEqualsAndHashCode(
        schema,
        mapOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields,
        true);
    testEqualsAndHashCode(
        schema,
        schema,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
  }

  @Test
  public void testMapOfStructStructProjection() {
    DataGenerator dataGenerator = new DataGenerators.MapOfStructStruct();
    Schema schema = dataGenerator.icebergSchema();

    // Project id only.
    Schema idOnly = schema.select("row_id");
    assertThat(idOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, idOnly);

    // Project map only.
    Schema mapOnly = schema.select("map");
    assertThat(mapOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, mapOnly);

    // Project all.
    generateAndValidate(schema, schema);

    // Project partial map key.
    Schema partialMapKey =
        new Schema(
            Types.NestedField.optional(
                2,
                "map",
                Types.MapType.ofOptional(
                    101,
                    102,
                    Types.StructType.of(
                        Types.NestedField.required(201, "key", Types.LongType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(203, "value", Types.LongType.get()),
                        Types.NestedField.required(204, "valueData", Types.StringType.get())))));
    assertThatThrownBy(() -> generateAndValidate(schema, partialMapKey))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial map key or value struct.");

    // Project partial map key.
    Schema partialMapValue =
        new Schema(
            Types.NestedField.optional(
                2,
                "map",
                Types.MapType.ofOptional(
                    101,
                    102,
                    Types.StructType.of(
                        Types.NestedField.required(201, "key", Types.LongType.get()),
                        Types.NestedField.required(202, "keyData", Types.StringType.get())),
                    Types.StructType.of(
                        Types.NestedField.required(203, "value", Types.LongType.get())))));
    assertThatThrownBy(() -> generateAndValidate(schema, partialMapValue))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial map key or value struct.");

    GenericRowData rowData = dataGenerator.generateFlinkRowData();
    GenericRowData copyRowData = dataGenerator.generateFlinkRowData();
    // modify the map field value
    GenericRowData otherRowData =
        GenericRowData.of(
            StringData.fromString("other_row_id_value"),
            new GenericMapData(
                ImmutableMap.of(
                    GenericRowData.of(1L, StringData.fromString("other_key_data")),
                    GenericRowData.of(1L, StringData.fromString("other_value_data")))));
    testEqualsAndHashCode(schema, idOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, mapOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);

    GenericRowData rowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericMapData(
                ImmutableMap.of(GenericRowData.of(1L, null), GenericRowData.of(1L, null))));
    GenericRowData copyRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericMapData(
                ImmutableMap.of(GenericRowData.of(1L, null), GenericRowData.of(1L, null))));
    // modify the map field value
    GenericRowData otherRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("other_row_id_value"),
            new GenericMapData(
                ImmutableMap.of(GenericRowData.of(2L, null), GenericRowData.of(2L, null))));
    testEqualsAndHashCode(
        schema,
        idOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
    testEqualsAndHashCode(
        schema,
        mapOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
    testEqualsAndHashCode(
        schema,
        schema,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
  }

  @Test
  public void testArrayOfPrimitiveProjection() {
    DataGenerator dataGenerator = new DataGenerators.ArrayOfPrimitive();
    Schema schema = dataGenerator.icebergSchema();

    // Project id only.
    Schema idOnly = schema.select("row_id");
    assertThat(idOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, idOnly);

    // Project list only.
    Schema arrayOnly = schema.select("array_of_int");
    assertThat(arrayOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, arrayOnly);

    // Project all.
    generateAndValidate(schema, schema);

    GenericRowData rowData = dataGenerator.generateFlinkRowData();
    GenericRowData copyRowData = dataGenerator.generateFlinkRowData();
    // modify the map field value
    GenericRowData otherRowData =
        GenericRowData.of(
            StringData.fromString("other_row_id_value"),
            new GenericArrayData(new Integer[] {4, 5, 6}));
    testEqualsAndHashCode(schema, idOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, arrayOnly, rowData, copyRowData, otherRowData);
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);

    GenericRowData rowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericArrayData(new Integer[] {1, null, 3}));
    GenericRowData copyRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericArrayData(new Integer[] {1, null, 3}));
    // modify the map field value
    GenericRowData otherRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("other_row_id_value"),
            new GenericArrayData(new Integer[] {4, null, 6}));
    testEqualsAndHashCode(
        schema,
        idOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
    testEqualsAndHashCode(
        schema,
        arrayOnly,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
    testEqualsAndHashCode(
        schema,
        schema,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
  }

  @Test
  public void testArrayOfStructProjection() {
    DataGenerator dataGenerator = new DataGenerators.ArrayOfStruct();
    Schema schema = dataGenerator.icebergSchema();

    // Project id only.
    Schema idOnly = schema.select("row_id");
    assertThat(idOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, idOnly);

    // Project list only.
    Schema arrayOnly = schema.select("array_of_struct");
    assertThat(arrayOnly.columns().size()).isGreaterThan(0);
    generateAndValidate(schema, arrayOnly);

    // Project all.
    generateAndValidate(schema, schema);

    // Project partial list value.
    Schema partialList =
        new Schema(
            Types.NestedField.optional(
                2,
                "array_of_struct",
                Types.ListType.ofOptional(
                    101,
                    Types.StructType.of(
                        Types.NestedField.required(202, "name", Types.StringType.get())))));

    assertThatThrownBy(() -> generateAndValidate(schema, partialList))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot project a partial list element struct.");

    GenericRowData rowData = dataGenerator.generateFlinkRowData();
    GenericRowData copyRowData = dataGenerator.generateFlinkRowData();
    // modify the map field value
    GenericRowData otherRowData =
        GenericRowData.of(
            StringData.fromString("row_id_value"), new GenericArrayData(new Integer[] {4, 5, 6}));
    testEqualsAndHashCode(schema, schema, rowData, copyRowData, otherRowData);

    GenericRowData rowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericArrayData(new Integer[] {1, null, 3}));
    GenericRowData copyRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericArrayData(new Integer[] {1, null, 3}));
    // modify the map field value
    GenericRowData otherRowDataNullOptionalFields =
        GenericRowData.of(
            StringData.fromString("row_id_value"),
            new GenericArrayData(new Integer[] {4, null, 6}));
    testEqualsAndHashCode(
        schema,
        schema,
        rowDataNullOptionalFields,
        copyRowDataNullOptionalFields,
        otherRowDataNullOptionalFields);
  }

  private void generateAndValidate(Schema schema, Schema projectSchema) {
    int numRecords = 100;
    List<Record> recordList = RandomGenericData.generate(schema, numRecords, 102L);
    List<RowData> rowDataList =
        Lists.newArrayList(RandomRowData.generate(schema, numRecords, 102L).iterator());
    assertThat(rowDataList).hasSize(recordList.size());

    StructProjection structProjection = StructProjection.create(schema, projectSchema);
    RowDataProjection rowDataProjection = RowDataProjection.create(schema, projectSchema);

    for (int i = 0; i < numRecords; i++) {
      StructLike expected = structProjection.wrap(recordList.get(i));
      RowData projected = rowDataProjection.wrap(rowDataList.get(i));
      TestHelpers.assertRowData(projectSchema, expected, projected);

      assertThat(projected).isEqualTo(projected);
      assertThat(projected).hasSameHashCodeAs(projected);
      // make sure toString doesn't throw NPE for null values
      assertThatNoException().isThrownBy(projected::toString);
    }
  }

  private void testEqualsAndHashCode(
      Schema schema,
      Schema projectionSchema,
      RowData rowData,
      RowData copyRowData,
      RowData otherRowData) {
    testEqualsAndHashCode(schema, projectionSchema, rowData, copyRowData, otherRowData, false);
  }

  /**
   * @param isOtherRowDataSameAsRowData sometimes projection on otherRowData can result in the same
   *     RowData, e.g. due to empty projection or null struct
   */
  private void testEqualsAndHashCode(
      Schema schema,
      Schema projectionSchema,
      RowData rowData,
      RowData copyRowData,
      RowData otherRowData,
      boolean isOtherRowDataSameAsRowData) {
    RowDataProjection projection = RowDataProjection.create(schema, projectionSchema);
    RowDataProjection copyProjection = RowDataProjection.create(schema, projectionSchema);
    RowDataProjection otherProjection = RowDataProjection.create(schema, projectionSchema);

    assertThat(projection.wrap(rowData)).isEqualTo(copyProjection.wrap(copyRowData));
    assertThat(projection.wrap(rowData)).hasSameHashCodeAs(copyProjection.wrap(copyRowData));

    if (isOtherRowDataSameAsRowData) {
      assertThat(projection.wrap(rowData)).isEqualTo(otherProjection.wrap(otherRowData));
      assertThat(projection.wrap(rowData)).hasSameHashCodeAs(otherProjection.wrap(otherRowData));
    } else {
      assertThat(projection.wrap(rowData)).isNotEqualTo(otherProjection.wrap(otherRowData));
      assertThat(projection.wrap(rowData))
          .doesNotHaveSameHashCodeAs(otherProjection.wrap(otherRowData));
    }
  }
}
