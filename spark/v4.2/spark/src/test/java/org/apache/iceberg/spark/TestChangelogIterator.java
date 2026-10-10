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
package org.apache.iceberg.spark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import java.util.stream.Stream;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.AbstractIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.CatalystTypeConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

public class TestChangelogIterator extends SparkTestHelperBase {

  private static final DataType BINARY_ARRAY = DataTypes.createArrayType(DataTypes.BinaryType);
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("name", DataTypes.StringType, false, Metadata.empty()),
            new StructField("data", DataTypes.StringType, true, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType, false, Metadata.empty()),
            new StructField(
                MetadataColumns.CHANGE_ORDINAL.name(),
                DataTypes.IntegerType,
                false,
                Metadata.empty()),
            new StructField(
                MetadataColumns.COMMIT_SNAPSHOT_ID.name(),
                DataTypes.LongType,
                false,
                Metadata.empty())
          });
  private static final String[] IDENTIFIER_FIELDS = new String[] {"id", "name"};

  private enum RowType {
    DELETED,
    INSERTED,
    CARRY_OVER,
    UPDATED
  }

  @Test
  public void testIterator() {
    List<Object[]> permutations = Lists.newArrayList();
    // generate 24 permutations
    permute(
        Arrays.asList(RowType.DELETED, RowType.INSERTED, RowType.CARRY_OVER, RowType.UPDATED),
        0,
        permutations);
    assertThat(permutations).hasSize(24);

    for (Object[] permutation : permutations) {
      validate(permutation);
    }
  }

  @Test
  public void testRowsWithNullValue() {
    final List<InternalRow> rowsWithNull =
        Lists.newArrayList(
            row(2, null, null, DELETE, 0, 0L),
            row(3, null, null, INSERT, 0, 0L),
            row(4, null, null, DELETE, 0, 0L),
            row(4, null, null, INSERT, 0, 0L),
            // mixed null and non-null value in non-identifier columns
            row(5, null, null, DELETE, 0, 0L),
            row(5, null, "data", INSERT, 0, 0L),
            // mixed null and non-null value in identifier columns
            row(6, null, null, DELETE, 0, 0L),
            row(6, "name", null, INSERT, 0, 0L));

    Iterator<InternalRow> iterator =
        ChangelogIterator.computeUpdates(rowsWithNull.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<InternalRow> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {2, null, null, DELETE, 0, 0L},
            new Object[] {3, null, null, INSERT, 0, 0L},
            new Object[] {5, null, null, UPDATE_BEFORE, 0, 0L},
            new Object[] {5, null, "data", UPDATE_AFTER, 0, 0L},
            new Object[] {6, null, null, DELETE, 0, 0L},
            new Object[] {6, "name", null, INSERT, 0, 0L}),
        internalRowsToJava(result));
  }

  @Test
  public void testUpdatedRowsWithDuplication() {
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            // two rows with same identifier fields(id, name)
            row(1, "a", "data", DELETE, 0, 0L),
            row(1, "a", "data", DELETE, 0, 0L),
            row(1, "a", "new_data", INSERT, 0, 0L),
            row(1, "a", "new_data", INSERT, 0, 0L));

    Iterator<InternalRow> iterator =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertThatThrownBy(() -> Lists.newArrayList(iterator))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Cannot compute updates because there are multiple rows with the same identifier fields([id,name]). Please make sure the rows are unique.");

    // still allow extra insert rows
    rowsWithDuplication =
        Lists.newArrayList(
            row(1, "a", "data", DELETE, 0, 0L),
            row(1, "a", "new_data1", INSERT, 0, 0L),
            row(1, "a", "new_data2", INSERT, 0, 0L));

    Iterator<InternalRow> iterator1 =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {1, "a", "data", UPDATE_BEFORE, 0, 0L},
            new Object[] {1, "a", "new_data1", UPDATE_AFTER, 0, 0L},
            new Object[] {1, "a", "new_data2", INSERT, 0, 0L}),
        internalRowsToJava(Lists.newArrayList(iterator1)));
  }

  @Test
  public void testCarryRowsRemoveWithDuplicates() {
    // assume rows are sorted by id and change type
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            // keep all delete rows for id 0 and id 1 since there is no insert row for them
            row(0, "a", "data", DELETE, 0, 0L),
            row(0, "a", "data", DELETE, 0, 0L),
            row(0, "a", "data", DELETE, 0, 0L),
            row(1, "a", "old_data", DELETE, 0, 0L),
            row(1, "a", "old_data", DELETE, 0, 0L),
            // the same number of delete and insert rows for id 2
            row(2, "a", "data", DELETE, 0, 0L),
            row(2, "a", "data", DELETE, 0, 0L),
            row(2, "a", "data", INSERT, 0, 0L),
            row(2, "a", "data", INSERT, 0, 0L),
            row(3, "a", "new_data", INSERT, 0, 0L));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "a", "data", DELETE, 0, 0L},
            new Object[] {0, "a", "data", DELETE, 0, 0L},
            new Object[] {0, "a", "data", DELETE, 0, 0L},
            new Object[] {1, "a", "old_data", DELETE, 0, 0L},
            new Object[] {1, "a", "old_data", DELETE, 0, 0L},
            new Object[] {3, "a", "new_data", INSERT, 0, 0L});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveLessInsertRows() {
    // less insert rows than delete rows
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            row(1, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", INSERT, 0, 0L),
            row(2, "d", "data", INSERT, 0, 0L));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE, 0, 0L},
            new Object[] {2, "d", "data", INSERT, 0, 0L});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveMoreInsertRows() {
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            row(0, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", DELETE, 0, 0L),
            // more insert rows than delete rows, should keep extra insert rows
            row(1, "d", "data", INSERT, 0, 0L),
            row(1, "d", "data", INSERT, 0, 0L),
            row(1, "d", "data", INSERT, 0, 0L),
            row(1, "d", "data", INSERT, 0, 0L));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "d", "data", DELETE, 0, 0L},
            new Object[] {1, "d", "data", INSERT, 0, 0L});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveNoInsertRows() {
    // no insert row
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            // next two rows are identical
            row(1, "d", "data", DELETE, 0, 0L), row(1, "d", "data", DELETE, 0, 0L));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE, 0, 0L},
            new Object[] {1, "d", "data", DELETE, 0, 0L});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testRemoveNetCarryovers() {
    List<InternalRow> rowsWithDuplication =
        Lists.newArrayList(
            // this row are different from other rows, it is a net change, should be kept
            row(0, "d", "data", DELETE, 0, 0L),
            // a pair of delete and insert rows, should be removed
            row(1, "d", "data", DELETE, 0, 0L),
            row(1, "d", "data", INSERT, 0, 0L),
            // 2 delete rows and 2 insert rows, should be removed
            row(1, "d", "data", DELETE, 1, 1L),
            row(1, "d", "data", DELETE, 1, 1L),
            row(1, "d", "data", INSERT, 1, 1L),
            row(1, "d", "data", INSERT, 1, 1L),
            // a pair of insert and delete rows across snapshots, should be removed
            row(1, "d", "data", INSERT, 2, 2L),
            row(1, "d", "data", DELETE, 3, 3L),
            // extra insert rows, they are net changes, should be kept
            row(1, "d", "data", INSERT, 4, 4L),
            row(1, "d", "data", INSERT, 4, 4L),
            // different key, net changes, should be kept
            row(2, "d", "data", DELETE, 4, 4L));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "d", "data", DELETE, 0, 0L},
            new Object[] {1, "d", "data", INSERT, 4, 4L},
            new Object[] {1, "d", "data", INSERT, 4, 4L},
            new Object[] {2, "d", "data", DELETE, 4, 4L});

    Iterator<InternalRow> iterator =
        ChangelogIterator.removeNetCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<InternalRow> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, internalRowsToJava(result));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  public void testRemoveNetChangesWithEqualValues(DataType type, IntFunction<Object> values) {
    InternalRow insert = row(type, values.apply(1), INSERT, 0);
    InternalRow delete = row(type, values.apply(1), DELETE, 1);
    InternalRow latest = row(type, values.apply(2), INSERT, 1);
    Iterator<InternalRow> result =
        ChangelogIterator.removeNetCarryovers(
            List.of(insert, delete, latest).iterator(), schema(type));
    assertThat(result).toIterable().containsExactly(latest);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  public void testRemoveCarryoversWithEqualValues(DataType type, IntFunction<Object> values) {
    assertRemoved(
        type,
        List.of(row(type, values.apply(1), DELETE, 0), row(type, values.apply(1), INSERT, 0)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  public void testRetainChangesWithDifferentValues(DataType type, IntFunction<Object> values) {
    assertRetained(
        type,
        List.of(row(type, values.apply(1), DELETE, 0), row(type, values.apply(2), INSERT, 0)));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("values")
  public void testDistinguishNullFromNonNull(DataType type, IntFunction<Object> values) {
    assertRetained(
        type, List.of(row(type, null, DELETE, 0), row(type, values.apply(1), INSERT, 0)));
    assertRetained(
        type, List.of(row(type, values.apply(1), DELETE, 0), row(type, null, INSERT, 0)));
    assertRemoved(type, List.of(row(type, null, DELETE, 0), row(type, null, INSERT, 0)));
  }

  @Test
  public void testRetainChangesWithDifferentArrayLengths() {
    assertRetained(
        BINARY_ARRAY,
        List.of(
            row(BINARY_ARRAY, List.of(new byte[] {1}), DELETE, 0),
            row(BINARY_ARRAY, List.of(new byte[] {1}, new byte[] {1}), INSERT, 0)));
  }

  @Test
  public void testRetainChangesWithDifferentArrayOrder() {
    assertRetained(
        BINARY_ARRAY,
        List.of(
            row(BINARY_ARRAY, List.of(new byte[] {1}, new byte[] {2}), DELETE, 0),
            row(BINARY_ARRAY, List.of(new byte[] {2}, new byte[] {1}), INSERT, 0)));
  }

  @Test
  void removeCarryoversWithSignedZerosInArrays() {
    DataType type = DataTypes.createArrayType(DataTypes.DoubleType);
    assertRemoved(
        type, List.of(row(type, List.of(-0.0), DELETE, 0), row(type, List.of(0.0), INSERT, 0)));
  }

  @Test
  void removeCarryoversWithSignedZerosInStructs() {
    StructType type =
        new StructType().add("binary", DataTypes.BinaryType).add("number", DataTypes.DoubleType);
    assertRemoved(
        type,
        List.of(
            row(type, RowFactory.create(new byte[] {1}, -0.0), DELETE, 0),
            row(type, RowFactory.create(new byte[] {1}, 0.0), INSERT, 0)));
  }

  @Test
  void removeCarryoversWithNaN() {
    assertRemoved(
        DataTypes.DoubleType,
        List.of(
            row(DataTypes.DoubleType, Double.NaN, DELETE, 0),
            row(DataTypes.DoubleType, Double.NaN, INSERT, 0)));
  }

  @Test
  void computeUpdatesWithReorderedMapEntries() {
    DataType type = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
    Map<String, String> before = new LinkedHashMap<>();
    before.put("a", "first");
    before.put("b", null);
    Map<String, String> after = new LinkedHashMap<>();
    after.put("b", null);
    after.put("a", "first");

    assertThat(
            ChangelogIterator.computeUpdates(
                List.of(row(type, before, DELETE, 0), row(type, after, INSERT, 0)).iterator(),
                schema(type),
                new String[] {"id"}))
        .isExhausted();
  }

  @Test
  void computeUpdatesWithChangedMapValues() {
    DataType type = DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
    List<InternalRow> rows =
        List.of(
            row(type, Map.of("a", "before"), DELETE, 0),
            row(type, Map.of("a", "after"), INSERT, 0));

    assertThat(ChangelogIterator.computeUpdates(rows.iterator(), schema(type), new String[] {"id"}))
        .toIterable()
        .containsExactly(
            row(type, Map.of("a", "before"), UPDATE_BEFORE, 0),
            row(type, Map.of("a", "after"), UPDATE_AFTER, 0));
  }

  @Test
  void computeUpdatesWithNestedMaps() {
    DataType mapType = DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType);
    StructType structType = new StructType().add("maps", DataTypes.createArrayType(mapType));
    DataType type = DataTypes.createMapType(DataTypes.StringType, structType);
    Map<String, byte[]> before = new LinkedHashMap<>();
    before.put("a", new byte[] {1});
    before.put("b", new byte[] {2});
    Map<String, byte[]> after = new LinkedHashMap<>();
    after.put("b", new byte[] {2});
    after.put("a", new byte[] {1});
    List<InternalRow> rows =
        List.of(
            row(type, Map.of("outer", RowFactory.create(List.of(before))), DELETE, 0),
            row(type, Map.of("outer", RowFactory.create(List.of(after))), INSERT, 0));

    assertThat(ChangelogIterator.computeUpdates(rows.iterator(), schema(type), new String[] {"id"}))
        .isExhausted();
  }

  @ParameterizedTest
  @ValueSource(strings = {"carryovers", "net", "updates"})
  void reusedRows(String mode) {
    List<InternalRow> rows =
        List.of(
            row(1, "a", "same", DELETE, 0, 0L),
            row(1, "a", "same", INSERT, 0, 0L),
            row(2, "b", "before", DELETE, 0, 0L),
            row(2, "b", "after", INSERT, 0, 0L),
            row(3, "c", "insert", INSERT, 0, 0L),
            row(4, "d", "delete", DELETE, 0, 0L));
    Iterator<InternalRow> input = rows.iterator();
    UnsafeProjection projection = UnsafeProjection.create(SCHEMA);
    Iterator<InternalRow> reused =
        new AbstractIterator<>() {
          @Override
          protected InternalRow computeNext() {
            return input.hasNext() ? projection.apply(input.next()) : endOfData();
          }
        };
    Iterator<InternalRow> changes =
        switch (mode) {
          case "carryovers" -> ChangelogIterator.removeCarryovers(reused, SCHEMA);
          case "net" -> ChangelogIterator.removeNetCarryovers(reused, SCHEMA);
          case "updates" -> ChangelogIterator.computeUpdates(reused, SCHEMA, IDENTIFIER_FIELDS);
          default -> throw new IllegalArgumentException("Unknown mode: " + mode);
        };
    List<InternalRow> result = Lists.newArrayList();
    while (changes.hasNext()) {
      assertThat(changes.hasNext()).isTrue();
      result.add(changes.next().copy());
    }

    assertThat(result)
        .containsExactly(
            row(2, "b", "before", mode.equals("updates") ? UPDATE_BEFORE : DELETE, 0, 0L),
            row(2, "b", "after", mode.equals("updates") ? UPDATE_AFTER : INSERT, 0, 0L),
            rows.get(4),
            rows.get(5));
  }

  static Stream<Arguments> values() {
    StructType struct =
        new StructType().add("patch", DataTypes.BinaryType).add("number", DataTypes.IntegerType);
    return Stream.of(
        Arguments.of(
            DataTypes.BinaryType, (IntFunction<Object>) value -> new byte[] {(byte) value}),
        Arguments.of(
            BINARY_ARRAY,
            (IntFunction<Object>) value -> Arrays.asList(new byte[] {(byte) value}, null)),
        Arguments.of(
            DataTypes.createArrayType(BINARY_ARRAY),
            (IntFunction<Object>) value -> List.of(List.of(new byte[] {(byte) value}))),
        Arguments.of(
            struct, (IntFunction<Object>) value -> RowFactory.create(new byte[] {(byte) value}, 7)),
        Arguments.of(
            DataTypes.createArrayType(struct),
            (IntFunction<Object>)
                value -> List.of(RowFactory.create(new byte[] {(byte) value}, 7))),
        Arguments.of(
            new StructType().add("patches", BINARY_ARRAY),
            (IntFunction<Object>) value -> RowFactory.create(List.of(new byte[] {(byte) value}))),
        Arguments.of(
            DataTypes.createArrayType(DataTypes.IntegerType),
            (IntFunction<Object>) value -> Arrays.asList(value, null)));
  }

  private void validate(Object[] permutation) {
    List<InternalRow> rows = Lists.newArrayList();
    List<Object[]> expectedRows = Lists.newArrayList();
    for (int i = 0; i < permutation.length; i++) {
      rows.addAll(toOriginalRows((RowType) permutation[i], i));
      expectedRows.addAll(toExpectedRows((RowType) permutation[i], i));
    }

    Iterator<InternalRow> iterator =
        ChangelogIterator.computeUpdates(rows.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<InternalRow> result = Lists.newArrayList(iterator);
    assertEquals("Rows should match", expectedRows, internalRowsToJava(result));
  }

  private List<InternalRow> toOriginalRows(RowType rowType, int index) {
    switch (rowType) {
      case DELETED:
        return Lists.newArrayList(row(index, "b", "data", DELETE, 0, 0L));
      case INSERTED:
        return Lists.newArrayList(row(index, "c", "data", INSERT, 0, 0L));
      case CARRY_OVER:
        return Lists.newArrayList(
            row(index, "d", "data", DELETE, 0, 0L), row(index, "d", "data", INSERT, 0, 0L));
      case UPDATED:
        return Lists.newArrayList(
            row(index, "a", "data", DELETE, 0, 0L), row(index, "a", "new_data", INSERT, 0, 0L));
      default:
        throw new IllegalArgumentException("Unknown row type: " + rowType);
    }
  }

  private List<Object[]> toExpectedRows(RowType rowType, int order) {
    switch (rowType) {
      case DELETED:
        List<Object[]> rows = Lists.newArrayList();
        rows.add(new Object[] {order, "b", "data", DELETE, 0, 0L});
        return rows;
      case INSERTED:
        List<Object[]> insertedRows = Lists.newArrayList();
        insertedRows.add(new Object[] {order, "c", "data", INSERT, 0, 0L});
        return insertedRows;
      case CARRY_OVER:
        return Lists.newArrayList();
      case UPDATED:
        return Lists.newArrayList(
            new Object[] {order, "a", "data", UPDATE_BEFORE, 0, 0L},
            new Object[] {order, "a", "new_data", UPDATE_AFTER, 0, 0L});
      default:
        throw new IllegalArgumentException("Unknown row type: " + rowType);
    }
  }

  private void permute(List<RowType> arr, int start, List<Object[]> pm) {
    for (int i = start; i < arr.size(); i++) {
      Collections.swap(arr, i, start);
      permute(arr, start + 1, pm);
      Collections.swap(arr, start, i);
    }
    if (start == arr.size() - 1) {
      pm.add(arr.toArray());
    }
  }

  private void validateIterators(
      List<InternalRow> rowsWithDuplication, List<Object[]> expectedRows) {
    Iterator<InternalRow> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<InternalRow> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, internalRowsToJava(result));

    iterator = ChangelogIterator.removeNetCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, internalRowsToJava(result));
  }

  private static void assertRemoved(DataType type, List<InternalRow> rows) {
    assertThat(ChangelogIterator.removeCarryovers(rows.iterator(), schema(type))).isExhausted();
    assertThat(ChangelogIterator.removeNetCarryovers(rows.iterator(), schema(type))).isExhausted();
  }

  private static void assertRetained(DataType type, List<InternalRow> rows) {
    assertThat(ChangelogIterator.removeCarryovers(rows.iterator(), schema(type)))
        .toIterable()
        .containsExactlyElementsOf(rows);
    assertThat(ChangelogIterator.removeNetCarryovers(rows.iterator(), schema(type)))
        .toIterable()
        .containsExactlyElementsOf(rows);
  }

  private static StructType schema(DataType type) {
    return new StructType()
        .add("id", DataTypes.IntegerType)
        .add("value", type)
        .add(MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType)
        .add(MetadataColumns.CHANGE_ORDINAL.name(), DataTypes.IntegerType)
        .add(MetadataColumns.COMMIT_SNAPSHOT_ID.name(), DataTypes.LongType);
  }

  private static InternalRow row(DataType type, Object value, String operation, int ordinal) {
    return internalRow(
        schema(type), RowFactory.create(1, value, operation, ordinal, (long) ordinal));
  }

  private static InternalRow row(Object... values) {
    return internalRow(SCHEMA, RowFactory.create(values));
  }

  private static InternalRow internalRow(StructType schema, Row row) {
    InternalRow internal =
        (InternalRow) CatalystTypeConverters.createToCatalystConverter(schema).apply(row);
    return UnsafeProjection.create(schema).apply(internal).copy();
  }

  private List<Object[]> internalRowsToJava(List<InternalRow> rows) {
    List<Row> externalRows = Lists.newArrayList();
    for (InternalRow row : rows) {
      externalRows.add((Row) CatalystTypeConverters.createToScalaConverter(SCHEMA).apply(row));
    }
    return rowsToJava(externalRows);
  }
}
