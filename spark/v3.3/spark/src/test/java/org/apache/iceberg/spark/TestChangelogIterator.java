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

import static org.junit.Assert.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

public class TestChangelogIterator extends SparkTestHelperBase {
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
                MetadataColumns.CHANGE_TYPE.name(), DataTypes.StringType, false, Metadata.empty())
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
    Assert.assertEquals(24, permutations.size());

    for (Object[] permutation : permutations) {
      validate(permutation);
    }
  }

  private void validate(Object[] permutation) {
    List<Row> rows = Lists.newArrayList();
    List<Object[]> expectedRows = Lists.newArrayList();
    for (int i = 0; i < permutation.length; i++) {
      rows.addAll(toOriginalRows((RowType) permutation[i], i));
      expectedRows.addAll(toExpectedRows((RowType) permutation[i], i));
    }

    Iterator<Row> iterator =
        ChangelogIterator.computeUpdates(rows.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);
    assertEquals("Rows should match", expectedRows, rowsToJava(result));
  }

  private List<Row> toOriginalRows(RowType rowType, int index) {
    switch (rowType) {
      case DELETED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "b", "data", DELETE}, null));
      case INSERTED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "c", "data", INSERT}, null));
      case CARRY_OVER:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {index, "d", "data", INSERT}, null));
      case UPDATED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {index, "a", "new_data", INSERT}, null));
      default:
        throw new IllegalArgumentException("Unknown row type: " + rowType);
    }
  }

  private List<Object[]> toExpectedRows(RowType rowType, int order) {
    switch (rowType) {
      case DELETED:
        List<Object[]> rows = Lists.newArrayList();
        rows.add(new Object[] {order, "b", "data", DELETE});
        return rows;
      case INSERTED:
        List<Object[]> insertedRows = Lists.newArrayList();
        insertedRows.add(new Object[] {order, "c", "data", INSERT});
        return insertedRows;
      case CARRY_OVER:
        return Lists.newArrayList();
      case UPDATED:
        return Lists.newArrayList(
            new Object[] {order, "a", "data", UPDATE_BEFORE},
            new Object[] {order, "a", "new_data", UPDATE_AFTER});
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

  @Test
  public void testRowsWithNullValue() {
    final List<Row> rowsWithNull =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {2, null, null, DELETE}, null),
            new GenericRowWithSchema(new Object[] {3, null, null, INSERT}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, DELETE}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, INSERT}, null),
            // mixed null and non-null value in non-identifier columns
            new GenericRowWithSchema(new Object[] {5, null, null, DELETE}, null),
            new GenericRowWithSchema(new Object[] {5, null, "data", INSERT}, null),
            // mixed null and non-null value in identifier columns
            new GenericRowWithSchema(new Object[] {6, null, null, DELETE}, null),
            new GenericRowWithSchema(new Object[] {6, "name", null, INSERT}, null));

    Iterator<Row> iterator =
        ChangelogIterator.computeUpdates(rowsWithNull.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {2, null, null, DELETE},
            new Object[] {3, null, null, INSERT},
            new Object[] {5, null, null, UPDATE_BEFORE},
            new Object[] {5, null, "data", UPDATE_AFTER},
            new Object[] {6, null, null, DELETE},
            new Object[] {6, "name", null, INSERT}),
        rowsToJava(result));
  }

  @Test
  public void testUpdatedRowsWithDuplication() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // two rows with same identifier fields(id, name)
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", INSERT}, null));

    Iterator<Row> iterator =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertThrows(
        "Cannot compute updates because there are multiple rows with the same identifier fields([id, name]). Please make sure the rows are unique.",
        IllegalStateException.class,
        () -> Lists.newArrayList(iterator));

    // still allow extra insert rows
    rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data1", INSERT}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data2", INSERT}, null));

    Iterator<Row> iterator1 =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {1, "a", "data", UPDATE_BEFORE},
            new Object[] {1, "a", "new_data1", UPDATE_AFTER},
            new Object[] {1, "a", "new_data2", INSERT}),
        rowsToJava(Lists.newArrayList(iterator1)));
  }

  @Test
  public void testCarryRowsRemoveWithDuplicates() {
    // assume rows are sorted by id and change type
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // keep all delete rows for id 0 and id 1 since there is no insert row for them
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "old_data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "old_data", DELETE}, null),
            // the same number of delete and insert rows for id 2
            new GenericRowWithSchema(new Object[] {2, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {3, "a", "new_data", INSERT}, null));

    Iterator<Row> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {0, "a", "data", DELETE},
            new Object[] {0, "a", "data", DELETE},
            new Object[] {0, "a", "data", DELETE},
            new Object[] {1, "a", "old_data", DELETE},
            new Object[] {1, "a", "old_data", DELETE},
            new Object[] {3, "a", "new_data", INSERT}),
        rowsToJava(result));
  }

  @Test
  public void testCarryRowsRemoveLessInsertRows() {
    // less insert rows than delete rows
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {2, "d", "data", INSERT}, null));

    Iterator<Row> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE}, new Object[] {2, "d", "data", INSERT}),
        rowsToJava(result));
  }

  @Test
  public void testCarryRowsRemoveMoreInsertRows() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {0, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            // more insert rows than delete rows, should keep extra insert rows
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT}, null));

    Iterator<Row> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {0, "d", "data", DELETE}, new Object[] {1, "d", "data", INSERT}),
        rowsToJava(result));
  }

  @Test
  public void testCarryRowsRemoveNoInsertRows() {
    // no insert row
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE}, null));

    Iterator<Row> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Duplicate rows should not be removed",
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE}, new Object[] {1, "d", "data", DELETE}),
        rowsToJava(result));
  }
}
