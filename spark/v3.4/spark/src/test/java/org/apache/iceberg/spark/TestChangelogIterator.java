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
            new GenericRowWithSchema(new Object[] {index, "b", "data", DELETE, 0, 0}, null));
      case INSERTED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "c", "data", INSERT, 0, 0}, null));
      case CARRY_OVER:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {index, "d", "data", INSERT, 0, 0}, null));
      case UPDATED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {index, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {index, "a", "new_data", INSERT, 0, 0}, null));
      default:
        throw new IllegalArgumentException("Unknown row type: " + rowType);
    }
  }

  private List<Object[]> toExpectedRows(RowType rowType, int order) {
    switch (rowType) {
      case DELETED:
        List<Object[]> rows = Lists.newArrayList();
        rows.add(new Object[] {order, "b", "data", DELETE, 0, 0});
        return rows;
      case INSERTED:
        List<Object[]> insertedRows = Lists.newArrayList();
        insertedRows.add(new Object[] {order, "c", "data", INSERT, 0, 0});
        return insertedRows;
      case CARRY_OVER:
        return Lists.newArrayList();
      case UPDATED:
        return Lists.newArrayList(
            new Object[] {order, "a", "data", UPDATE_BEFORE, 0, 0},
            new Object[] {order, "a", "new_data", UPDATE_AFTER, 0, 0});
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
            new GenericRowWithSchema(new Object[] {2, null, null, DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {3, null, null, INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, INSERT, 0, 0}, null),
            // mixed null and non-null value in non-identifier columns
            new GenericRowWithSchema(new Object[] {5, null, null, DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {5, null, "data", INSERT, 0, 0}, null),
            // mixed null and non-null value in identifier columns
            new GenericRowWithSchema(new Object[] {6, null, null, DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {6, "name", null, INSERT, 0, 0}, null));

    Iterator<Row> iterator =
        ChangelogIterator.computeUpdates(rowsWithNull.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {2, null, null, DELETE, 0, 0},
            new Object[] {3, null, null, INSERT, 0, 0},
            new Object[] {5, null, null, UPDATE_BEFORE, 0, 0},
            new Object[] {5, null, "data", UPDATE_AFTER, 0, 0},
            new Object[] {6, null, null, DELETE, 0, 0},
            new Object[] {6, "name", null, INSERT, 0, 0}),
        rowsToJava(result));
  }

  @Test
  public void testUpdatedRowsWithDuplication() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // two rows with same identifier fields(id, name)
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", INSERT, 0, 0}, null));

    Iterator<Row> iterator =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertThrows(
        "Cannot compute updates because there are multiple rows with the same identifier fields([id, name]). Please make sure the rows are unique.",
        IllegalStateException.class,
        () -> Lists.newArrayList(iterator));

    // still allow extra insert rows
    rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data1", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data2", INSERT, 0, 0}, null));

    Iterator<Row> iterator1 =
        ChangelogIterator.computeUpdates(rowsWithDuplication.iterator(), SCHEMA, IDENTIFIER_FIELDS);

    assertEquals(
        "Rows should match.",
        Lists.newArrayList(
            new Object[] {1, "a", "data", UPDATE_BEFORE, 0, 0},
            new Object[] {1, "a", "new_data1", UPDATE_AFTER, 0, 0},
            new Object[] {1, "a", "new_data2", INSERT, 0, 0}),
        rowsToJava(Lists.newArrayList(iterator1)));
  }

  @Test
  public void testCarryRowsRemoveWithDuplicates() {
    // assume rows are sorted by id and change type
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // keep all delete rows for id 0 and id 1 since there is no insert row for them
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {0, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "old_data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "old_data", DELETE, 0, 0}, null),
            // the same number of delete and insert rows for id 2
            new GenericRowWithSchema(new Object[] {2, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {2, "a", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {3, "a", "new_data", INSERT, 0, 0}, null));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "a", "data", DELETE, 0, 0},
            new Object[] {0, "a", "data", DELETE, 0, 0},
            new Object[] {0, "a", "data", DELETE, 0, 0},
            new Object[] {1, "a", "old_data", DELETE, 0, 0},
            new Object[] {1, "a", "old_data", DELETE, 0, 0},
            new Object[] {3, "a", "new_data", INSERT, 0, 0});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveLessInsertRows() {
    // less insert rows than delete rows
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {2, "d", "data", INSERT, 0, 0}, null));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE, 0, 0},
            new Object[] {2, "d", "data", INSERT, 0, 0});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveMoreInsertRows() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {0, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            // more insert rows than delete rows, should keep extra insert rows
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "d", "data", DELETE, 0, 0},
            new Object[] {1, "d", "data", INSERT, 0, 0});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  @Test
  public void testCarryRowsRemoveNoInsertRows() {
    // no insert row
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {1, "d", "data", DELETE, 0, 0},
            new Object[] {1, "d", "data", DELETE, 0, 0});

    validateIterators(rowsWithDuplication, expectedRows);
  }

  private void validateIterators(List<Row> rowsWithDuplication, List<Object[]> expectedRows) {
    Iterator<Row> iterator =
        ChangelogIterator.removeCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, rowsToJava(result));

    iterator = ChangelogIterator.removeNetCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, rowsToJava(result));
  }

  @Test
  public void testRemoveNetCarryovers() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // this row are different from other rows, it is a net change, should be kept
            new GenericRowWithSchema(new Object[] {0, "d", "data", DELETE, 0, 0}, null),
            // a pair of delete and insert rows, should be removed
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 0, 0}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 0, 0}, null),
            // 2 delete rows and 2 insert rows, should be removed
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 1, 1}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 1, 1}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 1, 1}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 1, 1}, null),
            // a pair of insert and delete rows across snapshots, should be removed
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 2, 2}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", DELETE, 3, 3}, null),
            // extra insert rows, they are net changes, should be kept
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 4, 4}, null),
            new GenericRowWithSchema(new Object[] {1, "d", "data", INSERT, 4, 4}, null),
            // different key, net changes, should be kept
            new GenericRowWithSchema(new Object[] {2, "d", "data", DELETE, 4, 4}, null));

    List<Object[]> expectedRows =
        Lists.newArrayList(
            new Object[] {0, "d", "data", DELETE, 0, 0},
            new Object[] {1, "d", "data", INSERT, 4, 4},
            new Object[] {1, "d", "data", INSERT, 4, 4},
            new Object[] {2, "d", "data", DELETE, 4, 4});

    Iterator<Row> iterator =
        ChangelogIterator.removeNetCarryovers(rowsWithDuplication.iterator(), SCHEMA);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match.", expectedRows, rowsToJava(result));
  }
}
