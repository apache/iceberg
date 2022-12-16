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

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.Assert;
import org.junit.Test;

public class TestChangelogIterator {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();
  private static final String UPDATE_BEFORE = ChangelogOperation.UPDATE_BEFORE.name();
  private static final String UPDATE_AFTER = ChangelogOperation.UPDATE_AFTER.name();

  private final int changeTypeIndex = 3;
  private final List<Integer> partitionIdx = Lists.newArrayList(0, 1);

  private enum RowType {
    DELETED,
    INSERTED,
    CARRY_OVER,
    UPDATED
  }

  @Test
  public void testIterator() {
    List<Object[]> pm = Lists.newArrayList();
    // generate 24 permutations.
    permute(
        Arrays.asList(RowType.DELETED, RowType.INSERTED, RowType.CARRY_OVER, RowType.UPDATED),
        0,
        pm);
    Assert.assertEquals(24, pm.size());

    for (Object[] item : pm) {
      validate(item);
    }
  }

  private void validate(Object[] item) {
    List<Row> rows = Lists.newArrayList();
    List<Object[]> expectedRows = Lists.newArrayList();
    for (int i = 0; i < item.length; i++) {
      rows.addAll(toOriginalRows((RowType) item[i], i));
      expectedRows.addAll(toExpectedRows((RowType) item[i], i));
    }

    Iterator<Row> iterator =
        ChangelogIterator.iterator(rows.iterator(), changeTypeIndex, partitionIdx);
    List<Row> result = Lists.newArrayList(iterator);
    SparkTestBase.assertEquals("Rows should match", expectedRows, SparkTestBase.rowsToJava(result));
  }

  private List<Row> toOriginalRows(RowType rowType, int order) {
    switch (rowType) {
      case DELETED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {order, "b", "data", DELETE}, null));
      case INSERTED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {order, "c", "data", INSERT}, null));
      case CARRY_OVER:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {order, "d", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {order, "d", "data", INSERT}, null));
      case UPDATED:
        return Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {order, "a", "data", DELETE}, null),
            new GenericRowWithSchema(new Object[] {order, "a", "new_data", INSERT}, null));
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

  private void permute(List<RowType> arr, int k, List<Object[]> pm) {
    for (int i = k; i < arr.size(); i++) {
      Collections.swap(arr, i, k);
      permute(arr, k + 1, pm);
      Collections.swap(arr, k, i);
    }
    if (k == arr.size() - 1) {
      pm.add(arr.toArray());
    }
  }

  @Test
  public void testRowsWithNullValue() {
    final List<Row> rowsWithNull =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {2, null, null, "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {3, null, null, "INSERT"}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {4, null, null, "INSERT"}, null),
            // mixed null and non-null value in non-identifier columns
            new GenericRowWithSchema(new Object[] {5, null, null, "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {5, null, "data", "INSERT"}, null),
            // mixed null and non-null value in identifier columns
            new GenericRowWithSchema(new Object[] {6, null, null, "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {6, "name", null, "INSERT"}, null));

    Iterator<Row> iterator =
        ChangelogIterator.iterator(rowsWithNull.iterator(), 3, Lists.newArrayList(0, 1));
    List<Row> result = Lists.newArrayList(iterator);

    SparkTestBase.assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {2, null, null, DELETE},
            new Object[] {3, null, null, INSERT},
            new Object[] {5, null, null, UPDATE_BEFORE},
            new Object[] {5, null, "data", UPDATE_AFTER},
            new Object[] {6, null, null, DELETE},
            new Object[] {6, "name", null, INSERT}),
        SparkTestBase.rowsToJava(result));
  }

  @Test
  public void testUpdatedRowsWithDuplication() {
    List<Row> rowsWithDuplication =
        Lists.newArrayList(
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {1, "a", "data", "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "data", "DELETE"}, null),
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", "INSERT"}, null),
            new GenericRowWithSchema(new Object[] {1, "a", "new_data", "INSERT"}, null),
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {4, "d", "data", "DELETE"}, null),
            new GenericRowWithSchema(new Object[] {4, "d", "data", "DELETE"}, null),
            // next two rows are identical
            new GenericRowWithSchema(new Object[] {4, "d", "data", "INSERT"}, null),
            new GenericRowWithSchema(new Object[] {4, "d", "data", "INSERT"}, null));

    Iterator<Row> iterator =
        ChangelogIterator.iterator(rowsWithDuplication.iterator(), changeTypeIndex, partitionIdx);
    List<Row> result = Lists.newArrayList(iterator);

    SparkTestBase.assertEquals(
        "Duplicate rows should not be removed",
        Lists.newArrayList(
            new Object[] {1, "a", "data", DELETE},
            new Object[] {1, "a", "data", UPDATE_BEFORE},
            new Object[] {1, "a", "new_data", UPDATE_AFTER},
            new Object[] {1, "a", "new_data", INSERT},
            new Object[] {4, "d", "data", DELETE},
            new Object[] {4, "d", "data", INSERT}),
        SparkTestBase.rowsToJava(result));
  }
}
