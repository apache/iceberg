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

import static org.apache.iceberg.ChangelogOperation.DELETE;
import static org.apache.iceberg.ChangelogOperation.INSERT;
import static org.apache.iceberg.ChangelogOperation.UPDATE_AFTER;
import static org.apache.iceberg.ChangelogOperation.UPDATE_BEFORE;

import java.util.List;
import java.util.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.Iterators;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.junit.Test;

public class TestChangelogIterator {
  private final List<Row> rows =
      Lists.newArrayList(
          new GenericRowWithSchema(new Object[] {1, "a", "data", "DELETE"}, null),
          new GenericRowWithSchema(new Object[] {1, "a", "new_data", "INSERT"}, null),
          // next two rows belong to different partitions
          new GenericRowWithSchema(new Object[] {2, "b", "data", "DELETE"}, null),
          new GenericRowWithSchema(new Object[] {3, "c", "data", "INSERT"}, null),
          new GenericRowWithSchema(new Object[] {4, "d", "data", "DELETE"}, null),
          new GenericRowWithSchema(new Object[] {4, "d", "data", "INSERT"}, null));

  private final int changeTypeIndex = 3;
  private final List<Integer> partitionIdx = Lists.newArrayList(0, 1);

  @Test
  public void testUpdatedRows() {
    ChangelogIterator iterator =
        new ChangelogIterator(rows.iterator(), changeTypeIndex, partitionIdx, true);

    List<Row> result = Lists.newArrayList(Iterators.filter(iterator, Objects::nonNull));
    SparkTestBase.assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {1, "a", "data", UPDATE_BEFORE.name()},
            new Object[] {1, "a", "new_data", UPDATE_AFTER.name()},
            new Object[] {2, "b", "data", "DELETE"},
            new Object[] {3, "c", "data", "INSERT"}),
        SparkTestBase.rowsToJava(result));
  }

  @Test
  public void testNotMarkUpdatedRows() {
    ChangelogIterator iterator =
        new ChangelogIterator(rows.iterator(), changeTypeIndex, partitionIdx, false);
    List<Row> result = Lists.newArrayList(Iterators.filter(iterator, Objects::nonNull));
    SparkTestBase.assertEquals(
        "Rows should match",
        Lists.newArrayList(
            new Object[] {1, "a", "data", "DELETE"},
            new Object[] {1, "a", "new_data", "INSERT"},
            new Object[] {2, "b", "data", "DELETE"},
            new Object[] {3, "c", "data", "INSERT"}),
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

    ChangelogIterator iterator =
        new ChangelogIterator(rowsWithDuplication.iterator(), changeTypeIndex, partitionIdx, true);

    List<Row> result = Lists.newArrayList(Iterators.filter(iterator, Objects::nonNull));
    SparkTestBase.assertEquals(
        "Duplicate rows should not be removed",
        Lists.newArrayList(
            new Object[] {1, "a", "data", DELETE.name()},
            new Object[] {1, "a", "data", UPDATE_BEFORE.name()},
            new Object[] {1, "a", "new_data", UPDATE_AFTER.name()},
            new Object[] {1, "a", "new_data", INSERT.name()},
            new Object[] {4, "d", "data", DELETE.name()},
            new Object[] {4, "d", "data", INSERT.name()}),
        SparkTestBase.rowsToJava(result));
  }
}
