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

import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Test;

public class TestComputeNetUpdateIterator extends SparkTestHelperBase {
  private static final String[] IDENTIFIER_FIELDS = new String[] {"id", "name"};

  @Test
  public void testMultipleInsertsTakesLast() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .insert(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .buildRows();

    List<Object[]> expected = new ChangelogRowBuilder().insert(1, "a", "data2", 2, 2L).build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testMultipleDeletesTakesFirst() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data", 1, 1L)
            .delete(1, "a", "data", 2, 2L)
            .buildRows();

    List<Object[]> expected = new ChangelogRowBuilder().delete(1, "a", "data", 1, 1L).build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testDeleteThenInsertBecomesUpdate() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .buildRows();

    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data2", 2, 2L)
            .build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testInsertThenDeleteIsNoOp() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data", 1, 1L)
            .insert(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data2", 3, 3L)
            .insert(1, "a", "data3", 3, 3L)
            .buildRows();

    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data", 1, 1L)
            .updateAfter(1, "a", "data3", 3, 3L)
            .build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testMultipleChangesWithFirstDeleteLastInsert() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 1, 1L)
            .delete(1, "a", "data2", 2, 2L)
            .insert(1, "a", "data3", 2, 2L)
            .buildRows();

    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data3", 2, 2L)
            .build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testMultipleKeys() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .insert(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .delete(2, "b", "data", 1, 1L)
            .delete(2, "b", "data", 2, 2L)
            .delete(3, "c", "data1", 1, 1L)
            .insert(3, "c", "data2", 2, 2L)
            .insert(4, "d", "data", 1, 1L)
            .delete(4, "d", "data", 2, 2L)
            .buildRows();

    List<Object[]> expected =
        new ChangelogRowBuilder()
            .insert(1, "a", "data2", 2, 2L)
            .delete(2, "b", "data", 1, 1L)
            .updateBefore(3, "c", "data1", 1, 1L)
            .updateAfter(3, "c", "data2", 2, 2L)
            .build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  @Test
  public void testWithNullIdentifiers() {
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .delete(2, null, "data1", 1, 1L)
            .insert(2, null, "data2", 2, 2L)
            .delete(3, null, "data1", 1, 1L)
            .insert(3, "c", "data2", 2, 2L)
            .buildRows();

    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data2", 2, 2L)
            .updateBefore(2, null, "data1", 1, 1L)
            .updateAfter(2, null, "data2", 2, 2L)
            .delete(3, null, "data1", 1, 1L)
            .insert(3, "c", "data2", 2, 2L)
            .build();

    validateNetUpdates(rows, expected, ChangelogRowBuilder.schema());
  }

  private void validateNetUpdates(
      List<Row> rows, List<Object[]> expected, org.apache.spark.sql.types.StructType schema) {
    Iterator<Row> iterator =
        ChangelogIterator.computeNetUpdates(rows.iterator(), schema, IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match", expected, rowsToJava(result));
  }
}
