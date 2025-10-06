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

/**
 * Tests for the net update computation across multiple snapshots. This tests the logic used by
 * computeNetUpdateImage method which filters rows to first and last changes per identifier group,
 * then computes net updates.
 */
public class TestNetUpdateChangelogIterator extends SparkTestHelperBase {
  private static final String[] IDENTIFIER_FIELDS = new String[] {"id", "name"};

  @Test
  public void testSimpleInsert() {
    // Single insert operation across snapshots
    List<Row> rows = new ChangelogRowBuilder().insert(1, "a", "data1", 1, 1L).buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().insert(1, "a", "data1", 1, 1L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testSimpleDelete() {
    // Single delete operation across snapshots
    List<Row> rows = new ChangelogRowBuilder().delete(1, "a", "data1", 1, 1L).buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().delete(1, "a", "data1", 1, 1L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testInsertThenDelete() {
    // Insert in one snapshot, delete in another => no-op (row never existed long-term)
    List<Row> rows =
        new ChangelogRowBuilder()
            .insert(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data1", 2, 2L)
            .buildRows();
    List<Object[]> expected = Lists.newArrayList();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testDeleteThenInsert() {
    // Delete in one snapshot, insert in another => UPDATE (row changed)
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

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMultipleInsertsAcrossSnapshots() {
    // Multiple inserts across snapshots, only last one matters
    List<Row> rows =
        new ChangelogRowBuilder()
            .insert(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .insert(1, "a", "data3", 3, 3L)
            .buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().insert(1, "a", "data3", 3, 3L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMultipleDeletesAcrossSnapshots() {
    // Multiple deletes across snapshots, only first one matters
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data1", 2, 2L)
            .delete(1, "a", "data1", 3, 3L)
            .buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().delete(1, "a", "data1", 1, 1L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testDeleteInsertDeleteAcrossSnapshots() {
    // Delete -> Insert -> Delete => net result is DELETE (first one)
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .delete(1, "a", "data2", 3, 3L)
            .buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().delete(1, "a", "data1", 1, 1L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testInsertDeleteInsertAcrossSnapshots() {
    // Insert -> Delete -> Insert => net result is last INSERT
    List<Row> rows =
        new ChangelogRowBuilder()
            .insert(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data1", 2, 2L)
            .insert(1, "a", "data2", 3, 3L)
            .buildRows();
    List<Object[]> expected = new ChangelogRowBuilder().insert(1, "a", "data2", 3, 3L).build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testComplexUpdateSequence() {
    // Delete -> Insert -> Delete -> Insert => UPDATE (first DELETE to last INSERT)
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .delete(1, "a", "data2", 3, 3L)
            .insert(1, "a", "data3", 4, 4L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data3", 4, 4L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMultipleRowsDifferentIdentifiers() {
    // Multiple rows with different identifiers processed independently
    List<Row> rows =
        new ChangelogRowBuilder()
            // Row 1: Insert -> Delete => no-op
            .insert(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data1", 2, 2L)
            // Row 2: Delete -> Insert => UPDATE
            .delete(2, "b", "data1", 1, 1L)
            .insert(2, "b", "data2", 2, 2L)
            // Row 3: Just INSERT
            .insert(3, "c", "data1", 1, 1L)
            // Row 4: Just DELETE
            .delete(4, "d", "data1", 1, 1L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            // Row 1: no-op (not included)
            // Row 2: UPDATE
            .updateBefore(2, "b", "data1", 1, 1L)
            .updateAfter(2, "b", "data2", 2, 2L)
            // Row 3: INSERT
            .insert(3, "c", "data1", 1, 1L)
            // Row 4: DELETE
            .delete(4, "d", "data1", 1, 1L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testSameIdentifierDifferentData() {
    // Row with same identifier but data changes across snapshots
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

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMultipleOperationsSameOrdinal() {
    // Multiple operations at the same ordinal (same snapshot, different files)
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 1, 1L)
            .delete(1, "a", "data2", 1, 1L)
            .insert(1, "a", "data3", 1, 1L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data3", 1, 1L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testNullValuesInData() {
    // Test with null values in non-identifier columns
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", null, 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", null, 1, 1L)
            .updateAfter(1, "a", "data2", 2, 2L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testNullValuesInIdentifierColumns() {
    // Test with null values in identifier columns
    List<Row> rows =
        new ChangelogRowBuilder()
            // Row 1: null name identifier
            .delete(1, null, "data1", 1, 1L)
            .insert(1, null, "data2", 2, 2L)
            // Row 2: different null identifier (different id)
            .insert(2, null, "data3", 1, 1L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            // Row 1: UPDATE (same identifier including null)
            .updateBefore(1, null, "data1", 1, 1L)
            .updateAfter(1, null, "data2", 2, 2L)
            // Row 2: INSERT
            .insert(2, null, "data3", 1, 1L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testCarryoverRowsRemoved() {
    // Carryover rows (DELETE + INSERT with same data at same ordinal) should be removed first
    List<Row> rows =
        new ChangelogRowBuilder()
            // Carryover at ordinal 1 (should be filtered out before net update computation)
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data1", 1, 1L)
            // Real change at ordinal 2
            .delete(1, "a", "data1", 2, 2L)
            .insert(1, "a", "data2", 2, 2L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data2", 2, 2L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testLongSequenceOfChanges() {
    // Long sequence of changes: only first and last matter
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "v1", 1, 1L)
            .insert(1, "a", "v2", 2, 2L)
            .delete(1, "a", "v2", 3, 3L)
            .insert(1, "a", "v3", 4, 4L)
            .delete(1, "a", "v3", 5, 5L)
            .insert(1, "a", "v4", 6, 6L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "v1", 1, 1L)
            .updateAfter(1, "a", "v4", 6, 6L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMixedIdentifierTypes() {
    // Test with different identifier values to ensure proper grouping
    List<Row> rows =
        new ChangelogRowBuilder()
            // Group 1: (id=1, name="a")
            .delete(1, "a", "data1", 1, 1L)
            .insert(1, "a", "data2", 2, 2L)
            // Group 2: (id=1, name="b") - different name, different group
            .insert(1, "b", "data3", 1, 1L)
            .delete(1, "b", "data3", 2, 2L)
            // Group 3: (id=2, name="a") - different id, different group
            .insert(2, "a", "data4", 1, 1L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            // Group 1: UPDATE
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data2", 2, 2L)
            // Group 2: no-op (not included)
            // Group 3: INSERT
            .insert(2, "a", "data4", 1, 1L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testMultipleUpdatesInSequence() {
    // Multiple deletes followed by multiple inserts
    List<Row> rows =
        new ChangelogRowBuilder()
            .delete(1, "a", "data1", 1, 1L)
            .delete(1, "a", "data1", 2, 2L)
            .insert(1, "a", "data2", 3, 3L)
            .insert(1, "a", "data3", 4, 4L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .updateBefore(1, "a", "data1", 1, 1L)
            .updateAfter(1, "a", "data3", 4, 4L)
            .build();

    validateNetUpdates(rows, expected);
  }

  @Test
  public void testAllOperationTypes() {
    // Comprehensive test with all operation types across multiple rows
    List<Row> rows =
        new ChangelogRowBuilder()
            // Row 1: Pure insert
            .insert(1, "pure_insert", "data", 1, 1L)
            // Row 2: Pure delete
            .delete(2, "pure_delete", "data", 1, 1L)
            // Row 3: Insert then delete (no-op)
            .insert(3, "temp_insert", "data", 1, 1L)
            .delete(3, "temp_insert", "data", 2, 2L)
            // Row 4: Update
            .delete(4, "updated", "old", 1, 1L)
            .insert(4, "updated", "new", 2, 2L)
            .buildRows();
    List<Object[]> expected =
        new ChangelogRowBuilder()
            .insert(1, "pure_insert", "data", 1, 1L)
            .delete(2, "pure_delete", "data", 1, 1L)
            // Row 3 is no-op (not included)
            .updateBefore(4, "updated", "old", 1, 1L)
            .updateAfter(4, "updated", "new", 2, 2L)
            .build();

    validateNetUpdates(rows, expected);
  }

  private void validateNetUpdates(List<Row> rows, List<Object[]> expected) {
    ChangelogRowBuilder schemaBuilder = new ChangelogRowBuilder();
    Iterator<Row> iterator =
        ChangelogIterator.computeNetUpdates(
            rows.iterator(), schemaBuilder.schema(), IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match", expected, rowsToJava(result));
  }
}
