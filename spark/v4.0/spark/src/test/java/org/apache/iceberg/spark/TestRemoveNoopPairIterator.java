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
import org.apache.iceberg.ChangelogOperation;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

public class TestRemoveNoopPairIterator extends SparkTestHelperBase {
  private static final String DELETE = ChangelogOperation.DELETE.name();
  private static final String INSERT = ChangelogOperation.INSERT.name();

  private static final StructType SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("name", DataTypes.StringType, false, Metadata.empty()),
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

  @Test
  public void testSingleInsertDelete() {
    // i -> d => removed (net no-op)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null));

    List<Object[]> expected = Lists.newArrayList();

    validate(rows, expected);
  }

  @Test
  public void testSingleDeleteInsert() {
    // d -> i => both kept (not a carry-over pattern)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", INSERT, 1, 1L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", DELETE, 0, 0L}, new Object[] {1, "b", INSERT, 1, 1L});

    validate(rows, expected);
  }

  @Test
  public void testMultipleInsertDeletePairs() {
    // i -> d -> i -> d => removed (all net no-ops)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 2, 2L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 3, 3L}, null));

    List<Object[]> expected = Lists.newArrayList();

    validate(rows, expected);
  }

  @Test
  public void testInsertDeleteWithExtraInsert() {
    // i -> d -> i => last insert kept (net insert after cancellation)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", INSERT, 2, 2L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "b", INSERT, 2, 2L});

    validate(rows, expected);
  }

  @Test
  public void testDeleteOnly() {
    // d => kept (no cancellation)
    List<Row> rows =
        Lists.newArrayList(new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", DELETE, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testInsertOnly() {
    // i => kept (no cancellation)
    List<Row> rows =
        Lists.newArrayList(new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", INSERT, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testMultipleKeys() {
    List<Row> rows =
        Lists.newArrayList(
            // Key 1: i -> d => removed
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null),

            // Key 2: d => kept
            new GenericRowWithSchema(new Object[] {2, "b", DELETE, 0, 0L}, null),

            // Key 3: i => kept
            new GenericRowWithSchema(new Object[] {3, "c", INSERT, 0, 0L}, null),

            // Key 4: d -> i => kept (both)
            new GenericRowWithSchema(new Object[] {4, "d", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {4, "e", INSERT, 1, 1L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {2, "b", DELETE, 0, 0L},
            new Object[] {3, "c", INSERT, 0, 0L},
            new Object[] {4, "d", DELETE, 0, 0L},
            new Object[] {4, "e", INSERT, 1, 1L});

    validate(rows, expected);
  }

  @Test
  public void testConsecutiveInserts() {
    // i -> i => both kept (second INSERT doesn't cancel first)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", INSERT, 1, 1L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", INSERT, 0, 0L}, new Object[] {1, "b", INSERT, 1, 1L});

    validate(rows, expected);
  }

  @Test
  public void testConsecutiveDeletes() {
    // d -> d => both kept (second DELETE doesn't cancel first)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", DELETE, 0, 0L}, new Object[] {1, "a", DELETE, 1, 1L});

    validate(rows, expected);
  }

  @Test
  public void testInsertDeleteInsert() {
    // i -> d -> i => last insert kept (first i-d pair cancelled)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 2, 2L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", INSERT, 2, 2L});

    validate(rows, expected);
  }

  @Test
  public void testDeleteInsertDelete() {
    // d -> i -> d => first delete kept, i-d cancelled
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", INSERT, 1, 1L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", DELETE, 2, 2L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", DELETE, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testWithNullIdentifiers() {
    List<Row> rows =
        Lists.newArrayList(
            // Key with null name: i -> d => removed
            new GenericRowWithSchema(new Object[] {1, null, INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, null, DELETE, 1, 1L}, null),

            // Different null keys: kept separately
            new GenericRowWithSchema(new Object[] {2, null, DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {3, null, INSERT, 0, 0L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {2, null, DELETE, 0, 0L}, new Object[] {3, null, INSERT, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testComplexSequence() {
    // Complex pattern: d -> i -> d -> i -> d -> i
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", INSERT, 1, 1L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", DELETE, 2, 2L}, null),
            new GenericRowWithSchema(new Object[] {1, "c", INSERT, 3, 3L}, null),
            new GenericRowWithSchema(new Object[] {1, "c", DELETE, 4, 4L}, null),
            new GenericRowWithSchema(new Object[] {1, "d", INSERT, 5, 5L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", DELETE, 0, 0L}, new Object[] {1, "d", INSERT, 5, 5L});

    validate(rows, expected);
  }

  @Test
  public void testSameOrdinalDifferentSnapshots() {
    // INSERT and DELETE at same ordinal but different snapshot IDs
    // (This tests the ordinal-based sorting, not snapshot-based)
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 1, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 1, 1L}, null));

    List<Object[]> expected = Lists.newArrayList();

    validate(rows, expected);
  }

  @Test
  public void testDifferentIdentifierValues() {
    // INSERT with one identifier value, DELETE with different value => both kept
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {1, "b", DELETE, 1, 1L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", INSERT, 0, 0L}, new Object[] {1, "b", DELETE, 1, 1L});

    validate(rows, expected);
  }

  @Test
  public void testEmptyInput() {
    List<Row> rows = Lists.newArrayList();
    List<Object[]> expected = Lists.newArrayList();

    validate(rows, expected);
  }

  @Test
  public void testSingleInsert() {
    List<Row> rows =
        Lists.newArrayList(new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", INSERT, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testSingleDelete() {
    List<Row> rows =
        Lists.newArrayList(new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null));

    List<Object[]> expected = Lists.newArrayList();
    expected.add(new Object[] {1, "a", DELETE, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testAllDeletes() {
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {2, "b", DELETE, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {3, "c", DELETE, 0, 0L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", DELETE, 0, 0L},
            new Object[] {2, "b", DELETE, 0, 0L},
            new Object[] {3, "c", DELETE, 0, 0L});

    validate(rows, expected);
  }

  @Test
  public void testAllInserts() {
    List<Row> rows =
        Lists.newArrayList(
            new GenericRowWithSchema(new Object[] {1, "a", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {2, "b", INSERT, 0, 0L}, null),
            new GenericRowWithSchema(new Object[] {3, "c", INSERT, 0, 0L}, null));

    List<Object[]> expected =
        Lists.newArrayList(
            new Object[] {1, "a", INSERT, 0, 0L},
            new Object[] {2, "b", INSERT, 0, 0L},
            new Object[] {3, "c", INSERT, 0, 0L});

    validate(rows, expected);
  }

  private void validate(List<Row> rows, List<Object[]> expected) {
    Iterator<Row> iterator =
        ChangelogIterator.removeLogicalNoopPairs(rows.iterator(), SCHEMA, IDENTIFIER_FIELDS);
    List<Row> result = Lists.newArrayList(iterator);

    assertEquals("Rows should match", expected, rowsToJava(result));
  }
}
