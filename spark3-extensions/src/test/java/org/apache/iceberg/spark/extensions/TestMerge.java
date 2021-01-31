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

package org.apache.iceberg.spark.extensions;

import java.util.Map;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMerge extends SparkRowLevelOperationsTestBase {

  public TestMerge(String catalogName, String implementation, Map<String, String> config,
                   String fileFormat, boolean vectorized) {
    super(catalogName, implementation, config, fileFormat, vectorized);
  }

  @BeforeClass
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @Test
  public void testMergeAlignsUpdateAndInsertActions() {
    createAndInitTable("id INT, a INT, b STRING", "{ \"id\": 1, \"a\": 2, \"b\": \"str\" }");
    createOrReplaceView("source",
        "{ \"id\": 1, \"c1\": -2, \"c2\": \"new_str_1\" }\n" +
        "{ \"id\": 2, \"c1\": -20, \"c2\": \"new_str_2\" }");

    sql("MERGE INTO %s t USING source " +
        "ON t.id == source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET b = c2, a = c1, t.id = source.id " +
        "WHEN NOT MATCHED THEN " +
        "  INSERT (b, a, id) VALUES (c2, c1, id)", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, -2, "new_str_1"), row(2, -20, "new_str_2")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeUpdatesNestedStructFields() {
    createAndInitTable("id INT, s STRUCT<c1:INT,c2:STRUCT<a:ARRAY<INT>,m:MAP<STRING, STRING>>>",
        "{ \"id\": 1, \"s\": { \"c1\": 2, \"c2\": { \"a\": [1,2], \"m\": { \"a\": \"b\"} } } } }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2 }");

    // update primitive, array, map columns inside a struct
    sql("MERGE INTO %s t USING source " +
        "ON t.id == source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.s.c1 = source.c1, t.s.c2.a = array(-1, -2), t.s.c2.m = map('k', 'v')", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, row(-2, row(ImmutableList.of(-1, -2), ImmutableMap.of("k", "v"))))),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // set primitive, array, map columns to NULL (proper casts should be in place)
    sql("MERGE INTO %s t USING source " +
        "ON t.id == source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.s.c1 = NULL, t.s.c2 = NULL", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, row(null, null))),
        sql("SELECT * FROM %s ORDER BY id", tableName));

    // update all fields in a struct
    sql("MERGE INTO %s t USING source " +
        "ON t.id == source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.s = named_struct('c1', 100, 'c2', named_struct('a', array(1), 'm', map('x', 'y')))", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, row(100, row(ImmutableList.of(1), ImmutableMap.of("x", "y"))))),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeWithInferredCasts() {
    createAndInitTable("id INT, s STRING", "{ \"id\": 1, \"s\": \"value\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"c1\": -2}");

    // -2 in source should be casted to "-2" in target
    sql("MERGE INTO %s t USING source " +
        "ON t.id == source.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.s = source.c1", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, "-2")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testMergeModifiesNullStruct() {
    createAndInitTable("id INT, s STRUCT<n1:INT,n2:INT>", "{ \"id\": 1, \"s\": null }");
    createOrReplaceView("source", "{ \"id\": 1, \"n1\": -10 }");

    sql("MERGE INTO %s t USING source s " +
        "ON t.id == s.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.s.n1 = s.n1", tableName);

    assertEquals("Output should match",
        ImmutableList.of(row(1, row(-10, null))),
        sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testMergeRefreshesRelationCache() {
    createAndInitTable("id INT, name STRING", "{ \"id\": 1, \"name\": \"n1\" }");
    createOrReplaceView("source", "{ \"id\": 1, \"name\": \"n2\" }");

    Dataset<Row> query = spark.sql("SELECT name FROM " + tableName);
    query.createOrReplaceTempView("tmp");

    spark.sql("CACHE TABLE tmp");

    assertEquals("View should have correct data",
        ImmutableList.of(row("n1")),
        sql("SELECT * FROM tmp"));

    sql("MERGE INTO %s t USING source s " +
        "ON t.id == s.id " +
        "WHEN MATCHED THEN " +
        "  UPDATE SET t.name = s.name", tableName);

    assertEquals("View should have correct data",
        ImmutableList.of(row("n2")),
        sql("SELECT * FROM tmp"));

    spark.sql("UNCACHE TABLE tmp");
  }

  @Test
  public void testMergeWithNonExistingColumns() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about the invalid top-level column",
        AnalysisException.class, "cannot resolve '`t.invalid_col`'",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.invalid_col = s.c2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about the invalid nested column",
        AnalysisException.class, "No such struct field invalid_col",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n2.invalid_col = s.c2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about the invalid top-level column",
        AnalysisException.class, "cannot resolve '`invalid_col`'",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n2.dn1 = s.c2 " +
              "WHEN NOT MATCHED THEN " +
              "  INSERT (id, invalid_col) VALUES (s.c1, null)", tableName);
        });
  }

  @Test
  public void testMergeWithInvalidColumnsInInsert() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about the nested column",
        AnalysisException.class, "Nested fields are not supported inside INSERT clauses",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n2.dn1 = s.c2 " +
              "WHEN NOT MATCHED THEN " +
              "  INSERT (id, c.n2) VALUES (s.c1, null)", tableName);
        });

    AssertHelpers.assertThrows("Should complain about duplicate columns",
        AnalysisException.class, "Duplicate column names inside INSERT clause",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n2.dn1 = s.c2 " +
              "WHEN NOT MATCHED THEN " +
              "  INSERT (id, id) VALUES (s.c1, null)", tableName);
        });

    AssertHelpers.assertThrows("Should complain about missing columns",
        AnalysisException.class, "must provide values for all columns of the target table",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN NOT MATCHED THEN " +
              "  INSERT (id) VALUES (s.c1)", tableName);
        });
  }

  @Test
  public void testMergeWithInvalidUpdates() {
    createAndInitTable("id INT, a ARRAY<STRUCT<c1:INT,c2:INT>>, m MAP<STRING,STRING>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about updating an array column",
        AnalysisException.class, "Updating nested fields is only supported for structs",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.a.c1 = s.c2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about updating a map column",
        AnalysisException.class, "Updating nested fields is only supported for structs",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.m.key = 'new_key'", tableName);
        });
  }

  @Test
  public void testMergeWithConflictingUpdates() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about conflicting updates to a top-level column",
        AnalysisException.class, "Updates are in conflict",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.id = 1, t.c.n1 = 2, t.id = 2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about conflicting updates to a nested column",
        AnalysisException.class, "Updates are in conflict for these columns",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n1 = 1, t.id = 2, t.c.n1 = 2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about conflicting updates to a nested column",
        AnalysisException.class, "Updates are in conflict",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET c.n1 = 1, c = named_struct('n1', 1, 'n2', named_struct('dn1', 1, 'dn2', 2))", tableName);
        });
  }

  @Test
  public void testMergeWithInvalidAssignments() {
    createAndInitTable("id INT NOT NULL, s STRUCT<n1:INT NOT NULL,n2:STRUCT<dn1:INT,dn2:INT>> NOT NULL");
    createOrReplaceView(
        "source",
        "c1 INT, c2 STRUCT<n1:INT NOT NULL> NOT NULL, c3 STRING NOT NULL, c4 STRUCT<dn2:INT,dn1:INT>",
        "{ \"c1\": -100, \"c2\": { \"n1\" : 1 }, \"c3\" : 'str', \"c4\": { \"dn2\": 1, \"dn2\": 2 } }");

    for (String policy : new String[]{"ansi", "strict"}) {
      withSQLConf(ImmutableMap.of("spark.sql.storeAssignmentPolicy", policy), () -> {

        AssertHelpers.assertThrows("Should complain about writing nulls to a top-level column",
            AnalysisException.class, "Cannot write nullable values to non-null column",
            () -> {
              sql("MERGE INTO %s t USING source s " +
                  "ON t.id == s.c1 " +
                  "WHEN MATCHED THEN " +
                  "  UPDATE SET t.id = NULL", tableName);
            });

        AssertHelpers.assertThrows("Should complain about writing nulls to a nested column",
            AnalysisException.class, "Cannot write nullable values to non-null column",
            () -> {
              sql("MERGE INTO %s t USING source s " +
                  "ON t.id == s.c1 " +
                  "WHEN MATCHED THEN " +
                  "  UPDATE SET t.s.n1 = NULL", tableName);
            });

        AssertHelpers.assertThrows("Should complain about writing missing fields in structs",
            AnalysisException.class, "missing fields",
            () -> {
              sql("MERGE INTO %s t USING source s " +
                  "ON t.id == s.c1 " +
                  "WHEN MATCHED THEN " +
                  "  UPDATE SET t.s = s.c2", tableName);
            });

        AssertHelpers.assertThrows("Should complain about writing invalid data types",
            AnalysisException.class, "Cannot safely cast",
            () -> {
              sql("MERGE INTO %s t USING source s " +
                  "ON t.id == s.c1 " +
                  "WHEN MATCHED THEN " +
                  "  UPDATE SET t.s.n1 = s.c3", tableName);
            });

        AssertHelpers.assertThrows("Should complain about writing incompatible structs",
            AnalysisException.class, "field name does not match",
            () -> {
              sql("MERGE INTO %s t USING source s " +
                  "ON t.id == s.c1 " +
                  "WHEN MATCHED THEN " +
                  "  UPDATE SET t.s.n2 = s.c4", tableName);
            });
      });
    }
  }

  @Test
  public void testMergeWithNonDeterministicConditions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about non-deterministic search conditions",
        AnalysisException.class, "nondeterministic expressions are only allowed in",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 AND rand() > t.id " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n1 = -1", tableName);
        });

    AssertHelpers.assertThrows("Should complain about non-deterministic update conditions",
        AnalysisException.class, "nondeterministic expressions are only allowed in",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND rand() > t.id THEN " +
              "  UPDATE SET t.c.n1 = -1", tableName);
        });

    AssertHelpers.assertThrows("Should complain about non-deterministic delete conditions",
        AnalysisException.class, "nondeterministic expressions are only allowed in",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND rand() > t.id THEN " +
              "  DELETE", tableName);
        });

    AssertHelpers.assertThrows("Should complain about non-deterministic insert conditions",
        AnalysisException.class, "nondeterministic expressions are only allowed in",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN NOT MATCHED AND rand() > c1 THEN " +
              "  INSERT (id, c) VALUES (1, null)", tableName);
        });
  }

  @Test
  public void testMergeWithAggregateExpressions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about agg expressions in search conditions",
        AnalysisException.class, "contains one or more unsupported",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 AND max(t.id) == 1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n1 = -1", tableName);
        });

    AssertHelpers.assertThrows("Should complain about agg expressions in update conditions",
        AnalysisException.class, "contains one or more unsupported",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND sum(t.id) < 1 THEN " +
              "  UPDATE SET t.c.n1 = -1", tableName);
        });

    AssertHelpers.assertThrows("Should complain about non-deterministic delete conditions",
        AnalysisException.class, "contains one or more unsupported",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND sum(t.id) THEN " +
              "  DELETE", tableName);
        });

    AssertHelpers.assertThrows("Should complain about non-deterministic insert conditions",
        AnalysisException.class, "contains one or more unsupported",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN NOT MATCHED AND sum(c1) < 1 THEN " +
              "  INSERT (id, c) VALUES (1, null)", tableName);
        });
  }

  @Test
  public void testMergeWithSubqueriesInConditions() {
    createAndInitTable("id INT, c STRUCT<n1:INT,n2:STRUCT<dn1:INT,dn2:INT>>");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain about subquery expressions",
        AnalysisException.class, "Subqueries are not supported in conditions",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 AND t.id < (SELECT max(c2) FROM source) " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET t.c.n1 = s.c2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about subquery expressions",
        AnalysisException.class, "Subqueries are not supported in conditions",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND t.id < (SELECT max(c2) FROM source) THEN " +
              "  UPDATE SET t.c.n1 = s.c2", tableName);
        });

    AssertHelpers.assertThrows("Should complain about subquery expressions",
        AnalysisException.class, "Subqueries are not supported in conditions",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN MATCHED AND t.id NOT IN (SELECT c2 FROM source) THEN " +
              "  DELETE", tableName);
        });

    AssertHelpers.assertThrows("Should complain about subquery expressions",
        AnalysisException.class, "Subqueries are not supported in conditions",
        () -> {
          sql("MERGE INTO %s t USING source s " +
              "ON t.id == s.c1 " +
              "WHEN NOT MATCHED AND s.c1 IN (SELECT c2 FROM source) THEN " +
              "  INSERT (id, c) VALUES (1, null)", tableName);
        });
  }

  @Test
  public void testMergeWithNonIcebergTargetTableNotSupported() {
    createOrReplaceView("target", "{ \"c1\": -100, \"c2\": -200 }");
    createOrReplaceView("source", "{ \"c1\": -100, \"c2\": -200 }");

    AssertHelpers.assertThrows("Should complain non iceberg target table",
        UnsupportedOperationException.class, "MERGE INTO TABLE is not supported temporarily.",
        () -> {
          sql("MERGE INTO target t USING source s " +
              "ON t.c1 == s.c1 " +
              "WHEN MATCHED THEN " +
              "  UPDATE SET *");
        });
  }
}
