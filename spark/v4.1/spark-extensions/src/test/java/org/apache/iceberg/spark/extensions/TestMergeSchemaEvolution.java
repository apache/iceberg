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

import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Map;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMergeSchemaEvolution extends SparkRowLevelOperationsTestBase {

  @BeforeAll
  public static void setupSparkConf() {
    spark.conf().set("spark.sql.shuffle.partitions", "4");
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
    sql("DROP TABLE IF EXISTS source");
  }

  @TestTemplate
  public void testMergeWithSchemaEvolutionSourceHasMoreColumns() {
    assumeThat(branch).as("Schema evolution does not work for branches currently").isNull();

    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"software\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, salary INT",
        "{ \"id\": 1, \"dep\": \"hr\", \"salary\": 100 }\n"
            + "{ \"id\": 3, \"dep\": \"finance\", \"salary\": 300 }");

    sql(
        "MERGE WITH SCHEMA EVOLUTION INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    // The new 'salary' column should be added to the target table
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "hr", 100), // updated with salary
            row(2, "software", null), // kept, salary is null
            row(3, "finance", 300)); // new row with salary
    assertEquals(
        "Should have expected rows with new column",
        expectedRows,
        sql("SELECT id, dep, salary FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMergeWithSchemaEvolutionSourceHasFewerColumns() {
    assumeThat(branch).as("Schema evolution does not work for branches currently").isNull();

    createAndInitTable(
        "id INT, dep STRING, salary INT",
        "{ \"id\": 1, \"dep\": \"hr\", \"salary\": 100 }\n"
            + "{ \"id\": 2, \"dep\": \"software\", \"salary\": 200 }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr-updated\" }\n" + "{ \"id\": 3, \"dep\": \"finance\" }");

    sql(
        "MERGE WITH SCHEMA EVOLUTION INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    // Rows should have null for missing salary column from source
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "hr-updated", 100), // updated, salary retains value
            row(2, "software", 200), // kept
            row(3, "finance", null)); // new row, salary is null
    assertEquals(
        "Should have expected rows",
        expectedRows,
        sql("SELECT id, dep, salary FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMergeWithSchemaEvolutionUsingDataFrameApi() {
    assumeThat(branch).as("Schema evolution does not work for branches currently").isNull();

    createAndInitTable(
        "id INT, dep STRING",
        "{ \"id\": 1, \"dep\": \"hr\" }\n" + "{ \"id\": 2, \"dep\": \"software\" }");

    createOrReplaceView(
        "source",
        "id INT, dep STRING, salary INT",
        "{ \"id\": 1, \"dep\": \"hr-updated\", \"salary\": 100 }\n"
            + "{ \"id\": 3, \"dep\": \"finance\", \"salary\": 300 }");

    spark
        .table("source")
        .mergeInto(commitTarget(), col(commitTarget() + ".id").equalTo(col("source.id")))
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .withSchemaEvolution()
        .merge();

    // The new 'salary' column should be added
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, "hr-updated", 100), // updated
            row(2, "software", null), // kept
            row(3, "finance", 300)); // new
    assertEquals(
        "Should have expected rows with schema evolution via DataFrame API",
        expectedRows,
        sql("SELECT id, dep, salary FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMergeWithSchemaEvolutionNestedStruct() {
    assumeThat(branch).as("Schema evolution does not work for branches currently").isNull();

    createAndInitTable(
        "id INT, s STRUCT<c1:INT,c2:STRING>",
        "{ \"id\": 1, \"s\": { \"c1\": 10, \"c2\": \"a\" } }\n"
            + "{ \"id\": 2, \"s\": { \"c1\": 20, \"c2\": \"b\" } }");

    createOrReplaceView(
        "source",
        "id INT, s STRUCT<c1:INT,c2:STRING,c3:INT>",
        "{ \"id\": 1, \"s\": { \"c1\": 100, \"c2\": \"aa\", \"c3\": 1000 } }\n"
            + "{ \"id\": 3, \"s\": { \"c1\": 300, \"c2\": \"cc\", \"c3\": 3000 } }");

    sql(
        "MERGE WITH SCHEMA EVOLUTION INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    // The nested struct should have the new c3 field
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, row(100, "aa", 1000)), // updated with nested field
            row(2, row(20, "b", null)), // kept, c3 is null
            row(3, row(300, "cc", 3000))); // new
    assertEquals(
        "Should have expected rows with nested struct evolution",
        expectedRows,
        sql("SELECT id, s FROM %s ORDER BY id", selectTarget()));
  }

  @TestTemplate
  public void testMergeWithSchemaEvolutionTypeWidening() {
    assumeThat(branch).as("Schema evolution does not work for branches currently").isNull();

    // Target has INT column
    createAndInitTable(
        "id INT, value INT", "{ \"id\": 1, \"value\": 100 }\n" + "{ \"id\": 2, \"value\": 200 }");

    // Source has LONG column - should widen INT to LONG
    createOrReplaceView(
        "source",
        "id INT, value LONG",
        "{ \"id\": 1, \"value\": 1000000000000 }\n" + "{ \"id\": 3, \"value\": 3000000000000 }");

    sql(
        "MERGE WITH SCHEMA EVOLUTION INTO %s AS t USING source AS s "
            + "ON t.id == s.id "
            + "WHEN MATCHED THEN "
            + "  UPDATE SET * "
            + "WHEN NOT MATCHED THEN "
            + "  INSERT *",
        commitTarget());

    // The 'value' column should be widened from INT to LONG
    ImmutableList<Object[]> expectedRows =
        ImmutableList.of(
            row(1, 1000000000000L), // updated with long value
            row(2, 200L), // kept, value promoted to long
            row(3, 3000000000000L)); // new row with long value
    assertEquals(
        "Should have expected rows with type widening",
        expectedRows,
        sql("SELECT id, value FROM %s ORDER BY id", selectTarget()));
  }

  @Override
  protected Map<String, String> extraTableProperties() {
    return Map.of();
  }
}
