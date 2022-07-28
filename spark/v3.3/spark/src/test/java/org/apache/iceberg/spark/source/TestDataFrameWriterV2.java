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
package org.apache.iceberg.spark.source;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestDataFrameWriterV2 extends SparkTestBaseWithCatalog {
  @Before
  public void createTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testMergeSchemaFailsWithoutWriterOption() throws Exception {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);

    Dataset<Row> twoColDF =
        jsonToDF(
            "id bigint, data string",
            "{ \"id\": 1, \"data\": \"a\" }",
            "{ \"id\": 2, \"data\": \"b\" }");

    twoColDF.writeTo(tableName).append();

    assertEquals(
        "Should have initial 2-column rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("select * from %s order by id", tableName));

    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col float",
            "{ \"id\": 3, \"data\": \"c\", \"new_col\": 12.06 }",
            "{ \"id\": 4, \"data\": \"d\", \"new_col\": 14.41 }");

    // this has a different error message than the case without accept-any-schema because it uses
    // Iceberg checks
    AssertHelpers.assertThrows(
        "Should fail when merge-schema is not enabled on the writer",
        IllegalArgumentException.class,
        "Field new_col not found in source schema",
        () -> {
          try {
            threeColDF.writeTo(tableName).append();
          } catch (NoSuchTableException e) {
            // needed because append has checked exceptions
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testMergeSchemaWithoutAcceptAnySchema() throws Exception {
    Dataset<Row> twoColDF =
        jsonToDF(
            "id bigint, data string",
            "{ \"id\": 1, \"data\": \"a\" }",
            "{ \"id\": 2, \"data\": \"b\" }");

    twoColDF.writeTo(tableName).append();

    assertEquals(
        "Should have initial 2-column rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("select * from %s order by id", tableName));

    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col float",
            "{ \"id\": 3, \"data\": \"c\", \"new_col\": 12.06 }",
            "{ \"id\": 4, \"data\": \"d\", \"new_col\": 14.41 }");

    AssertHelpers.assertThrows(
        "Should fail when accept-any-schema is not enabled on the table",
        AnalysisException.class,
        "too many data columns",
        () -> {
          try {
            threeColDF.writeTo(tableName).option("merge-schema", "true").append();
          } catch (NoSuchTableException e) {
            // needed because append has checked exceptions
            throw new RuntimeException(e);
          }
        });
  }

  @Test
  public void testMergeSchemaSparkProperty() throws Exception {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);

    Dataset<Row> twoColDF =
        jsonToDF(
            "id bigint, data string",
            "{ \"id\": 1, \"data\": \"a\" }",
            "{ \"id\": 2, \"data\": \"b\" }");

    twoColDF.writeTo(tableName).append();

    assertEquals(
        "Should have initial 2-column rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("select * from %s order by id", tableName));

    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col float",
            "{ \"id\": 3, \"data\": \"c\", \"new_col\": 12.06 }",
            "{ \"id\": 4, \"data\": \"d\", \"new_col\": 14.41 }");

    threeColDF.writeTo(tableName).option("mergeSchema", "true").append();

    assertEquals(
        "Should have 3-column rows",
        ImmutableList.of(
            row(1L, "a", null), row(2L, "b", null), row(3L, "c", 12.06F), row(4L, "d", 14.41F)),
        sql("select * from %s order by id", tableName));
  }

  @Test
  public void testMergeSchemaIcebergProperty() throws Exception {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);

    Dataset<Row> twoColDF =
        jsonToDF(
            "id bigint, data string",
            "{ \"id\": 1, \"data\": \"a\" }",
            "{ \"id\": 2, \"data\": \"b\" }");

    twoColDF.writeTo(tableName).append();

    assertEquals(
        "Should have initial 2-column rows",
        ImmutableList.of(row(1L, "a"), row(2L, "b")),
        sql("select * from %s order by id", tableName));

    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col float",
            "{ \"id\": 3, \"data\": \"c\", \"new_col\": 12.06 }",
            "{ \"id\": 4, \"data\": \"d\", \"new_col\": 14.41 }");

    threeColDF.writeTo(tableName).option("merge-schema", "true").append();

    assertEquals(
        "Should have 3-column rows",
        ImmutableList.of(
            row(1L, "a", null), row(2L, "b", null), row(3L, "c", 12.06F), row(4L, "d", 14.41F)),
        sql("select * from %s order by id", tableName));
  }
}
