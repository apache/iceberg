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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.TestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.internal.SQLConf;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;

public class TestDataFrameWriterV2 extends TestBaseWithCatalog {
  @BeforeEach
  public void createTable() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
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
    assertThatThrownBy(() -> threeColDF.writeTo(tableName).append())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field new_col not found in source schema");
  }

  @TestTemplate
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

    assertThatThrownBy(() -> threeColDF.writeTo(tableName).option("merge-schema", "true").append())
        .isInstanceOf(AnalysisException.class)
        .hasMessageContaining(
            "Cannot write to `testhadoop`.`default`.`table`, the reason is too many data columns");
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  public void testWriteWithCaseSensitiveOption() throws NoSuchTableException, ParseException {
    SparkSession sparkSession = spark.cloneSession();
    sparkSession
        .sql(
            String.format(
                "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
                tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA))
        .collect();

    String schema = "ID bigint, DaTa string";
    ImmutableList<String> records =
        ImmutableList.of("{ \"id\": 1, \"data\": \"a\" }", "{ \"id\": 2, \"data\": \"b\" }");

    // disable spark.sql.caseSensitive
    sparkSession.sql(String.format("SET %s=false", SQLConf.CASE_SENSITIVE().key()));
    Dataset<String> jsonDF =
        sparkSession.createDataset(ImmutableList.copyOf(records), Encoders.STRING());
    Dataset<Row> ds = sparkSession.read().schema(schema).json(jsonDF);
    // write should succeed
    ds.writeTo(tableName).option("merge-schema", "true").option("check-ordering", "false").append();
    List<Types.NestedField> fields =
        Spark3Util.loadIcebergTable(sparkSession, tableName).schema().asStruct().fields();
    // Additional columns should not be created
    assertThat(fields).hasSize(2);

    // enable spark.sql.caseSensitive
    sparkSession.sql(String.format("SET %s=true", SQLConf.CASE_SENSITIVE().key()));
    ds.writeTo(tableName).option("merge-schema", "true").option("check-ordering", "false").append();
    fields = Spark3Util.loadIcebergTable(sparkSession, tableName).schema().asStruct().fields();
    assertThat(fields).hasSize(4);
  }

  @TestTemplate
  public void testWithNullableProperty() throws Exception {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);
    spark.conf().set("spark.sql.iceberg.set-all-nullable-field", "true");
    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col struct<col1: bigint NOT NULL, col2: bigint NOT NULL> ",
            "{ \"id\": 1,  \"data\": \"a\", \"new_col\":{\"col1\":1, \"col2\":2} }");

    threeColDF.writeTo(tableName).option("merge-schema", "true").append();

    Dataset<Row> newColDF =
        jsonToDF(
            "id bigint, data string, new_col struct<col1: bigint NOT NULL, col3: bigint> ",
            "{ \"id\": 2,  \"data\": \"b\", \"new_col\":{\"col1\":2, \"col3\":3} }");
    newColDF.writeTo(tableName).option("merge-schema", "true").append();

    assertEquals(
        "Should have struct column with all the fields ",
        ImmutableList.of(row(1L, "a", row(1L, 2L, null)), row(2L, "b", row(2L, null, 3L))),
        sql("select * from %s order by id", tableName));
  }

  @TestTemplate
  public void testWithoutNullableProperty() throws Exception {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('%s'='true')",
        tableName, TableProperties.SPARK_WRITE_ACCEPT_ANY_SCHEMA);
    spark.conf().set("spark.sql.iceberg.set-all-nullable-field", "false");
    Dataset<Row> threeColDF =
        jsonToDF(
            "id bigint, data string, new_col struct<col1: bigint NOT NULL, col2: bigint NOT NULL> ",
            "{ \"id\": 1, \"data\": \"a\", \"new_col\":{\"col1\":1, \"col2\":2} }");

    threeColDF.writeTo(tableName).option("merge-schema", "true").append();

    Dataset<Row> newColDF =
        jsonToDF(
            "id bigint, data string, new_col struct<col1: bigint NOT NULL, col3: bigint> ",
            "{ \"id\": 1, \"data\": \"a\", \"new_col\":{\"col1\":2, \"col3\":3} }");
    assertThatThrownBy(() -> newColDF.writeTo(tableName).option("merge-schema", "true").append())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Cannot write incompatible dataset to table with schema");
  }
}
