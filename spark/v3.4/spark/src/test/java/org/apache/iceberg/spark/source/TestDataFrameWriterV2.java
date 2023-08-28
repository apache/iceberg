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

import java.util.List;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.internal.SQLConf;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
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
    Assertions.assertThatThrownBy(() -> threeColDF.writeTo(tableName).append())
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Field new_col not found in source schema");
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

    Assertions.assertThatThrownBy(
            () -> threeColDF.writeTo(tableName).option("merge-schema", "true").append())
        .isInstanceOf(AnalysisException.class)
        .hasMessageStartingWith(
            "Cannot write to 'testhadoop.default.table', too many data columns");
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

  @Test
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
    Assert.assertEquals(2, fields.size());

    // enable spark.sql.caseSensitive
    sparkSession.sql(String.format("SET %s=true", SQLConf.CASE_SENSITIVE().key()));
    ds.writeTo(tableName).option("merge-schema", "true").option("check-ordering", "false").append();
    fields = Spark3Util.loadIcebergTable(sparkSession, tableName).schema().asStruct().fields();
    Assert.assertEquals(4, fields.size());
  }

  @Test
  public void testMergeSchemaInMiddle() throws Exception {
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
                    "id bigint, new_col float, data string",
                    "{ \"id\": 3, \"new_col\": 12.06, \"data\": \"c\" }",
                    "{ \"id\": 4, \"new_col\": 14.41, \"data\": \"d\" }");

    threeColDF.writeTo(tableName).option("mergeSchema", "true").append();

    assertEquals(
            "Should have 3-column rows",
            ImmutableList.of(
                    row(1L, null, "a"), row(2L, null, "b"), row(3L, 12.06F, "c"), row(4L, 14.41F, "d")),
            sql("select * from %s order by id", tableName));
  }

  @Test
  public void testMergeSchemaAtStart() throws Exception {
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
                    "new_col float, id bigint, data string",
                    "{ \"new_col\": 12.06, \"id\": 3, \"data\": \"c\" }",
                    "{ \"new_col\": 14.41, \"id\": 4, \"data\": \"d\" }");

    threeColDF.writeTo(tableName).option("mergeSchema", "true").append();

    assertEquals(
            "Should have 3-column rows",
            ImmutableList.of(
                    row(null, 1L, "a"), row(null, 2L, "b"), row(12.06F, 3L, "c"), row(14.41F, 4L, "d")),
            sql("select * from %s order by id", tableName));
  }

  @Test
  public void testMergeSchemaMultiple() throws Exception {
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
                    "new_col float, new_col2 bigint, id bigint, new_col3 float, new_col4 bigint, data string, new_col5 float, new_col6 bigint",
                    "{ \"new_col\": 12.06, \"new_col2\": 5, \"id\": 3, \"new_col3\": 11.06, \"new_col4\": 6, \"data\": \"c\", \"new_col5\": 15.06, \"new_col6\": 8 }",
                    "{ \"new_col\": 14.41, \"new_col2\": 4, \"id\": 4, \"new_col3\": 13.06, \"new_col4\": 7, \"data\": \"d\", \"new_col5\": 16.06, \"new_col6\": 9 }");

    threeColDF.writeTo(tableName).option("mergeSchema", "true").append();

    assertThat(
            spark.table(tableName).schema()
    ).as("Schema should be correct").isEqualTo(
            DataTypes.createStructType(new StructField[] {
                    DataTypes.createStructField("new_col", DataTypes.FloatType, true),
                    DataTypes.createStructField("new_col2", DataTypes.LongType, true),
                    DataTypes.createStructField("id", DataTypes.LongType, true),
                    DataTypes.createStructField("new_col3", DataTypes.FloatType, true),
                    DataTypes.createStructField("new_col4", DataTypes.LongType, true),
                    DataTypes.createStructField("data", DataTypes.StringType, true),
                    DataTypes.createStructField("new_col5", DataTypes.FloatType, true),
                    DataTypes.createStructField("new_col6", DataTypes.LongType, true)
            })
    );

    assertEquals(
            "Should have 8-column rows",
            ImmutableList.of(
                    row(null, null, 1L, null, null, "a", null, null),
                    row(null, null, 2L, null, null, "b", null, null),
                    row(12.06F, 5L, 3L, 11.06F, 6L, "c", 15.06F, 8L),
                    row(14.41F, 4L, 4L, 13.06F, 7L, "d", 16.06F, 9L)),
            sql("select * from %s order by id", tableName));
  }

  @Test
  public void testMergeSchemaFailsOnIncompatible() throws Exception {
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
                    "id string, data string",
                    "{ \"id\": \"3\", \"data\": \"c\" }",
                    "{ \"id\": \"4\", \"data\": \"d\" }");

    // this has a different error message than the case without accept-any-schema because it uses
    // Iceberg checks
    assertThatThrownBy(() -> threeColDF.writeTo(tableName).option("merge-schema", "true").append())
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot change column type: id: long -> string");
  }
}
