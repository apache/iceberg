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
package org.apache.iceberg.spark.actions;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.AnalyzeTable;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestAnalyzeTableAction extends CatalogTestBase {

  @TestTemplate
  public void testAnalyzeTableAction() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    AnalyzeTable.Result results = actions.analyzeTable(table).columns("id", "data").execute();
    assertNotNull(results);

    Assertions.assertEquals(table.statisticsFiles().size(), 1);
    Assertions.assertEquals(table.statisticsFiles().get(0).blobMetadata().size(), 2);
    assertNotEquals(table.statisticsFiles().get(0).fileSizeInBytes(), 0);
  }

  @TestTemplate
  public void testAnalyzeTableActionWithoutExplicitColumns()
      throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    AnalyzeTable.Result results = actions.analyzeTable(table).execute();
    assertNotNull(results);

    Assertions.assertEquals(1, table.statisticsFiles().size());
    Assertions.assertEquals(2, table.statisticsFiles().get(0).blobMetadata().size());
    assertNotEquals(0, table.statisticsFiles().get(0).fileSizeInBytes());
  }

  @TestTemplate
  public void testAnalyzeTableForInvalidColumns() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ValidationException validationException =
        assertThrows(
            ValidationException.class, () -> actions.analyzeTable(table).columns("id1").execute());
    String message = validationException.getMessage();
    assertTrue(message.contains("No column with id1 name in the table"));
  }

  @TestTemplate
  public void testAnalyzeTableShouldThrowErrorForInvalidStatsType()
      throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    String statsName = "abcd";

    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                actions
                    .analyzeTable(table)
                    .types(Sets.newHashSet(statsName))
                    .columns("id", "data")
                    .execute());

    assertTrue(illegalArgumentException.getMessage().equalsIgnoreCase("type not supported"));
  }

  @TestTemplate
  public void testAnalyzeTableShouldThrowErrorForTypes()
      throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    // Define schema
    StructType schema =
        new StructType(
            new StructField[] {
              DataTypes.createStructField("name", DataTypes.StringType, true),
              DataTypes.createStructField(
                  "scores", DataTypes.createArrayType(DataTypes.IntegerType), true)
            });
    Dataset<Row> dataFrame = spark.createDataFrame(Lists.newArrayList(), schema);
    dataFrame.writeTo(tableName).using("iceberg").createOrReplace();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    IllegalArgumentException illegalArgumentException =
        assertThrows(
            IllegalArgumentException.class, () -> actions.analyzeTable(table).columns("scores"));
    assertTrue(
        illegalArgumentException.getMessage().contains("Cannot be applied to the given columns"));
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
