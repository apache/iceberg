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
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
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
    AnalyzeTable.Result results =
        actions.analyzeTable(table).columns(Sets.newHashSet("id", "data")).execute();
    assertNotNull(results);

    Assertions.assertEquals(table.statisticsFiles().size(), 1);
    Assertions.assertEquals(table.statisticsFiles().get(0).blobMetadata().size(), 2);
    assertNotEquals(table.statisticsFiles().get(0).fileSizeInBytes(), 0);
  }

  @TestTemplate
  public void testAnalyzeTableActionWithErrors() {
    // TODO
  }

  @TestTemplate
  public void testAnalyzeTableForTypes() {
    // TODO
  }

  @TestTemplate
  public void testAnalyzeTableForInvalidColumns() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql(
        "CREATE TABLE %s (id int, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2')",
        tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ValidationException validationException =
        assertThrows(
            ValidationException.class,
            () -> actions.analyzeTable(table).columns(Sets.newHashSet("id1")).execute());
    String message = validationException.getMessage();
    assertTrue(message.contains("No column with id1 name in the table"));
  }

  @TestTemplate
  public void testAnalyzeTableShouldThrowErrorForInvalidStatsType()
      throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql(
        "CREATE TABLE %s (id int, data string) USING iceberg TBLPROPERTIES"
            + "('format-version'='2')",
        tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    String statsName = "abcd";
    AnalyzeTable.Result result =
        actions
            .analyzeTable(table)
            .stats(Sets.newHashSet(statsName))
            .columns(Sets.newHashSet("id", "data"))
            .execute();

    Assertions.assertEquals(result.analysisResults().size(), 1);
    AnalyzeTable.AnalysisResult analysisResult = result.analysisResults().get(0);
    Assertions.assertEquals(analysisResult.statsName(), statsName);
    Assertions.assertFalse(analysisResult.statsCollected());
    Assertions.assertTrue(
        () ->
            analysisResult.errors().size() == 1
                && analysisResult.errors().get(0).contains("Stats type not supported"));
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
