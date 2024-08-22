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
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestComputeTableStatsAction extends CatalogTestBase {

  @TestTemplate
  public void testComputeTableStatsAction() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
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
    ComputeTableStats.Result results =
        actions.computeTableStats(table).columns("id", "data").execute();
    assertNotNull(results);

    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);

    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 2);

    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    Assertions.assertEquals(
        blobMetadata.properties().get(NDVSketchGenerator.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY),
        String.valueOf(4));
  }

  @TestTemplate
  public void testComputeTableStatsActionWithoutExplicitColumns()
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
    ComputeTableStats.Result results = actions.computeTableStats(table).execute();
    assertNotNull(results);

    Assertions.assertEquals(1, table.statisticsFiles().size());
    StatisticsFile statisticsFile = table.statisticsFiles().get(0);
    Assertions.assertEquals(2, statisticsFile.blobMetadata().size());
    assertNotEquals(0, statisticsFile.fileSizeInBytes());
    assertNotEquals(
        4,
        statisticsFile
            .blobMetadata()
            .get(0)
            .properties()
            .get(NDVSketchGenerator.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY));
    assertNotEquals(
        4,
        statisticsFile
            .blobMetadata()
            .get(1)
            .properties()
            .get(NDVSketchGenerator.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY));
  }

  @TestTemplate
  public void testComputeTableStatsForInvalidColumns() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    // Append data to create snapshot
    sql("INSERT into %s values(1, 'abcd')", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> actions.computeTableStats(table).columns("id1").execute());
    String message = exception.getMessage();
    assertTrue(message.contains("No column with id1 name in the table"));
  }

  @TestTemplate
  public void testComputeTableStatsWithNoSnapshots() throws NoSuchTableException, ParseException {
    assumeTrue(catalogName.equals("spark_catalog"));
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result result = actions.computeTableStats(table).columns("id").execute();
    Assertions.assertNull(result.statisticsFile());
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
