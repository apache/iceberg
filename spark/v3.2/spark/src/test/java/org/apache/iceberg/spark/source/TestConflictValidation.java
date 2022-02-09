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
import java.util.Map;
import org.apache.iceberg.IsolationLevel;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestConflictValidation extends SparkCatalogTestBase {

  public TestConflictValidation(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg PARTITIONED BY (id)", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testDynamicValidation() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);
    long snapshotId = table.currentSnapshot().snapshotId();

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.select("id", "data").writeTo(tableName).append();

    // Validating from previous snapshot finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    Exception thrown = null;
    try {
      conflictingDf.writeTo(tableName)
          .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
          .option(SparkWriteOptions.DYNAMIC_OVERWRITE_ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
          .overwritePartitions();
    } catch (Exception e) {
      thrown = e;
    }
    Assert.assertNotNull("Expected validation exception but none thrown", thrown);
    Assert.assertTrue("Nested validation exception", thrown.getCause() instanceof ValidationException);
    Assert.assertTrue(thrown.getCause().getMessage().contains(
        "Found conflicting files that can contain records matching partitions [id=1]"));

    // Validating from latest snapshot should succeed
    table.refresh();
    snapshotId = table.currentSnapshot().snapshotId();
    conflictingDf.writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
        .option(SparkWriteOptions.DYNAMIC_OVERWRITE_ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwritePartitions();
  }

  @Test
  public void testDynamicValidationNoSnapshotId() throws Exception {
    Table table = validationCatalog.loadTable(tableIdent);

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"));
    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);
    df.select("id", "data").writeTo(tableName).append();

    // Validating from previous snapshot finds conflicts
    Dataset<Row> conflictingDf = spark.createDataFrame(records, SimpleRecord.class);
    Exception thrown = null;
    try {
      conflictingDf.writeTo(tableName)
          .option(SparkWriteOptions.DYNAMIC_OVERWRITE_ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
          .overwritePartitions();
    } catch (Exception e) {
      thrown = e;
    }
    Assert.assertNotNull("Expected validation exception but none thrown", thrown);
    Assert.assertTrue("Nested validation exception", thrown.getCause() instanceof ValidationException);
    Assert.assertTrue(thrown.getCause().getMessage().contains(
        "Cannot determine history between starting snapshot"));

    // Validating from latest snapshot should succeed
    table.refresh();
    long snapshotId = table.currentSnapshot().snapshotId();
    conflictingDf.writeTo(tableName)
        .option(SparkWriteOptions.VALIDATE_FROM_SNAPSHOT_ID, String.valueOf(snapshotId))
        .option(SparkWriteOptions.DYNAMIC_OVERWRITE_ISOLATION_LEVEL, IsolationLevel.SERIALIZABLE.toString())
        .overwritePartitions();
  }
}
