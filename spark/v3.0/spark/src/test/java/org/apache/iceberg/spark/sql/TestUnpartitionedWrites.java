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

package org.apache.iceberg.spark.sql;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TestUnpartitionedWrites extends SparkCatalogTestBase {
  public TestUnpartitionedWrites(String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() {
    sql("CREATE TABLE %s (id bigint, data string) USING iceberg", tableName);
    sql("INSERT INTO %s VALUES (1, 'a'), (2, 'b'), (3, 'c')", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testInsertAppend() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    sql("INSERT INTO %s VALUES (4, 'd'), (5, 'e')", tableName);

    Assert.assertEquals("Should have 5 rows after insert", 5L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a"),
        row(2L, "b"),
        row(3L, "c"),
        row(4L, "d"),
        row(5L, "e")
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testInsertOverwrite() {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    sql("INSERT OVERWRITE %s VALUES (4, 'd'), (5, 'e')", tableName);

    Assert.assertEquals("Should have 2 rows after overwrite", 2L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(4L, "d"),
        row(5L, "e")
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Append() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).append();

    Assert.assertEquals("Should have 5 rows after insert", 5L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(1L, "a"),
        row(2L, "b"),
        row(3L, "c"),
        row(4L, "d"),
        row(5L, "e")
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2DynamicOverwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwritePartitions();

    Assert.assertEquals("Should have 2 rows after overwrite", 2L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(4L, "d"),
        row(5L, "e")
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @Test
  public void testDataFrameV2Overwrite() throws NoSuchTableException {
    Assert.assertEquals("Should have 3 rows", 3L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<SimpleRecord> data = ImmutableList.of(
        new SimpleRecord(4, "d"),
        new SimpleRecord(5, "e")
    );
    Dataset<Row> ds = spark.createDataFrame(data, SimpleRecord.class);

    ds.writeTo(tableName).overwrite(functions.col("id").$less$eq(3));

    Assert.assertEquals("Should have 2 rows after overwrite", 2L, scalarSql("SELECT count(*) FROM %s", tableName));

    List<Object[]> expected = ImmutableList.of(
        row(4L, "d"),
        row(5L, "e")
    );

    assertEquals("Row data should match expected", expected, sql("SELECT * FROM %s ORDER BY id", tableName));
  }
}
