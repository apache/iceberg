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

import static org.apache.iceberg.types.Types.NestedField.optional;

import java.io.File;
import java.io.Serializable;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.events.Listeners;
import org.apache.iceberg.events.ScanEvent;
import org.apache.iceberg.expressions.And;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSelect {
  private static final HadoopTables TABLES = new HadoopTables(new Configuration());
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()),
          optional(2, "data", Types.StringType.get()),
          optional(3, "doubleVal", Types.DoubleType.get()));

  private static SparkSession spark;

  private static int scanEventCount = 0;
  private static ScanEvent lastScanEvent = null;

  private Table table;

  static {
    Listeners.register(
        event -> {
          scanEventCount += 1;
          lastScanEvent = event;
        },
        ScanEvent.class);
  }

  @BeforeClass
  public static void startSpark() {
    spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = spark;
    spark = null;
    currentSpark.stop();
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private String tableLocation = null;

  @Before
  public void init() throws Exception {
    File tableDir = temp.newFolder();
    this.tableLocation = tableDir.toURI().toString();

    table = TABLES.create(SCHEMA, tableLocation);

    List<Record> rows =
        Lists.newArrayList(
            new Record(1, "a", 1.0), new Record(2, "b", 2.0), new Record(3, "c", Double.NaN));

    Dataset<Row> df = spark.createDataFrame(rows, Record.class);

    df.select("id", "data", "doubleVal")
        .write()
        .format("iceberg")
        .mode("append")
        .save(tableLocation);

    table.refresh();

    Dataset<Row> results = spark.read().format("iceberg").load(tableLocation);
    results.createOrReplaceTempView("table");

    scanEventCount = 0;
    lastScanEvent = null;
  }

  @Test
  public void testSelect() {
    List<Record> expected =
        ImmutableList.of(
            new Record(1, "a", 1.0), new Record(2, "b", 2.0), new Record(3, "c", Double.NaN));

    Assert.assertEquals(
        "Should return all expected rows",
        expected,
        sql("select * from table", Encoders.bean(Record.class)));
  }

  @Test
  public void testSelectRewrite() {
    List<Record> expected = ImmutableList.of(new Record(3, "c", Double.NaN));

    Assert.assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM table where doubleVal = double('NaN')", Encoders.bean(Record.class)));
    Assert.assertEquals("Should create only one scan", 1, scanEventCount);

    Expression filter = lastScanEvent.filter();
    Assert.assertEquals("Should create AND expression", Expression.Operation.AND, filter.op());
    Expression left = ((And) filter).left();
    Expression right = ((And) filter).right();

    Assert.assertEquals(
        "Left expression should be NOT_NULL", Expression.Operation.NOT_NULL, left.op());
    Assert.assertTrue(
        "Left expression should contain column name 'doubleVal'",
        left.toString().contains("doubleVal"));
    Assert.assertEquals(
        "Right expression should be IS_NAN", Expression.Operation.IS_NAN, right.op());
    Assert.assertTrue(
        "Right expression should contain column name 'doubleVal'",
        right.toString().contains("doubleVal"));
  }

  @Test
  public void testProjection() {
    List<Integer> expected = ImmutableList.of(1, 2, 3);

    Assert.assertEquals(
        "Should return all expected rows", expected, sql("SELECT id FROM table", Encoders.INT()));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals(
        "Should not push down a filter", Expressions.alwaysTrue(), lastScanEvent.filter());
    Assert.assertEquals(
        "Should project only the id column",
        table.schema().select("id").asStruct(),
        lastScanEvent.projection().asStruct());
  }

  @Test
  public void testExpressionPushdown() {
    List<String> expected = ImmutableList.of("b");

    Assert.assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT data FROM table WHERE id = 2", Encoders.STRING()));

    Assert.assertEquals("Should create only one scan", 1, scanEventCount);
    Assert.assertEquals(
        "Should project only id and data columns",
        table.schema().select("id", "data").asStruct(),
        lastScanEvent.projection().asStruct());
  }

  private <T> List<T> sql(String str, Encoder<T> encoder) {
    return spark.sql(str).as(encoder).collectAsList();
  }

  public static class Record implements Serializable {
    private Integer id;
    private String data;
    private Double doubleVal;

    public Record() {}

    Record(Integer id, String data, Double doubleVal) {
      this.id = id;
      this.data = data;
      this.doubleVal = doubleVal;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public void setData(String data) {
      this.data = data;
    }

    public void setDoubleVal(Double doubleVal) {
      this.doubleVal = doubleVal;
    }

    public Integer getId() {
      return id;
    }

    public String getData() {
      return data;
    }

    public Double getDoubleVal() {
      return doubleVal;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Record record = (Record) o;
      return Objects.equal(id, record.id)
          && Objects.equal(data, record.data)
          && Objects.equal(doubleVal, record.doubleVal);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(id, data, doubleVal);
    }
  }
}
