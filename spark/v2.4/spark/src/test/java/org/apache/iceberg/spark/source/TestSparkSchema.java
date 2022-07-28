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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestSparkSchema {

  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA =
      new Schema(
          optional(1, "id", Types.IntegerType.get()), optional(2, "data", Types.StringType.get()));
  private static SparkSession spark = null;

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  @BeforeClass
  public static void startSpark() {
    TestSparkSchema.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestSparkSchema.spark;
    TestSparkSchema.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testSparkReadSchemaIsHonored() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, null, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    StructType sparkReadSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, Metadata.empty())
            });

    Dataset<Row> resultDf =
        spark.read().schema(sparkReadSchema).format("iceberg").load(tableLocation);

    Row[] results = (Row[]) resultDf.collect();

    Assert.assertEquals("Result size matches", 1, results.length);
    Assert.assertEquals("Row length matches with sparkReadSchema", 1, results[0].length());
    Assert.assertEquals("Row content matches data", 1, results[0].getInt(0));
  }

  @Test
  public void testFailIfSparkReadSchemaIsOff() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, null, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    StructType sparkReadSchema =
        new StructType(
            new StructField[] {
              new StructField(
                  "idd", DataTypes.IntegerType, true, Metadata.empty()) // wrong field name
            });

    AssertHelpers.assertThrows(
        "Iceberg should not allow a projection that contain unknown fields",
        java.lang.IllegalArgumentException.class,
        "Field idd not found in source schema",
        () -> spark.read().schema(sparkReadSchema).format("iceberg").load(tableLocation));
  }

  @Test
  public void testSparkReadSchemaCombinedWithProjection() throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, null, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    StructType sparkReadSchema =
        new StructType(
            new StructField[] {
              new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
              new StructField("data", DataTypes.StringType, true, Metadata.empty())
            });

    Dataset<Row> resultDf =
        spark.read().schema(sparkReadSchema).format("iceberg").load(tableLocation).select("id");

    Row[] results = (Row[]) resultDf.collect();

    Assert.assertEquals("Result size matches", 1, results.length);
    Assert.assertEquals("Row length matches with sparkReadSchema", 1, results[0].length());
    Assert.assertEquals("Row content matches data", 1, results[0].getInt(0));
  }

  @Test
  public void testFailSparkReadSchemaCombinedWithProjectionWhenSchemaDoesNotContainProjection()
      throws IOException {
    String tableLocation = temp.newFolder("iceberg-table").toString();

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    tables.create(SCHEMA, spec, null, tableLocation);

    List<SimpleRecord> expectedRecords = Lists.newArrayList(new SimpleRecord(1, "a"));
    Dataset<Row> originalDf = spark.createDataFrame(expectedRecords, SimpleRecord.class);
    originalDf.select("id", "data").write().format("iceberg").mode("append").save(tableLocation);

    StructType sparkReadSchema =
        new StructType(
            new StructField[] {
              new StructField("data", DataTypes.StringType, true, Metadata.empty())
            });

    AssertHelpers.assertThrows(
        "Spark should not allow a projection that is not included in the read schema",
        org.apache.spark.sql.AnalysisException.class,
        "cannot resolve '`id`' given input columns: [data]",
        () ->
            spark
                .read()
                .schema(sparkReadSchema)
                .format("iceberg")
                .load(tableLocation)
                .select("id"));
  }
}
