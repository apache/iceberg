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

package com.netflix.iceberg.spark.source;

import com.google.common.collect.Lists;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableProperties;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.util.List;

import static com.netflix.iceberg.types.Types.NestedField.optional;
import static com.netflix.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestPartitionValues {
  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("data")
      .build();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestPartitionValues.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestPartitionValues.spark;
    TestPartitionValues.spark = null;
    spark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String format;

  public TestPartitionValues(String format) {
    this.format = format;
  }

  @Test
  public void testNullPartitionValue() throws Exception {
    String desc = "null_part";
    File parent = temp.newFolder(desc);
    File location = new File(parent, "test");
    File dataFolder = new File(location, "data");
    Assert.assertTrue("mkdirs should succeed", dataFolder.mkdirs());

    HadoopTables tables = new HadoopTables(spark.sparkContext().hadoopConfiguration());
    Table table = tables.create(SCHEMA, SPEC, location.toString());
    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    try {
      // TODO: incoming columns must be ordered according to the table's schema
      df.select("id", "data").write()
          .format("iceberg")
          .mode("append")
          .save(location.toString());

      Dataset<Row> result = spark.read()
          .format("iceberg")
          .load(location.toString());

      List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();

      Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
      Assert.assertEquals("Result rows should match", expected, actual);

    } finally {
      TestTables.clearTables();
    }
  }

}
