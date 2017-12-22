/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.spark.source;

import com.google.common.collect.Lists;
import com.netflix.iceberg.Files;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableProperties;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.avro.AvroIterable;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.spark.data.AvroDataTest;
import com.netflix.iceberg.spark.data.RandomData;
import com.netflix.iceberg.spark.data.SparkAvroReader;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.netflix.iceberg.spark.SparkSchemaUtil.convert;
import static com.netflix.iceberg.spark.data.TestHelpers.assertEqualsSafe;
import static com.netflix.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;

@RunWith(Parameterized.class)
public class TestAvroWrite extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  private String format = null;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  public TestAvroWrite(String format) {
    this.format = format;
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;
  private static JavaSparkContext sc = null;

  @BeforeClass
  public static void startSpark() {
    TestAvroWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
    TestAvroWrite.sc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestAvroWrite.spark;
    TestAvroWrite.spark = null;
    TestAvroWrite.sc = null;
    spark.stop();
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");
    Assert.assertTrue("Mkdir should succeed", location.mkdirs());

    HadoopTables tables = new HadoopTables(CONF);
    Table table = tables.create(schema, PartitionSpec.unpartitioned(), location.toString());
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<Record> expected = RandomData.generate(tableSchema, 100, 0L);
    Dataset<Row> df = createDataset(expected, tableSchema);

    df.write()
        .format("iceberg")
        .mode("append")
        .option("iceberg.table.location", location.toString())
        .save();

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .option("iceberg.table.location", location.toString())
        .load();

    List<Row> actual = result.collectAsList();

    Assert.assertEquals("Result size should match expected", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i += 1) {
      assertEqualsSafe(tableSchema.asStruct(), expected.get(i), actual.get(i));
    }
  }

  private Dataset<Row> createDataset(List<Record> records, Schema schema) throws IOException {
    // this uses the SparkAvroReader to create a DataFrame from the list of records
    // it assumes that SparkAvroReader is correct
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      for (Record rec : records) {
        writer.add(rec);
      }
    }

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader = Avro.read(Files.localInput(testFile))
        .createReaderFunc(SparkAvroReader::new)
        .project(schema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    // make sure the dataframe matches the records before moving on
    for (int i = 0; i < records.size(); i += 1) {
      assertEqualsUnsafe(schema.asStruct(), records.get(i), rows.get(i));
    }

    JavaRDD<InternalRow> rdd = sc.parallelize(rows);
    return spark.internalCreateDataFrame(JavaRDD.toRDD(rdd), convert(schema), false);
  }
}
