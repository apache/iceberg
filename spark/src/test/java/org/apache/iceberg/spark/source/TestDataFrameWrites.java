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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.spark.data.AvroDataTest;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.data.SparkAvroReader;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.spark.SparkSchemaUtil.convert;
import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsSafe;
import static org.apache.iceberg.spark.data.TestHelpers.assertEqualsUnsafe;

@RunWith(Parameterized.class)
public class TestDataFrameWrites extends AvroDataTest {
  private static final Configuration CONF = new Configuration();

  private final String format;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  public TestDataFrameWrites(String format) {
    this.format = format;
  }

  private static SparkSession spark = null;
  private static JavaSparkContext sc = null;

  @BeforeClass
  public static void startSpark() {
    TestDataFrameWrites.spark = SparkSession.builder().master("local[2]").getOrCreate();
    TestDataFrameWrites.sc = new JavaSparkContext(spark.sparkContext());
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestDataFrameWrites.spark;
    TestDataFrameWrites.spark = null;
    TestDataFrameWrites.sc = null;
    currentSpark.stop();
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    File location = createTableFolder();
    Table table = createTable(schema, location);
    writeAndValidateWithLocations(table, location, new File(location, "data"));
  }

  @Test
  public void testWriteWithCustomDataLocation() throws IOException {
    File location = createTableFolder();
    File tablePropertyDataLocation = temp.newFolder("test-table-property-data-dir");
    Table table = createTable(new Schema(SUPPORTED_PRIMITIVES.fields()), location);
    table.updateProperties().set(
        TableProperties.WRITE_NEW_DATA_LOCATION, tablePropertyDataLocation.getAbsolutePath()).commit();
    writeAndValidateWithLocations(table, location, tablePropertyDataLocation);
  }

  private File createTableFolder() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");
    Assert.assertTrue("Mkdir should succeed", location.mkdirs());
    return location;
  }

  private Table createTable(Schema schema, File location) {
    HadoopTables tables = new HadoopTables(CONF);
    return tables.create(schema, PartitionSpec.unpartitioned(), location.toString());
  }

  private void writeAndValidateWithLocations(Table table, File location, File expectedDataDir) throws IOException {
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    table.updateProperties().set(TableProperties.DEFAULT_FILE_FORMAT, format).commit();

    List<Record> expected = RandomData.generateList(tableSchema, 100, 0L);
    Dataset<Row> df = createDataset(expected, tableSchema);
    DataFrameWriter<?> writer = df.write().format("iceberg").mode("append");

    writer.save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> actual = result.collectAsList();

    Assert.assertEquals("Result size should match expected", expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i += 1) {
      assertEqualsSafe(tableSchema.asStruct(), expected.get(i), actual.get(i));
    }

    table.currentSnapshot().addedFiles().forEach(dataFile ->
        Assert.assertTrue(
            String.format(
                "File should have the parent directory %s, but has: %s.",
                expectedDataDir.getAbsolutePath(),
                dataFile.path()),
            URI.create(dataFile.path().toString()).getPath().startsWith(expectedDataDir.getAbsolutePath())));
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
