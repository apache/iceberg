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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
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

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestParquetWrite {
  private static final Configuration CONF = new Configuration();
  private static final Schema SCHEMA = new Schema(
      optional(1, "id", Types.IntegerType.get()),
      optional(2, "data", Types.StringType.get())
  );

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestParquetWrite.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestParquetWrite.spark;
    TestParquetWrite.spark = null;
    currentSpark.stop();
  }

  @Test
  public void testBasicWrite() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(localInput(manifest.path()), null)) {
        Assert.assertNotNull("Split offsets not present", file.splitOffsets());
        Assert.assertEquals("Should have reported record count as 1", 1, file.recordCount());
        Assert.assertNotNull("Column sizes metric not present", file.columnSizes());
        Assert.assertNotNull("Counts metric not present", file.valueCounts());
        Assert.assertNotNull("Null value counts metric not present", file.nullValueCounts());
        Assert.assertNotNull("Lower bounds metric not present", file.lowerBounds());
        Assert.assertNotNull("Upper bounds metric not present", file.upperBounds());
      }
    }
  }

  @Test
  public void testAppend() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "a"),
        new SimpleRecord(5, "b"),
        new SimpleRecord(6, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    df.withColumn("id", df.col("id").plus(3)).select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testOverwrite() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("id").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> records = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "a"),
        new SimpleRecord(3, "c"),
        new SimpleRecord(4, "b"),
        new SimpleRecord(6, "c")
    );

    Dataset<Row> df = spark.createDataFrame(records, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    // overwrite with 2*id to replace record 2, append 4 and 6
    df.withColumn("id", df.col("id").multiply(2)).select("id", "data").write()
        .format("iceberg")
        .mode("overwrite")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedOverwrite() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, "a"),
        new SimpleRecord(2, "b"),
        new SimpleRecord(3, "c")
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    // overwrite with the same data; should not produce two copies
    df.select("id", "data").write()
        .format("iceberg")
        .mode("overwrite")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testUnpartitionedCreateWithTargetFileSizeViaTableProperties() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    table.updateProperties()
        .set(TableProperties.WRITE_TARGET_FILE_SIZE_BYTES, "4") // ~4 bytes; low enough to trigger
        .commit();

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(4000);
    for (int i = 0; i < 4000; i++) {
      expected.add(new SimpleRecord(i, "a"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(localInput(manifest.path()), null)) {
        files.add(file);
      }
    }
    Assert.assertEquals("Should have 4 DataFiles", 4, files.size());
    Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
  }

  @Test
  public void testPartitionedCreateWithTargetFileSizeViaOption() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("data").build();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayListWithCapacity(8000);
    for (int i = 0; i < 2000; i++) {
      expected.add(new SimpleRecord(i, "a"));
      expected.add(new SimpleRecord(i, "b"));
      expected.add(new SimpleRecord(i, "c"));
      expected.add(new SimpleRecord(i, "d"));
    }

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id", "data").sort("data").write()
        .format("iceberg")
        .mode("append")
        .option("target-file-size-bytes", 4) // ~4 bytes; low enough to trigger
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);

    List<DataFile> files = Lists.newArrayList();
    for (ManifestFile manifest : table.currentSnapshot().manifests()) {
      for (DataFile file : ManifestReader.read(localInput(manifest.path()), null)) {
        files.add(file);
      }
    }
    Assert.assertEquals("Should have 8 DataFiles", 8, files.size());
    Assert.assertTrue("All DataFiles contain 1000 rows", files.stream().allMatch(d -> d.recordCount() == 1000));
  }

  @Test
  public void testWriteProjection() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Table table = tables.create(SCHEMA, spec, location.toString());

    List<SimpleRecord> expected = Lists.newArrayList(
        new SimpleRecord(1, null),
        new SimpleRecord(2, null),
        new SimpleRecord(3, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, SimpleRecord.class);

    df.select("id").write() // select only id column
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<SimpleRecord> actual = result.orderBy("id").as(Encoders.bean(SimpleRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testWriteProjectionWithMiddle() throws IOException {
    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(CONF);
    PartitionSpec spec = PartitionSpec.unpartitioned();
    Schema schema = new Schema(
        optional(1, "c1", Types.IntegerType.get()),
        optional(2, "c2", Types.StringType.get()),
        optional(3, "c3", Types.StringType.get())
    );
    Table table = tables.create(schema, spec, location.toString());

    List<ThreeColumnRecord> expected = Lists.newArrayList(
        new ThreeColumnRecord(1, null, "hello"),
        new ThreeColumnRecord(2, null, "world"),
        new ThreeColumnRecord(3, null, null)
    );

    Dataset<Row> df = spark.createDataFrame(expected, ThreeColumnRecord.class);

    df.select("c1", "c3").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<ThreeColumnRecord> actual = result.orderBy("c1").as(Encoders.bean(ThreeColumnRecord.class)).collectAsList();
    Assert.assertEquals("Number of rows should match", expected.size(), actual.size());
    Assert.assertEquals("Result rows should match", expected, actual);
  }

  @Test
  public void testNestedPartitioning() throws IOException {
    Schema nestedSchema = new Schema(
        optional(1, "id", Types.IntegerType.get()),
        optional(2, "data", Types.StringType.get()),
        optional(3, "nestedData", Types.StructType.of(
            optional(4, "id", Types.IntegerType.get()),
            optional(5, "moreData", Types.StringType.get()))),
        optional(6, "timestamp", Types.TimestampType.withZone())
    );

    File parent = temp.newFolder("parquet");
    File location = new File(parent, "test");

    HadoopTables tables = new HadoopTables(new Configuration());
    PartitionSpec spec = PartitionSpec.builderFor(nestedSchema)
        .identity("id")
        .day("timestamp")
        .identity("nestedData.moreData")
        .build();
    Table table = tables.create(nestedSchema, spec, location.toString());

    List<String> jsons = Lists.newArrayList(
        "{ \"id\": 1, \"data\": \"a\", \"nestedData\": { \"id\": 100, \"moreData\": \"p1\"}, " +
            "\"timestamp\": \"2017-12-01T10:12:55.034Z\" }",
        "{ \"id\": 2, \"data\": \"b\", \"nestedData\": { \"id\": 200, \"moreData\": \"p1\"}, " +
            "\"timestamp\": \"2017-12-02T10:12:55.034Z\" }",
        "{ \"id\": 3, \"data\": \"c\", \"nestedData\": { \"id\": 300, \"moreData\": \"p2\"}, " +
            "\"timestamp\": \"2017-12-03T10:12:55.034Z\" }",
        "{ \"id\": 4, \"data\": \"d\", \"nestedData\": { \"id\": 400, \"moreData\": \"p2\"}, " +
            "\"timestamp\": \"2017-12-04T10:12:55.034Z\" }"
    );
    Dataset<Row> df = spark.read().schema(SparkSchemaUtil.convert(nestedSchema))
        .json(spark.createDataset(jsons, Encoders.STRING()));

    // TODO: incoming columns must be ordered according to the table's schema
    df.select("id", "data", "nestedData", "timestamp").write()
        .format("iceberg")
        .mode("append")
        .save(location.toString());

    table.refresh();

    Dataset<Row> result = spark.read()
        .format("iceberg")
        .load(location.toString());

    List<Row> actual = result.orderBy("id").collectAsList();
    Assert.assertEquals("Number of rows should match", jsons.size(), actual.size());
    Assert.assertEquals("Row 1 col 1 is 1", 1, actual.get(0).getInt(0));
    Assert.assertEquals("Row 1 col 2 is a", "a", actual.get(0).getString(1));
    Assert.assertEquals("Row 1 col 3,1 is 100", 100, actual.get(0).getStruct(2).getInt(0));
    Assert.assertEquals("Row 1 col 3,2 is p1", "p1", actual.get(0).getStruct(2).getString(1));
    Assert.assertEquals("Row 1 col 4 is 2017-12-01T10:12:55.034+00:00",
        0, actual.get(0).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-01T10:12:55.034Z"))));
    Assert.assertEquals("Row 2 col 1 is 2", 2, actual.get(1).getInt(0));
    Assert.assertEquals("Row 2 col 2 is b", "b", actual.get(1).getString(1));
    Assert.assertEquals("Row 2 col 3,1 is 200", 200, actual.get(1).getStruct(2).getInt(0));
    Assert.assertEquals("Row 2 col 3,2 is p1", "p1", actual.get(1).getStruct(2).getString(1));
    Assert.assertEquals("Row 2 col 4 is 2017-12-02 12:12:55.034",
        0, actual.get(1).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-02T10:12:55.034Z"))));
    Assert.assertEquals("Row 3 col 1 is 3", 3, actual.get(2).getInt(0));
    Assert.assertEquals("Row 3 col 2 is c", "c", actual.get(2).getString(1));
    Assert.assertEquals("Row 3 col 3,1 is 300", 300, actual.get(2).getStruct(2).getInt(0));
    Assert.assertEquals("Row 3 col 3,2 is p2", "p2", actual.get(2).getStruct(2).getString(1));
    Assert.assertEquals("Row 3 col 4 is 2017-12-03 12:12:55.034",
        0, actual.get(2).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-03T10:12:55.034Z"))));
    Assert.assertEquals("Row 4 col 1 is 4", 4, actual.get(3).getInt(0));
    Assert.assertEquals("Row 4 col 2 is d", "d", actual.get(3).getString(1));
    Assert.assertEquals("Row 4 col 3,1 is 400", 400, actual.get(3).getStruct(2).getInt(0));
    Assert.assertEquals("Row 4 col 3,2 is p2", "p2", actual.get(3).getStruct(2).getString(1));
    Assert.assertEquals("Row 4 col 4 is 2017-12-04 12:12:55.034",
        0, actual.get(3).getTimestamp(3).compareTo(Timestamp.from(Instant.parse("2017-12-04T10:12:55.034Z"))));
  }
}
