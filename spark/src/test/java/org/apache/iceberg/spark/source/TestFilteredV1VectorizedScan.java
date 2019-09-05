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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.expressions.Literal;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.spark.data.TestHelpers;
import org.apache.iceberg.transforms.Transform;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.InputPartition;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsScanColumnarBatch;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.Files.localOutput;
import static org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.column;

@RunWith(Parameterized.class)
public class TestFilteredV1VectorizedScan {

  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.LongType.get()),
      Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
      Types.NestedField.optional(3, "data", Types.StringType.get())
  );

  private static final PartitionSpec BUCKET_BY_ID = PartitionSpec.builderFor(SCHEMA)
      .bucket("id", 4)
      .build();

  private static final PartitionSpec PARTITION_BY_DAY = PartitionSpec.builderFor(SCHEMA)
      .day("ts")
      .build();

  private static final PartitionSpec PARTITION_BY_HOUR = PartitionSpec.builderFor(SCHEMA)
      .hour("ts")
      .build();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestFilteredV1VectorizedScan.spark = SparkSession.builder().master("local[2]").getOrCreate();

    // define UDFs used by partition tests
    Transform<Long, Integer> bucket4 = Transforms.bucket(Types.LongType.get(), 4);
    spark.udf().register("bucket4", (UDF1<Long, Integer>) bucket4::apply, IntegerType$.MODULE$);

    Transform<Long, Integer> day = Transforms.day(Types.TimestampType.withZone());
    spark.udf().register("ts_day",
        (UDF1<Timestamp, Integer>) timestamp -> day.apply((Long) fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);

    Transform<Long, Integer> hour = Transforms.hour(Types.TimestampType.withZone());
    spark.udf().register("ts_hour",
        (UDF1<Timestamp, Integer>) timestamp -> hour.apply((Long) fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession currentSpark = TestFilteredV1VectorizedScan.spark;
    TestFilteredV1VectorizedScan.spark = null;
    currentSpark.stop();
  }

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private final String format;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] {
        new Object[] { "parquet" },
        new Object[] { "avro" }
    };
  }

  public TestFilteredV1VectorizedScan(String format) {
    this.format = format;
  }

  private File parent = null;
  private File unpartitioned = null;
  private List<GenericData.Record> records = null;

  @Before
  public void writeUnpartitionedTable() throws IOException {
    this.parent = temp.newFolder("TestFilteredV1VectorizedScan");
    this.unpartitioned = new File(parent, "unpartitioned");
    File dataFolder = new File(unpartitioned, "data");
    Assert.assertTrue("Mkdir should succeed", dataFolder.mkdirs());

    Table table = TABLES.create(SCHEMA, PartitionSpec.unpartitioned(), unpartitioned.toString());
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    FileFormat fileFormat = FileFormat.valueOf(format.toUpperCase(Locale.ENGLISH));

    File testFile = new File(dataFolder, fileFormat.addExtension(UUID.randomUUID().toString()));

    // create records using the table's schema
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(tableSchema, "test");
    this.records = testRecords(avroSchema);

    switch (fileFormat) {
      case AVRO:
        try (FileAppender<GenericData.Record> writer = Avro.write(localOutput(testFile))
            .schema(tableSchema)
            .build()) {
          writer.addAll(records);
        }
        break;

      case PARQUET:
        try (FileAppender<GenericData.Record> writer = Parquet.write(localOutput(testFile))
            .schema(tableSchema)
            .build()) {
          writer.addAll(records);
        }
        break;
    }

    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withRecordCount(records.size())
        .withFileSizeInBytes(testFile.length())
        .withPath(testFile.toString())
        .build();

    table.newAppend().appendFile(file).commit();
  }

  @Test
  public void testUnpartitionedIDFilters() {
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", unpartitioned.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    IcebergSource source = new IcebergSource();

    for (int i = 0; i < 10; i += 1) {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, EqualTo.apply("id", i));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
      Assert.assertEquals("Should only create one task for a small file", 1, tasks.size());

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i),
          read(unpartitioned.toString(), "id = " + i));
    }
  }

  @Test
  public void testUnpartitionedCaseInsensitiveIDFilters() {
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", unpartitioned.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    // set spark.sql.caseSensitive to false
    String caseSensitivityBeforeTest = TestFilteredV1VectorizedScan.spark.conf().get("spark.sql.caseSensitive");
    TestFilteredV1VectorizedScan.spark.conf().set("spark.sql.caseSensitive", "false");

    try {
      IcebergSource source = new IcebergSource();

      for (int i = 0; i < 10; i += 1) {
        SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

        pushFilters(reader, EqualTo.apply("ID", i)); // note lower(ID) == lower(id), so there must be a match

        List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
        Assert.assertEquals("Should only create one task for a small file", 1, tasks.size());

        // validate row filtering
        assertEqualsSafe(SCHEMA.asStruct(), expected(i),
            read(unpartitioned.toString(), "id = " + i));
      }
    } finally {
      // return global conf to previous state
      TestFilteredV1VectorizedScan.spark.conf().set("spark.sql.caseSensitive", caseSensitivityBeforeTest);
    }
  }

  @Test
  public void testUnpartitionedTimestampFilter() {
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", unpartitioned.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    IcebergSource source = new IcebergSource();

    SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

    pushFilters(reader, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));

    List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
    Assert.assertEquals("Should only create one task for a small file", 1, tasks.size());

    assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
        read(unpartitioned.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
  }

  @Test
  public void testBucketPartitionedIDFilters() {
    File location = buildPartitionedTable("bucketed_by_id", BUCKET_BY_ID, "bucket4", "id");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    IcebergSource source = new IcebergSource();
    SupportsScanColumnarBatch unfiltered = (SupportsScanColumnarBatch) source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 4 read tasks",
        4, unfiltered.planBatchInputPartitions().size());

    for (int i = 0; i < 10; i += 1) {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, EqualTo.apply("id", i));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();

      // validate predicate push-down
      Assert.assertEquals("Should create one task for a single bucket", 1, tasks.size());

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i), read(location.toString(), "id = " + i));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testDayPartitionedTimestampFilters() {
    File location = buildPartitionedTable("partitioned_by_day", PARTITION_BY_DAY, "ts_day", "ts");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    IcebergSource source = new IcebergSource();
    SupportsScanColumnarBatch unfiltered = (SupportsScanColumnarBatch) source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 2 read tasks",
        2, unfiltered.planBatchInputPartitions().size());

    {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
      Assert.assertEquals("Should create one task for 2017-12-21", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
          read(location.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, And.apply(
          GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
          LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
      Assert.assertEquals("Should create one task for 2017-12-22", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(1, 2), read(location.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testHourPartitionedTimestampFilters() {
    File location = buildPartitionedTable("partitioned_by_hour", PARTITION_BY_HOUR, "ts_hour", "ts");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString(),
        IcebergSource.ICEBERG_READ_ENABLE_V1_VECTORIZATION_CONF, "true")
    );

    IcebergSource source = new IcebergSource();
    SupportsScanColumnarBatch unfiltered = (SupportsScanColumnarBatch) source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 9 read tasks",
        9, unfiltered.planBatchInputPartitions().size());

    {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
      Assert.assertEquals("Should create 4 tasks for 2017-12-21: 15, 17, 21, 22", 4, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(8, 9, 7, 6, 5),
          read(location.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SupportsScanColumnarBatch reader = (SupportsScanColumnarBatch) source.createReader(options);

      pushFilters(reader, And.apply(
          GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
          LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));

      List<InputPartition<ColumnarBatch>> tasks = reader.planBatchInputPartitions();
      Assert.assertEquals("Should create 2 tasks for 2017-12-22: 6, 7", 2, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(2, 1), read(location.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @Test
  public void testFilterByNonProjectedColumn() {
    {
      Schema actualProjection = SCHEMA.select("id", "data");
      List<GenericData.Record> expected = Lists.newArrayList();
      for (GenericData.Record rec : expected(5, 6, 7, 8, 9)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
          unpartitioned.toString(),
          "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)",
          "id", "data"));
    }

    {
      // only project id: ts will be projected because of the filter, but data will not be included

      Schema actualProjection = SCHEMA.select("id");
      List<GenericData.Record> expected = Lists.newArrayList();
      for (GenericData.Record rec : expected(1, 2)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
          unpartitioned.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)",
          "id"));
    }
  }

  private static GenericData.Record projectFlat(Schema projection, GenericData.Record record) {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(projection, "test");
    GenericData.Record result = new GenericData.Record(avroSchema);
    List<Types.NestedField> fields = projection.asStruct().fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      result.put(i, record.get(field.name()));
    }
    return result;
  }

  public static void assertEqualsUnsafe(Types.StructType struct,
      List<GenericData.Record> expected, List<UnsafeRow> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      TestHelpers.assertEqualsUnsafe(struct, expected.get(i), actual.get(i));
    }
    Assert.assertEquals("Number of results should match expected", expected.size(), actual.size());
  }

  public static void assertEqualsSafe(Types.StructType struct,
      List<GenericData.Record> expected, List<Row> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      TestHelpers.assertEqualsSafe(struct, expected.get(i), actual.get(i));
    }
    Assert.assertEquals("Number of results should match expected", expected.size(), actual.size());
  }

  private List<GenericData.Record> expected(int... ordinals) {
    List<GenericData.Record> expected = Lists.newArrayListWithExpectedSize(ordinals.length);
    for (int ord : ordinals) {
      expected.add(records.get(ord));
    }
    return expected;
  }

  private void pushFilters(DataSourceReader reader, Filter... filters) {
    Assert.assertTrue(reader instanceof SupportsPushDownFilters);
    SupportsPushDownFilters filterable = (SupportsPushDownFilters) reader;
    filterable.pushFilters(filters);
  }

  private File buildPartitionedTable(String desc, PartitionSpec spec, String udf, String partitionColumn) {
    File location = new File(parent, desc);
    Table byId = TABLES.create(SCHEMA, spec, location.toString());

    // Do not combine or split files because the tests expect a split per partition.
    // A target split size of 2048 helps us achieve that.
    byId.updateProperties().set("read.split.target-size", "2048").commit();

    // copy the unpartitioned table into the partitioned table to produce the partitioned data
    Dataset<Row> allRows = spark.read()
        .format("iceberg")
        .load(unpartitioned.toString());

    allRows
        .coalesce(1) // ensure only 1 file per partition is written
        .withColumn("part", callUDF(udf, column(partitionColumn)))
        .sortWithinPartitions("part")
        .drop("part")
        .write()
        .format("iceberg")
        .mode("append")
        .save(byId.location());

    return location;
  }

  private List<GenericData.Record> testRecords(org.apache.avro.Schema avroSchema) {
    return Lists.newArrayList(
        record(avroSchema, 0L, timestamp("2017-12-22T09:20:44.294658+00:00"), "junction"),
        record(avroSchema, 1L, timestamp("2017-12-22T07:15:34.582910+00:00"), "alligator"),
        record(avroSchema, 2L, timestamp("2017-12-22T06:02:09.243857+00:00"), "forrest"),
        record(avroSchema, 3L, timestamp("2017-12-22T03:10:11.134509+00:00"), "clapping"),
        record(avroSchema, 4L, timestamp("2017-12-22T00:34:00.184671+00:00"), "brush"),
        record(avroSchema, 5L, timestamp("2017-12-21T22:20:08.935889+00:00"), "trap"),
        record(avroSchema, 6L, timestamp("2017-12-21T21:55:30.589712+00:00"), "element"),
        record(avroSchema, 7L, timestamp("2017-12-21T17:31:14.532797+00:00"), "limited"),
        record(avroSchema, 8L, timestamp("2017-12-21T15:21:51.237521+00:00"), "global"),
        record(avroSchema, 9L, timestamp("2017-12-21T15:02:15.230570+00:00"), "goldfish")
    );
  }

  private static List<Row> read(String table, String expr) {
    return read(table, expr, "*");
  }

  private static List<Row> read(String table, String expr, String select0, String... selectN) {
    Dataset<Row> dataset = spark.read().format("iceberg").load(table).filter(expr)
        .select(select0, selectN);
    return dataset.collectAsList();
  }

  private static long timestamp(String timestamp) {
    return Literal.of(timestamp).<Long>to(Types.TimestampType.withZone()).value();
  }

  private static GenericData.Record record(org.apache.avro.Schema schema, Object... values) {
    GenericData.Record rec = new GenericData.Record(schema);
    for (int i = 0; i < values.length; i += 1) {
      rec.put(i, values[i]);
    }
    return rec;
  }

}
