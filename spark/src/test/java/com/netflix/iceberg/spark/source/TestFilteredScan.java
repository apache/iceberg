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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.netflix.iceberg.DataFile;
import com.netflix.iceberg.DataFiles;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.Table;
import com.netflix.iceberg.avro.Avro;
import com.netflix.iceberg.avro.AvroSchemaUtil;
import com.netflix.iceberg.expressions.Expressions;
import com.netflix.iceberg.expressions.Literal;
import com.netflix.iceberg.hadoop.HadoopTables;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.spark.SparkExpressions;
import com.netflix.iceberg.spark.data.TestHelpers;
import com.netflix.iceberg.transforms.Transform;
import com.netflix.iceberg.transforms.Transforms;
import com.netflix.iceberg.types.Types;
import org.apache.avro.generic.GenericData.Record;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.reader.DataReaderFactory;
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownCatalystFilters;
import org.apache.spark.sql.sources.v2.reader.SupportsScanUnsafeRow;
import org.apache.spark.sql.types.DateType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static com.netflix.iceberg.Files.localOutput;
import static org.apache.spark.sql.catalyst.util.DateTimeUtils.fromJavaTimestamp;
import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.column;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_date;

@RunWith(Parameterized.class)
public class TestFilteredScan {
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

  private static final PartitionSpec PARTITION_BY_FIRST_LETTER = PartitionSpec.builderFor(SCHEMA)
      .truncate("data", 1)
      .build();

  private static SparkSession spark = null;

  @BeforeClass
  public static void startSpark() {
    TestFilteredScan.spark = SparkSession.builder().master("local[2]").getOrCreate();

    // define UDFs used by partition tests
    Transform<Long, Integer> bucket4 = Transforms.bucket(Types.LongType.get(), 4);
    spark.udf().register("bucket4", (UDF1<Long, Integer>) bucket4::apply, IntegerType$.MODULE$);

    Transform<Long, Integer> day = Transforms.day(Types.TimestampType.withZone());
    spark.udf().register("ts_day",
        (UDF1<Timestamp, Integer>) timestamp -> day.apply(fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);

    Transform<Long, Integer> hour = Transforms.hour(Types.TimestampType.withZone());
    spark.udf().register("ts_hour",
        (UDF1<Timestamp, Integer>) timestamp -> hour.apply(fromJavaTimestamp(timestamp)),
        IntegerType$.MODULE$);

    Transform<CharSequence, CharSequence> trunc1 = Transforms.truncate(Types.StringType.get(), 1);
    spark.udf().register("trunc1",
        (UDF1<CharSequence, CharSequence>) str -> trunc1.apply(str.toString()),
        StringType$.MODULE$);
  }

  @AfterClass
  public static void stopSpark() {
    SparkSession spark = TestFilteredScan.spark;
    TestFilteredScan.spark = null;
    spark.stop();
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

  public TestFilteredScan(String format) {
    this.format = format;
  }

  private File parent = null;
  private File unpartitioned = null;
  private List<Record> records = null;

  @Before
  public void writeUnpartitionedTable() throws IOException {
    this.parent = temp.newFolder("TestFilteredScan");
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
        try (FileAppender<Record> writer = Avro.write(localOutput(testFile))
            .schema(tableSchema)
            .build()) {
          writer.addAll(records);
        }
        break;

      case PARQUET:
        try (FileAppender<Record> writer = Parquet.write(localOutput(testFile))
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
        "path", unpartitioned.toString())
    );

    IcebergSource source = new IcebergSource();

    for (int i = 0; i < 10; i += 1) {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.equal("id", i));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should only create one task for a small file", 1, tasks.size());

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i),
          read(unpartitioned.toString(), "id = " + i));
    }
  }

  @Test
  public void testUnpartitionedTimestampFilter() {
    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", unpartitioned.toString())
    );

    IcebergSource source = new IcebergSource();

    DataSourceReader reader = source.createReader(options);

    pushFilters(reader, Expressions.lessThan("ts", "2017-12-22T00:00:00+00:00"));

    List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
    Assert.assertEquals("Should only create one task for a small file", 1, tasks.size());

    assertEqualsSafe(SCHEMA.asStruct(), expected(5,6,7,8,9),
        read(unpartitioned.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
  }

  @Test
  public void testBucketPartitionedIDFilters() {
    File location = buildPartitionedTable("bucketed_by_id", BUCKET_BY_ID, "bucket4", "id");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString())
    );

    IcebergSource source = new IcebergSource();
    DataSourceReader unfiltered = source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 4 read tasks",
        4, planTasks(unfiltered).size());

    for (int i = 0; i < 10; i += 1) {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.equal("id", i));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);

      // validate predicate push-down
      Assert.assertEquals("Should create one task for a single bucket", 1, tasks.size());

      // validate row filtering
      assertEqualsSafe(SCHEMA.asStruct(), expected(i), read(location.toString(), "id = " + i));
    }
  }

  @Test
  public void testDayPartitionedTimestampFilters() {
    File location = buildPartitionedTable("partitioned_by_day", PARTITION_BY_DAY, "ts_day", "ts");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString())
    );

    int day = Literal.of("2017-12-21").<Integer>to(Types.DateType.get()).value();
    IcebergSource source = new IcebergSource();
    DataSourceReader unfiltered = source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 2 read tasks",
        2, planTasks(unfiltered).size());

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.lessThan("ts", "2017-12-22T00:00:00+00:00"));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create one task for 2017-12-21", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
          read(location.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, col("ts").cast(DateType$.MODULE$).$eq$eq$eq(lit(day)).expr());

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create one task for 2017-12-21", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
          read(location.toString(), "cast(ts as date) = date '2017-12-21'"));
    }

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, to_date(col("ts")).$eq$eq$eq(lit(day)).expr());

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create one task for 2017-12-21", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(5, 6, 7, 8, 9),
          read(location.toString(), "to_date(ts) = date '2017-12-21'"));
    }

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.and(
          Expressions.greaterThan("ts", "2017-12-22T06:00:00+00:00"),
          Expressions.lessThan("ts", "2017-12-22T08:00:00+00:00")));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create one task for 2017-12-22", 1, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(1, 2), read(location.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @Test
  public void testHourPartitionedTimestampFilters() {
    File location = buildPartitionedTable("partitioned_by_hour", PARTITION_BY_HOUR, "ts_hour", "ts");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString())
    );

    IcebergSource source = new IcebergSource();
    DataSourceReader unfiltered = source.createReader(options);
    Assert.assertEquals("Unfiltered table should created 9 read tasks",
        9, planTasks(unfiltered).size());

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.lessThan("ts", "2017-12-22T00:00:00+00:00"));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create 4 tasks for 2017-12-21: 15, 17, 21, 22", 4, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(8, 9, 7, 6, 5),
          read(location.toString(), "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.and(
          Expressions.greaterThan("ts", "2017-12-22T06:00:00+00:00"),
          Expressions.lessThan("ts", "2017-12-22T08:00:00+00:00")));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create 2 tasks for 2017-12-22: 6, 7", 2, tasks.size());

      assertEqualsSafe(SCHEMA.asStruct(), expected(2, 1), read(location.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @Test
  public void testTrunctateDataPartitionedFilters() {
    File location = buildPartitionedTable("trunc", PARTITION_BY_FIRST_LETTER, "trunc1", "data");

    DataSourceOptions options = new DataSourceOptions(ImmutableMap.of(
        "path", location.toString())
    );

    IcebergSource source = new IcebergSource();
    DataSourceReader unfiltered = source.createReader(options);
    Assert.assertEquals("Unfiltered table should have created 9 read tasks",
        9, planTasks(unfiltered).size());

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, Expressions.equal("data", "goldfish"));

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create 1 task for 'goldfish' (g)", 1, tasks.size());
    }

    {
      DataSourceReader reader = source.createReader(options);

      pushFilters(reader, col("data").$eq$eq$eq("goldfish").expr());

      List<DataReaderFactory<UnsafeRow>> tasks = planTasks(reader);
      Assert.assertEquals("Should create 1 task for 'goldfish' (g)", 1, tasks.size());
    }

    assertEqualsSafe(SCHEMA.asStruct(), expected(9),
        read(location.toString(), "data = 'goldfish'"));
  }

  @Test
  public void testFilterByNonProjectedColumn() {
    {
      Schema actualProjection = SCHEMA.select("id", "data");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(5, 6 ,7, 8, 9)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
              unpartitioned.toString(),
              "cast('2017-12-22 00:00:00+00:00' as timestamp) > ts",
              "id", "data"));
    }

    {
      // only project id: ts will be projected because of the filter, but data will not be included

      Schema actualProjection = SCHEMA.select("id");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(1, 2)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(actualProjection.asStruct(), expected, read(
          unpartitioned.toString(),
          "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and " +
              "cast('2017-12-22 08:00:00+00:00' as timestamp) > ts",
          "id"));
    }
  }

  private static Record projectFlat(Schema projection, Record record) {
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(projection, "test");
    Record result = new Record(avroSchema);
    List<Types.NestedField> fields = projection.asStruct().fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      result.put(i, record.get(field.name()));
    }
    return result;
  }

  public static void assertEqualsSafe(Types.StructType struct,
                                      List<Record> expected, List<Row> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      TestHelpers.assertEqualsSafe(struct, expected.get(i), actual.get(i));
    }
    Assert.assertEquals("Number of results should match expected", expected.size(), actual.size());
  }

  private List<Record> expected(int... ordinals) {
    List<Record> expected = Lists.newArrayListWithExpectedSize(ordinals.length);
    for (int ord : ordinals) {
      expected.add(records.get(ord));
    }
    return expected;
  }

  private void pushFilters(DataSourceReader reader,
                           com.netflix.iceberg.expressions.Expression... filters) {
    Expression[] expressions = new Expression[filters.length];
    for (int i = 0; i < filters.length; i += 1) {
      expressions[i] = SparkExpressions.convert(filters[i], SCHEMA);
    }
    pushFilters(reader, expressions);
  }

  private void pushFilters(DataSourceReader reader,
                           Expression... expressions) {
    Assert.assertTrue(reader instanceof SupportsPushDownCatalystFilters);
    SupportsPushDownCatalystFilters filterable = (SupportsPushDownCatalystFilters) reader;
    filterable.pushCatalystFilters(expressions);
  }

  private List<DataReaderFactory<UnsafeRow>> planTasks(DataSourceReader reader) {
    Assert.assertTrue(reader instanceof SupportsScanUnsafeRow);
    SupportsScanUnsafeRow unsafeReader = (SupportsScanUnsafeRow) reader;
    return unsafeReader.createUnsafeRowReaderFactories();
  }

  private File buildPartitionedTable(String desc, PartitionSpec spec, String udf, String partitionColumn) {
    File location = new File(parent, desc);
    Table byId = TABLES.create(SCHEMA, spec, location.toString());

    // do not combine splits because the tests expect a split per partition
    byId.updateProperties().set("read.split.target-size", "1").commit();

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

  private List<Record> testRecords(org.apache.avro.Schema avroSchema) {
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

  private static Record record(org.apache.avro.Schema schema, Object... values) {
    Record rec = new Record(schema);
    for (int i = 0; i < values.length; i += 1) {
      rec.put(i, values[i]);
    }
    return rec;
  }
}
