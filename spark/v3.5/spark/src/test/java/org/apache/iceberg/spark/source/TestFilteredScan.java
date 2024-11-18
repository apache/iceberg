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

import static org.apache.iceberg.Files.localOutput;
import static org.apache.iceberg.PlanningMode.DISTRIBUTED;
import static org.apache.iceberg.PlanningMode.LOCAL;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PlanningMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkReadOptions;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.data.GenericsHelpers;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.connector.expressions.filter.Predicate;
import org.apache.spark.sql.connector.read.Batch;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.SupportsPushDownV2Filters;
import org.apache.spark.sql.sources.And;
import org.apache.spark.sql.sources.EqualTo;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.sources.GreaterThan;
import org.apache.spark.sql.sources.LessThan;
import org.apache.spark.sql.sources.Not;
import org.apache.spark.sql.sources.StringStartsWith;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestFilteredScan {
  private static final Configuration CONF = new Configuration();
  private static final HadoopTables TABLES = new HadoopTables(CONF);

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "ts", Types.TimestampType.withZone()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));

  private static final PartitionSpec BUCKET_BY_ID =
      PartitionSpec.builderFor(SCHEMA).bucket("id", 4).build();

  private static final PartitionSpec PARTITION_BY_DAY =
      PartitionSpec.builderFor(SCHEMA).day("ts").build();

  private static final PartitionSpec PARTITION_BY_HOUR =
      PartitionSpec.builderFor(SCHEMA).hour("ts").build();

  private static final PartitionSpec PARTITION_BY_DATA =
      PartitionSpec.builderFor(SCHEMA).identity("data").build();

  private static final PartitionSpec PARTITION_BY_ID =
      PartitionSpec.builderFor(SCHEMA).identity("id").build();

  private static SparkSession spark = null;

  @BeforeAll
  public static void startSpark() {
    TestFilteredScan.spark = SparkSession.builder().master("local[2]").getOrCreate();
  }

  @AfterAll
  public static void stopSpark() {
    SparkSession currentSpark = TestFilteredScan.spark;
    TestFilteredScan.spark = null;
    currentSpark.stop();
  }

  @TempDir private Path temp;

  @Parameter(index = 0)
  private String format;

  @Parameter(index = 1)
  private boolean vectorized;

  @Parameter(index = 2)
  private PlanningMode planningMode;

  @Parameters(name = "format = {0}, vectorized = {1}, planningMode = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {"parquet", false, LOCAL},
      {"parquet", true, DISTRIBUTED},
      {"avro", false, LOCAL},
      {"orc", false, DISTRIBUTED},
      {"orc", true, LOCAL}
    };
  }

  private File parent = null;
  private File unpartitioned = null;
  private List<Record> records = null;

  @BeforeEach
  public void writeUnpartitionedTable() throws IOException {
    this.parent = temp.resolve("TestFilteredScan").toFile();
    this.unpartitioned = new File(parent, "unpartitioned");
    File dataFolder = new File(unpartitioned, "data");
    assertThat(dataFolder.mkdirs()).as("Mkdir should succeed").isTrue();

    Table table =
        TABLES.create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of(
                TableProperties.DATA_PLANNING_MODE,
                planningMode.modeName(),
                TableProperties.DELETE_PLANNING_MODE,
                planningMode.modeName()),
            unpartitioned.toString());
    Schema tableSchema = table.schema(); // use the table schema because ids are reassigned

    FileFormat fileFormat = FileFormat.fromString(format);

    File testFile = new File(dataFolder, fileFormat.addExtension(UUID.randomUUID().toString()));

    this.records = testRecords(tableSchema);

    try (FileAppender<Record> writer =
        new GenericAppenderFactory(tableSchema).newAppender(localOutput(testFile), fileFormat)) {
      writer.addAll(records);
    }

    DataFile file =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(records.size())
            .withFileSizeInBytes(testFile.length())
            .withPath(testFile.toString())
            .build();

    table.newAppend().appendFile(file).commit();
  }

  @TestTemplate
  public void testUnpartitionedIDFilters() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", unpartitioned.toString()));
    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    for (int i = 0; i < 10; i += 1) {
      pushFilters(builder, EqualTo.apply("id", i));
      Batch scan = builder.build().toBatch();

      InputPartition[] partitions = scan.planInputPartitions();
      assertThat(partitions).as("Should only create one task for a small file").hasSize(1);

      // validate row filtering
      assertEqualsSafe(
          SCHEMA.asStruct(), expected(i), read(unpartitioned.toString(), vectorized, "id = " + i));
    }
  }

  @TestTemplate
  public void testUnpartitionedCaseInsensitiveIDFilters() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", unpartitioned.toString()));

    // set spark.sql.caseSensitive to false
    String caseSensitivityBeforeTest = TestFilteredScan.spark.conf().get("spark.sql.caseSensitive");
    TestFilteredScan.spark.conf().set("spark.sql.caseSensitive", "false");

    try {

      for (int i = 0; i < 10; i += 1) {
        SparkScanBuilder builder =
            new SparkScanBuilder(spark, TABLES.load(options.get("path")), options)
                .caseSensitive(false);

        pushFilters(
            builder,
            EqualTo.apply("ID", i)); // note lower(ID) == lower(id), so there must be a match
        Batch scan = builder.build().toBatch();

        InputPartition[] tasks = scan.planInputPartitions();
        assertThat(tasks).as("Should only create one task for a small file").hasSize(1);

        // validate row filtering
        assertEqualsSafe(
            SCHEMA.asStruct(),
            expected(i),
            read(unpartitioned.toString(), vectorized, "id = " + i));
      }
    } finally {
      // return global conf to previous state
      TestFilteredScan.spark.conf().set("spark.sql.caseSensitive", caseSensitivityBeforeTest);
    }
  }

  @TestTemplate
  public void testUnpartitionedTimestampFilter() {
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", unpartitioned.toString()));

    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
    Batch scan = builder.build().toBatch();

    InputPartition[] tasks = scan.planInputPartitions();
    assertThat(tasks).as("Should only create one task for a small file").hasSize(1);

    assertEqualsSafe(
        SCHEMA.asStruct(),
        expected(5, 6, 7, 8, 9),
        read(
            unpartitioned.toString(),
            vectorized,
            "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
  }

  @TestTemplate
  public void testBucketPartitionedIDFilters() {
    Table table = buildPartitionedTable("bucketed_by_id", BUCKET_BY_ID);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    Batch unfiltered =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();
    assertThat(unfiltered.planInputPartitions())
        .as("Unfiltered table should created 4 read tasks")
        .hasSize(4);

    for (int i = 0; i < 10; i += 1) {
      SparkScanBuilder builder =
          new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, EqualTo.apply("id", i));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();

      // validate predicate push-down
      assertThat(tasks).as("Should only create one task for a single bucket").hasSize(1);

      // validate row filtering
      assertEqualsSafe(
          SCHEMA.asStruct(), expected(i), read(table.location(), vectorized, "id = " + i));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @TestTemplate
  public void testDayPartitionedTimestampFilters() {
    Table table = buildPartitionedTable("partitioned_by_day", PARTITION_BY_DAY);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));
    Batch unfiltered =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();

    assertThat(unfiltered.planInputPartitions())
        .as("Unfiltered table should created 2 read tasks")
        .hasSize(2);

    {
      SparkScanBuilder builder =
          new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      assertThat(tasks).as("Should create one task for 2017-12-21").hasSize(1);

      assertEqualsSafe(
          SCHEMA.asStruct(),
          expected(5, 6, 7, 8, 9),
          read(
              table.location(), vectorized, "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SparkScanBuilder builder =
          new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(
          builder,
          And.apply(
              GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
              LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      assertThat(tasks).as("Should create one task for 2017-12-22").hasSize(1);

      assertEqualsSafe(
          SCHEMA.asStruct(),
          expected(1, 2),
          read(
              table.location(),
              vectorized,
              "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and "
                  + "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @TestTemplate
  public void testHourPartitionedTimestampFilters() {
    Table table = buildPartitionedTable("partitioned_by_hour", PARTITION_BY_HOUR);

    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));
    Batch unfiltered =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options).build().toBatch();

    assertThat(unfiltered.planInputPartitions())
        .as("Unfiltered table should created 9 read tasks")
        .hasSize(9);

    {
      SparkScanBuilder builder =
          new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(builder, LessThan.apply("ts", "2017-12-22T00:00:00+00:00"));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      assertThat(tasks).as("Should create 4 tasks for 2017-12-21: 15, 17, 21, 22").hasSize(4);

      assertEqualsSafe(
          SCHEMA.asStruct(),
          expected(8, 9, 7, 6, 5),
          read(
              table.location(), vectorized, "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)"));
    }

    {
      SparkScanBuilder builder =
          new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

      pushFilters(
          builder,
          And.apply(
              GreaterThan.apply("ts", "2017-12-22T06:00:00+00:00"),
              LessThan.apply("ts", "2017-12-22T08:00:00+00:00")));
      Batch scan = builder.build().toBatch();

      InputPartition[] tasks = scan.planInputPartitions();
      assertThat(tasks).as("Should create 2 tasks for 2017-12-22: 6, 7").hasSize(2);

      assertEqualsSafe(
          SCHEMA.asStruct(),
          expected(2, 1),
          read(
              table.location(),
              vectorized,
              "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and "
                  + "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)"));
    }
  }

  @SuppressWarnings("checkstyle:AvoidNestedBlocks")
  @TestTemplate
  public void testFilterByNonProjectedColumn() {
    {
      Schema actualProjection = SCHEMA.select("id", "data");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(5, 6, 7, 8, 9)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(
          actualProjection.asStruct(),
          expected,
          read(
              unpartitioned.toString(),
              vectorized,
              "ts < cast('2017-12-22 00:00:00+00:00' as timestamp)",
              "id",
              "data"));
    }

    {
      // only project id: ts will be projected because of the filter, but data will not be included

      Schema actualProjection = SCHEMA.select("id");
      List<Record> expected = Lists.newArrayList();
      for (Record rec : expected(1, 2)) {
        expected.add(projectFlat(actualProjection, rec));
      }

      assertEqualsSafe(
          actualProjection.asStruct(),
          expected,
          read(
              unpartitioned.toString(),
              vectorized,
              "ts > cast('2017-12-22 06:00:00+00:00' as timestamp) and "
                  + "ts < cast('2017-12-22 08:00:00+00:00' as timestamp)",
              "id"));
    }
  }

  @TestTemplate
  public void testPartitionedByDataStartsWithFilter() {
    Table table = buildPartitionedTable("partitioned_by_data", PARTITION_BY_DATA);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new StringStartsWith("data", "junc"));
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions()).hasSize(1);
  }

  @TestTemplate
  public void testPartitionedByDataNotStartsWithFilter() {
    Table table = buildPartitionedTable("partitioned_by_data", PARTITION_BY_DATA);
    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new Not(new StringStartsWith("data", "junc")));
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions()).hasSize(9);
  }

  @TestTemplate
  public void testPartitionedByIdStartsWith() {
    Table table = buildPartitionedTable("partitioned_by_id", PARTITION_BY_ID);

    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new StringStartsWith("data", "junc"));
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions()).hasSize(1);
  }

  @TestTemplate
  public void testPartitionedByIdNotStartsWith() {
    Table table = buildPartitionedTable("partitioned_by_id", PARTITION_BY_ID);

    CaseInsensitiveStringMap options =
        new CaseInsensitiveStringMap(ImmutableMap.of("path", table.location()));

    SparkScanBuilder builder =
        new SparkScanBuilder(spark, TABLES.load(options.get("path")), options);

    pushFilters(builder, new Not(new StringStartsWith("data", "junc")));
    Batch scan = builder.build().toBatch();

    assertThat(scan.planInputPartitions()).hasSize(9);
  }

  @TestTemplate
  public void testUnpartitionedStartsWith() {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(unpartitioned.toString());

    List<String> matchedData =
        df.select("data").where("data LIKE 'jun%'").as(Encoders.STRING()).collectAsList();

    assertThat(matchedData).hasSize(1);
    assertThat(matchedData.get(0)).isEqualTo("junction");
  }

  @TestTemplate
  public void testUnpartitionedNotStartsWith() {
    Dataset<Row> df =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(unpartitioned.toString());

    List<String> matchedData =
        df.select("data").where("data NOT LIKE 'jun%'").as(Encoders.STRING()).collectAsList();

    List<String> expected =
        testRecords(SCHEMA).stream()
            .map(r -> r.getField("data").toString())
            .filter(d -> !d.startsWith("jun"))
            .collect(Collectors.toList());

    assertThat(matchedData).hasSize(9);
    assertThat(Sets.newHashSet(matchedData)).isEqualTo(Sets.newHashSet(expected));
  }

  private static Record projectFlat(Schema projection, Record record) {
    Record result = GenericRecord.create(projection);
    List<Types.NestedField> fields = projection.asStruct().fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Types.NestedField field = fields.get(i);
      result.set(i, record.getField(field.name()));
    }
    return result;
  }

  public static void assertEqualsUnsafe(
      Types.StructType struct, List<Record> expected, List<UnsafeRow> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      GenericsHelpers.assertEqualsUnsafe(struct, expected.get(i), actual.get(i));
    }
    assertThat(actual).as("Number of results should match expected").hasSameSizeAs(expected);
  }

  public static void assertEqualsSafe(
      Types.StructType struct, List<Record> expected, List<Row> actual) {
    // TODO: match records by ID
    int numRecords = Math.min(expected.size(), actual.size());
    for (int i = 0; i < numRecords; i += 1) {
      GenericsHelpers.assertEqualsSafe(struct, expected.get(i), actual.get(i));
    }
    assertThat(actual).as("Number of results should match expected").hasSameSizeAs(expected);
  }

  private List<Record> expected(int... ordinals) {
    List<Record> expected = Lists.newArrayListWithExpectedSize(ordinals.length);
    for (int ord : ordinals) {
      expected.add(records.get(ord));
    }
    return expected;
  }

  private void pushFilters(ScanBuilder scan, Filter... filters) {
    assertThat(scan).isInstanceOf(SupportsPushDownV2Filters.class);
    SupportsPushDownV2Filters filterable = (SupportsPushDownV2Filters) scan;
    filterable.pushPredicates(Arrays.stream(filters).map(Filter::toV2).toArray(Predicate[]::new));
  }

  private Table buildPartitionedTable(String desc, PartitionSpec spec) {
    File location = new File(parent, desc);
    Table table = TABLES.create(SCHEMA, spec, location.toString());

    // Do not combine or split files because the tests expect a split per partition.
    // A target split size of 2048 helps us achieve that.
    table.updateProperties().set("read.split.target-size", "2048").commit();

    // copy the unpartitioned table into the partitioned table to produce the partitioned data
    Dataset<Row> allRows =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(unpartitioned.toString());

    // disable fanout writers to locally order records for future verifications
    allRows
        .write()
        .option(SparkWriteOptions.FANOUT_ENABLED, "false")
        .format("iceberg")
        .mode("append")
        .save(table.location());

    table.refresh();

    return table;
  }

  private List<Record> testRecords(Schema schema) {
    return Lists.newArrayList(
        record(schema, 0L, parse("2017-12-22T09:20:44.294658+00:00"), "junction"),
        record(schema, 1L, parse("2017-12-22T07:15:34.582910+00:00"), "alligator"),
        record(schema, 2L, parse("2017-12-22T06:02:09.243857+00:00"), ""),
        record(schema, 3L, parse("2017-12-22T03:10:11.134509+00:00"), "clapping"),
        record(schema, 4L, parse("2017-12-22T00:34:00.184671+00:00"), "brush"),
        record(schema, 5L, parse("2017-12-21T22:20:08.935889+00:00"), "trap"),
        record(schema, 6L, parse("2017-12-21T21:55:30.589712+00:00"), "element"),
        record(schema, 7L, parse("2017-12-21T17:31:14.532797+00:00"), "limited"),
        record(schema, 8L, parse("2017-12-21T15:21:51.237521+00:00"), "global"),
        record(schema, 9L, parse("2017-12-21T15:02:15.230570+00:00"), "goldfish"));
  }

  private static List<Row> read(String table, boolean vectorized, String expr) {
    return read(table, vectorized, expr, "*");
  }

  private static List<Row> read(
      String table, boolean vectorized, String expr, String select0, String... selectN) {
    Dataset<Row> dataset =
        spark
            .read()
            .format("iceberg")
            .option(SparkReadOptions.VECTORIZATION_ENABLED, String.valueOf(vectorized))
            .load(table)
            .filter(expr)
            .select(select0, selectN);
    return dataset.collectAsList();
  }

  private static OffsetDateTime parse(String timestamp) {
    return OffsetDateTime.parse(timestamp);
  }

  private static Record record(Schema schema, Object... values) {
    Record rec = GenericRecord.create(schema);
    for (int i = 0; i < values.length; i += 1) {
      rec.set(i, values[i]);
    }
    return rec;
  }
}
