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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkValueConverter;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
public class TestSparkReaderWithBloomFilter {

  protected String tableName = null;
  protected Table table = null;
  protected List<Record> records = null;
  protected DataFile dataFile = null;

  private static TestHiveMetastore metastore = null;
  protected static SparkSession spark = null;
  protected static HiveCatalog catalog = null;

  @Parameter(index = 0)
  protected boolean vectorized;

  @Parameter(index = 1)
  protected boolean useBloomFilter;

  // Schema passed to create tables
  public static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(2, "id_long", Types.LongType.get()),
          Types.NestedField.required(3, "id_double", Types.DoubleType.get()),
          Types.NestedField.required(4, "id_float", Types.FloatType.get()),
          Types.NestedField.required(5, "id_string", Types.StringType.get()),
          Types.NestedField.optional(6, "id_boolean", Types.BooleanType.get()),
          Types.NestedField.optional(7, "id_date", Types.DateType.get()),
          Types.NestedField.optional(8, "id_int_decimal", Types.DecimalType.of(8, 2)),
          Types.NestedField.optional(9, "id_long_decimal", Types.DecimalType.of(14, 2)),
          Types.NestedField.optional(10, "id_fixed_decimal", Types.DecimalType.of(31, 2)));

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 329;
  private static final int INT_VALUE_COUNT = INT_MAX_VALUE - INT_MIN_VALUE + 1;
  private static final long LONG_BASE = 1000L;
  private static final double DOUBLE_BASE = 10000D;
  private static final float FLOAT_BASE = 100000F;
  private static final String BINARY_PREFIX = "BINARY测试_";

  @TempDir private Path temp;

  @BeforeEach
  public void writeTestDataFile() throws IOException {
    this.tableName = "test";
    createTable(tableName, SCHEMA);
    this.records = Lists.newArrayList();

    // records all use IDs that are in bucket id_bucket=0
    GenericRecord record = GenericRecord.create(table.schema());

    for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
      records.add(
          record.copy(
              ImmutableMap.of(
                  "id",
                  INT_MIN_VALUE + i,
                  "id_long",
                  LONG_BASE + INT_MIN_VALUE + i,
                  "id_double",
                  DOUBLE_BASE + INT_MIN_VALUE + i,
                  "id_float",
                  FLOAT_BASE + INT_MIN_VALUE + i,
                  "id_string",
                  BINARY_PREFIX + (INT_MIN_VALUE + i),
                  "id_boolean",
                  i % 2 == 0,
                  "id_date",
                  LocalDate.parse("2021-09-05"),
                  "id_int_decimal",
                  new BigDecimal(String.valueOf(77.77)),
                  "id_long_decimal",
                  new BigDecimal(String.valueOf(88.88)),
                  "id_fixed_decimal",
                  new BigDecimal(String.valueOf(99.99)))));
    }

    this.dataFile =
        writeDataFile(
            Files.localOutput(File.createTempFile("junit", null, temp.toFile())),
            Row.of(0),
            records);

    table.newAppend().appendFile(dataFile).commit();
  }

  @AfterEach
  public void cleanup() throws IOException {
    dropTable("test");
  }

  @Parameters(name = "vectorized = {0}, useBloomFilter = {1}")
  public static Object[][] parameters() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    metastore = new TestHiveMetastore();
    metastore.start();
    HiveConf hiveConf = metastore.hiveConf();

    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.hadoop." + METASTOREURIS.varname, hiveConf.get(METASTOREURIS.varname))
            .enableHiveSupport()
            .getOrCreate();

    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), "hive", ImmutableMap.of(), hiveConf);

    try {
      catalog.createNamespace(Namespace.of("default"));
    } catch (AlreadyExistsException ignored) {
      // the default namespace already exists. ignore the create error
    }
  }

  @AfterAll
  public static void stopMetastoreAndSpark() throws Exception {
    catalog = null;
    metastore.stop();
    metastore = null;
    spark.stop();
    spark = null;
  }

  protected void createTable(String name, Schema schema) {
    table = catalog.createTable(TableIdentifier.of("default", name), schema);
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    if (useBloomFilter) {
      table
          .updateProperties()
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_double", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_float", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_string", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_boolean", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_date", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_int_decimal", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long_decimal", "true")
          .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_fixed_decimal", "true")
          .commit();
    }

    table
        .updateProperties()
        .set(TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "100") // to have multiple row groups
        .commit();
    if (vectorized) {
      table
          .updateProperties()
          .set(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
          .set(TableProperties.PARQUET_BATCH_SIZE, "4")
          .commit();
    }
  }

  protected void dropTable(String name) {
    catalog.dropTable(TableIdentifier.of("default", name));
  }

  private DataFile writeDataFile(OutputFile out, StructLike partition, List<Record> rows)
      throws IOException {
    FileFormat format = defaultFormat(table.properties());
    GenericAppenderFactory factory = new GenericAppenderFactory(table.schema(), table.spec());

    boolean useBloomFilterCol1 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", Boolean.toString(useBloomFilterCol1));
    boolean useBloomFilterCol2 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long",
        Boolean.toString(useBloomFilterCol2));
    boolean useBloomFilterCol3 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_double", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_double",
        Boolean.toString(useBloomFilterCol3));
    boolean useBloomFilterCol4 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_float", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_float",
        Boolean.toString(useBloomFilterCol4));
    boolean useBloomFilterCol5 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_string", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_string",
        Boolean.toString(useBloomFilterCol5));
    boolean useBloomFilterCol6 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_boolean", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_boolean",
        Boolean.toString(useBloomFilterCol6));
    boolean useBloomFilterCol7 =
        PropertyUtil.propertyAsBoolean(
            table.properties(), PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_date", false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_date",
        Boolean.toString(useBloomFilterCol7));
    boolean useBloomFilterCol8 =
        PropertyUtil.propertyAsBoolean(
            table.properties(),
            PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_int_decimal",
            false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_int_decimal",
        Boolean.toString(useBloomFilterCol8));
    boolean useBloomFilterCol9 =
        PropertyUtil.propertyAsBoolean(
            table.properties(),
            PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long_decimal",
            false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_long_decimal",
        Boolean.toString(useBloomFilterCol9));
    boolean useBloomFilterCol10 =
        PropertyUtil.propertyAsBoolean(
            table.properties(),
            PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_fixed_decimal",
            false);
    factory.set(
        PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id_fixed_decimal",
        Boolean.toString(useBloomFilterCol10));
    long blockSize =
        PropertyUtil.propertyAsLong(
            table.properties(), PARQUET_ROW_GROUP_SIZE_BYTES, PARQUET_ROW_GROUP_SIZE_BYTES_DEFAULT);
    factory.set(PARQUET_ROW_GROUP_SIZE_BYTES, Long.toString(blockSize));

    FileAppender<Record> writer = factory.newAppender(out, format);
    try (Closeable toClose = writer) {
      writer.addAll(rows);
    }

    return DataFiles.builder(table.spec())
        .withFormat(format)
        .withPath(out.location())
        .withPartition(partition)
        .withFileSizeInBytes(writer.length())
        .withSplitOffsets(writer.splitOffsets())
        .withMetrics(writer.metrics())
        .build();
  }

  private FileFormat defaultFormat(Map<String, String> properties) {
    String formatString = properties.getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.fromString(formatString);
  }

  @TestTemplate
  public void testReadWithFilter() {
    Dataset<org.apache.spark.sql.Row> df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            // this is from the first row group
            .filter(
                "id = 30 AND id_long = 1030 AND id_double = 10030.0 AND id_float = 100030.0"
                    + " AND id_string = 'BINARY测试_30' AND id_boolean = true AND id_date = '2021-09-05'"
                    + " AND id_int_decimal = 77.77 AND id_long_decimal = 88.88 AND id_fixed_decimal = 99.99");

    Record record = SparkValueConverter.convert(table.schema(), df.collectAsList().get(0));

    assertThat(df.collectAsList()).as("Table should contain 1 row").hasSize(1);
    assertThat(record.get(0)).as("Table should contain expected rows").isEqualTo(30);

    df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            // this is from the third row group
            .filter(
                "id = 250 AND id_long = 1250 AND id_double = 10250.0 AND id_float = 100250.0"
                    + " AND id_string = 'BINARY测试_250' AND id_boolean = true AND id_date = '2021-09-05'"
                    + " AND id_int_decimal = 77.77 AND id_long_decimal = 88.88 AND id_fixed_decimal = 99.99");

    record = SparkValueConverter.convert(table.schema(), df.collectAsList().get(0));

    assertThat(df.collectAsList()).as("Table should contain 1 row").hasSize(1);
    assertThat(record.get(0)).as("Table should contain expected rows").isEqualTo(250);
  }
}
