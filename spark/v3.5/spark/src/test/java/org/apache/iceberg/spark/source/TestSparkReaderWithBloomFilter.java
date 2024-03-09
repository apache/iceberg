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

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.spark.sql.CreateTableWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
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
  protected List<Row> rowList = null;
  protected DataFile dataFile = null;

  protected static SparkSession spark = null;

  @Parameter(index = 0)
  protected boolean vectorized;

  @Parameter(index = 1)
  protected boolean useBloomFilter;

  // Schema passed to create tables
  public static final StructType schema =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("id_long", DataTypes.LongType, false, Metadata.empty()),
            new StructField("id_double", DataTypes.DoubleType, false, Metadata.empty()),
            new StructField("id_float", DataTypes.FloatType, false, Metadata.empty()),
            new StructField("id_string", DataTypes.StringType, false, Metadata.empty()),
            new StructField("id_boolean", DataTypes.BooleanType, true, Metadata.empty()),
            new StructField("id_date", DataTypes.DateType, true, Metadata.empty()),
            new StructField(
                "id_int_decimal", DataTypes.createDecimalType(8, 2), true, Metadata.empty()),
            new StructField(
                "id_long_decimal", DataTypes.createDecimalType(14, 2), true, Metadata.empty()),
            new StructField(
                "id_fixed_decimal", DataTypes.createDecimalType(31, 2), true, Metadata.empty()),
            new StructField(
                "id_nested",
                new StructType(
                    new StructField[] {
                      new StructField("nested_id", DataTypes.IntegerType, true, Metadata.empty())
                    }),
                true,
                Metadata.empty())
          });

  private static final int INT_MIN_VALUE = 30;
  private static final int INT_MAX_VALUE = 329;
  private static final int INT_VALUE_COUNT = INT_MAX_VALUE - INT_MIN_VALUE + 1;
  private static final long LONG_BASE = 1000L;
  private static final double DOUBLE_BASE = 10000D;
  private static final float FLOAT_BASE = 100000F;
  private static final String BINARY_PREFIX = "BINARY测试_";

  @TempDir private static Path temp;

  @BeforeEach
  public void writeData() throws NoSuchTableException, TableAlreadyExistsException {
    this.tableName = "test";
    createTable(tableName, schema);
    this.rowList = Lists.newArrayList();

    for (int i = 0; i < INT_VALUE_COUNT; i += 1) {
      Row row =
          RowFactory.create(
              INT_MIN_VALUE + i,
              LONG_BASE + INT_MIN_VALUE + i,
              DOUBLE_BASE + INT_MIN_VALUE + i,
              FLOAT_BASE + INT_MIN_VALUE + i,
              BINARY_PREFIX + (INT_MIN_VALUE + i),
              i % 2 == 0,
              java.sql.Date.valueOf(LocalDate.parse("2021-09-05")),
              new BigDecimal("77.77"),
              new BigDecimal("88.88"),
              new BigDecimal("99.99"),
              RowFactory.create(INT_MIN_VALUE + i));
      rowList.add(row);
    }

    Dataset<Row> dataset = spark.createDataFrame(rowList, schema);

    dataset.writeTo("default." + tableName).append();
  }

  @AfterEach
  public void cleanup() {
    dropTable("test");
  }

  @Parameters(name = "vectorized = {0}, useBloomFilter = {1}")
  public static Object[][] parameters() {
    return new Object[][] {{false, false}, {true, false}, {false, true}, {true, true}};
  }

  @BeforeAll
  public static void startMetastoreAndSpark() {
    spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", temp.toString())
            .config("spark.sql.defaultCatalog", "local")
            .getOrCreate();

    spark.sql("CREATE DATABASE IF NOT EXISTS default");
    spark.sql("USE default");
  }

  @AfterAll
  public static void stopMetastoreAndSpark() {
    spark.stop();
    spark = null;
  }

  protected void createTable(String name, StructType schema) throws TableAlreadyExistsException {
    Dataset<Row> emptyDf = spark.createDataFrame(Lists.newArrayList(), schema);
    CreateTableWriter<Row> createTableWriter = emptyDf.writeTo("default." + name);

    if (useBloomFilter) {
      String[] columns = {
        "id",
        "id_long",
        "id_double",
        "id_float",
        "id_string",
        "id_boolean",
        "id_date",
        "id_int_decimal",
        "id_long_decimal",
        "id_fixed_decimal",
        "id_nested.nested_id"
      };
      for (String column : columns) {
        createTableWriter.tableProperty(
            PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + column, "true");
      }
    }

    createTableWriter.tableProperty(PARQUET_ROW_GROUP_SIZE_BYTES, "100");

    if (vectorized) {
      createTableWriter
          .tableProperty(TableProperties.PARQUET_VECTORIZATION_ENABLED, "true")
          .tableProperty(TableProperties.PARQUET_BATCH_SIZE, "4");
    }

    createTableWriter.create();
  }

  protected void dropTable(String name) {
    spark.sql("DROP TABLE IF EXISTS default." + name);
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
                    + " AND id_int_decimal = 77.77 AND id_long_decimal = 88.88 AND id_fixed_decimal = 99.99"
                    + " AND id_nested.nested_id = 30");

    List<Row> rows = df.collectAsList();

    assertThat(rows).as("Table should contain 1 row").hasSize(1);
    assertThat(rows.get(0).get(0)).as("Table should contain expected rows").isEqualTo(30);

    df =
        spark
            .read()
            .format("iceberg")
            .load(TableIdentifier.of("default", tableName).toString())
            // this is from the third row group
            .filter(
                "id = 250 AND id_long = 1250 AND id_double = 10250.0 AND id_float = 100250.0"
                    + " AND id_string = 'BINARY测试_250' AND id_boolean = true AND id_date = '2021-09-05'"
                    + " AND id_int_decimal = 77.77 AND id_long_decimal = 88.88 AND id_fixed_decimal = 99.99"
                    + " AND id_nested.nested_id = 250");

    rows = df.collectAsList();

    assertThat(rows).as("Table should contain 1 row").hasSize(1);
    assertThat(rows.get(0).get(0)).as("Table should contain expected rows").isEqualTo(250);
  }

  @TestTemplate
  public void testBloomCreation() throws IOException {
    org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(temp + "/default/test/data");
    FileSystem fs = FileSystem.get(path.toUri(), spark.sparkContext().hadoopConfiguration());
    Optional<FileStatus> parquetFile = Arrays.stream(fs.listStatus(path)).findAny();
    assertThat(parquetFile).isPresent();
    ParquetMetadata parquetMetadata =
        ParquetFileReader.readFooter(new Configuration(), parquetFile.get());
    for (int i = 0; i < 11; i++) {
      if (useBloomFilter) {
        assertNotEquals(
            parquetMetadata.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset(), -1L);
      } else {
        assertEquals(
            parquetMetadata.getBlocks().get(0).getColumns().get(0).getBloomFilterOffset(), -1L);
      }
    }
  }
}
