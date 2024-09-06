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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.actions.NDVSketchUtil.APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.BlobMetadata;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.ComputeTableStats;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.spark.SparkWriteOptions;
import org.apache.iceberg.spark.data.RandomData;
import org.apache.iceberg.spark.source.SimpleRecord;
import org.apache.iceberg.types.Types;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;

public class TestComputeTableStatsAction extends CatalogTestBase {

  private static final Types.StructType LEAF_STRUCT_TYPE =
      Types.StructType.of(
          optional(1, "leafLongCol", Types.LongType.get()),
          optional(2, "leafDoubleCol", Types.DoubleType.get()));

  private static final Types.StructType NESTED_STRUCT_TYPE =
      Types.StructType.of(required(3, "leafStructCol", LEAF_STRUCT_TYPE));

  private static final Schema NESTED_SCHEMA =
      new Schema(required(4, "nestedStructCol", NESTED_STRUCT_TYPE));

  private static final Schema SCHEMA_WITH_NESTED_COLUMN =
      new Schema(
          required(4, "nestedStructCol", NESTED_STRUCT_TYPE),
          required(5, "stringCol", Types.StringType.get()));

  @TestTemplate
  public void testLoadingTableDirectly() {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    sql("INSERT into %s values(1, 'abcd')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result results = actions.computeTableStats(table).execute();
    StatisticsFile statisticsFile = results.statisticsFile();
    Assertions.assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 2);
  }

  @TestTemplate
  public void testComputeTableStatsAction() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    // To create multiple splits on the mapper
    table
        .updateProperties()
        .set("read.split.target-size", "100")
        .set("write.parquet.row-group-size-bytes", "100")
        .commit();
    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark.createDataset(records, Encoders.bean(SimpleRecord.class)).writeTo(tableName).append();
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result results =
        actions.computeTableStats(table).columns("id", "data").execute();
    assertNotNull(results);

    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);

    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 2);

    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    Assertions.assertEquals(
        blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY),
        String.valueOf(4));
  }

  @TestTemplate
  public void testComputeTableStatsActionWithoutExplicitColumns()
      throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);

    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result results = actions.computeTableStats(table).execute();
    assertNotNull(results);

    Assertions.assertEquals(1, table.statisticsFiles().size());
    StatisticsFile statisticsFile = table.statisticsFiles().get(0);
    Assertions.assertEquals(2, statisticsFile.blobMetadata().size());
    assertNotEquals(0, statisticsFile.fileSizeInBytes());
    Assertions.assertEquals(
        4,
        Long.parseLong(
            statisticsFile
                .blobMetadata()
                .get(0)
                .properties()
                .get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY)));
    Assertions.assertEquals(
        4,
        Long.parseLong(
            statisticsFile
                .blobMetadata()
                .get(1)
                .properties()
                .get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY)));
  }

  @TestTemplate
  public void testComputeTableStatsForInvalidColumns() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    // Append data to create snapshot
    sql("INSERT into %s values(1, 'abcd')", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> actions.computeTableStats(table).columns("id1").execute());
    String message = exception.getMessage();
    assertTrue(message.contains("Can't find column id1 in table"));
  }

  @TestTemplate
  public void testComputeTableStatsWithNoSnapshots() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result result = actions.computeTableStats(table).columns("id").execute();
    Assertions.assertNull(result.statisticsFile());
  }

  @TestTemplate
  public void testComputeTableStatsWithNullValues() throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    List<SimpleRecord> records =
        Lists.newArrayList(
            new SimpleRecord(1, null),
            new SimpleRecord(1, "a"),
            new SimpleRecord(2, "b"),
            new SimpleRecord(3, "c"),
            new SimpleRecord(4, "d"));
    spark
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result results = actions.computeTableStats(table).columns("data").execute();
    assertNotNull(results);

    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);

    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 1);

    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    Assertions.assertEquals(
        blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY),
        String.valueOf(4));
  }

  @TestTemplate
  public void testComputeTableStatsWithSnapshotHavingDifferentSchemas()
      throws NoSuchTableException, ParseException {
    SparkActions actions = SparkActions.get();
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    // Append data to create snapshot
    sql("INSERT into %s values(1, 'abcd')", tableName);
    long snapshotId1 = Spark3Util.loadIcebergTable(spark, tableName).currentSnapshot().snapshotId();
    // Snapshot id not specified
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    assertDoesNotThrow(() -> actions.computeTableStats(table).columns("data").execute());

    sql("ALTER TABLE %s DROP COLUMN %s", tableName, "data");
    // Append data to create snapshot
    sql("INSERT into %s values(1)", tableName);
    table.refresh();
    long snapshotId2 = Spark3Util.loadIcebergTable(spark, tableName).currentSnapshot().snapshotId();

    // Snapshot id specified
    assertDoesNotThrow(
        () -> actions.computeTableStats(table).snapshot(snapshotId1).columns("data").execute());

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> actions.computeTableStats(table).snapshot(snapshotId2).columns("data").execute());
    String message = exception.getMessage();
    assertTrue(message.contains("Can't find column data in table"));
  }

  @TestTemplate
  public void testComputeTableStatsWhenSnapshotIdNotSpecified()
      throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, data string) USING iceberg", tableName);
    // Append data to create snapshot
    sql("INSERT into %s values(1, 'abcd')", tableName);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    ComputeTableStats.Result results = actions.computeTableStats(table).columns("data").execute();

    assertNotNull(results);

    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);

    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 1);

    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    Assertions.assertEquals(
        blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY),
        String.valueOf(1));
  }

  @TestTemplate
  public void testComputeTableStatsWithNestedSchema()
      throws NoSuchTableException, ParseException, IOException {
    List<Record> records = Lists.newArrayList(createNestedRecord());
    Table table =
        validationCatalog.createTable(
            tableIdent,
            SCHEMA_WITH_NESTED_COLUMN,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of());
    DataFile dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.toFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    Table tbl = Spark3Util.loadIcebergTable(spark, tableName);
    SparkActions actions = SparkActions.get();
    actions.computeTableStats(tbl).execute();

    tbl.refresh();
    List<StatisticsFile> statisticsFiles = tbl.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);
    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 1);
  }

  @TestTemplate
  public void testComputeTableStatsWithNoComputableColumns() throws IOException {
    List<Record> records = Lists.newArrayList(createNestedRecord());
    Table table =
        validationCatalog.createTable(
            tableIdent, NESTED_SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of());
    DataFile dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.toFile()), records);
    table.newAppend().appendFile(dataFile).commit();

    table.refresh();
    SparkActions actions = SparkActions.get();
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> actions.computeTableStats(table).execute());
    Assertions.assertEquals(exception.getMessage(), "No columns found to compute stats");
  }

  @TestTemplate
  public void testComputeTableStatsOnByteColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("byte_col", "TINYINT");
  }

  @TestTemplate
  public void testComputeTableStatsOnShortColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("short_col", "SMALLINT");
  }

  @TestTemplate
  public void testComputeTableStatsOnIntColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("int_col", "INT");
  }

  @TestTemplate
  public void testComputeTableStatsOnLongColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("long_col", "BIGINT");
  }

  @TestTemplate
  public void testComputeTableStatsOnTimestampColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("timestamp_col", "TIMESTAMP");
  }

  @TestTemplate
  public void testComputeTableStatsOnTimestampNtzColumn()
      throws NoSuchTableException, ParseException {
    testComputeTableStats("timestamp_col", "TIMESTAMP_NTZ");
  }

  @TestTemplate
  public void testComputeTableStatsOnDateColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("date_col", "DATE");
  }

  @TestTemplate
  public void testComputeTableStatsOnDecimalColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("decimal_col", "DECIMAL(20, 2)");
  }

  @TestTemplate
  public void testComputeTableStatsOnBinaryColumn() throws NoSuchTableException, ParseException {
    testComputeTableStats("binary_col", "BINARY");
  }

  public void testComputeTableStats(String columnName, String type)
      throws NoSuchTableException, ParseException {
    sql("CREATE TABLE %s (id int, %s %s) USING iceberg", tableName, columnName, type);
    Table table = Spark3Util.loadIcebergTable(spark, tableName);

    Dataset<Row> dataDF = randomDataDF(table.schema());
    append(tableName, dataDF);

    SparkActions actions = SparkActions.get();
    table.refresh();
    ComputeTableStats.Result results =
        actions.computeTableStats(table).columns(columnName).execute();
    assertNotNull(results);

    List<StatisticsFile> statisticsFiles = table.statisticsFiles();
    Assertions.assertEquals(statisticsFiles.size(), 1);

    StatisticsFile statisticsFile = statisticsFiles.get(0);
    assertNotEquals(statisticsFile.fileSizeInBytes(), 0);
    Assertions.assertEquals(statisticsFile.blobMetadata().size(), 1);

    BlobMetadata blobMetadata = statisticsFile.blobMetadata().get(0);
    Assertions.assertNotNull(
        blobMetadata.properties().get(APACHE_DATASKETCHES_THETA_V1_NDV_PROPERTY));
  }

  private GenericRecord createNestedRecord() {
    GenericRecord record = GenericRecord.create(SCHEMA_WITH_NESTED_COLUMN);
    GenericRecord nested = GenericRecord.create(NESTED_STRUCT_TYPE);
    GenericRecord leaf = GenericRecord.create(LEAF_STRUCT_TYPE);
    leaf.set(0, 0L);
    leaf.set(1, 0.0);
    nested.set(0, leaf);
    record.set(0, nested);
    record.set(1, "data");
    return record;
  }

  private Dataset<Row> randomDataDF(Schema schema) {
    Iterable<InternalRow> rows = RandomData.generateSpark(schema, 10, 0);
    JavaRDD<InternalRow> rowRDD = sparkContext.parallelize(Lists.newArrayList(rows));
    StructType rowSparkType = SparkSchemaUtil.convert(schema);
    return spark.internalCreateDataFrame(JavaRDD.toRDD(rowRDD), rowSparkType, false);
  }

  private void append(String table, Dataset<Row> df) throws NoSuchTableException {
    // fanout writes are enabled as write-time clustering is not supported without Spark extensions
    df.coalesce(1).writeTo(table).option(SparkWriteOptions.FANOUT_ENABLED, "true").append();
  }

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }
}
