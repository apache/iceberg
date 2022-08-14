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

import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.MetadataTableType.ALL_DATA_FILES;
import static org.apache.iceberg.MetadataTableType.ALL_ENTRIES;
import static org.apache.iceberg.MetadataTableType.ENTRIES;
import static org.apache.iceberg.MetadataTableType.FILES;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkCatalogTestBase;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestMetadataTablesWithPartitionEvolution extends SparkCatalogTestBase {

  @Parameters(name = "catalog = {0}, impl = {1}, conf = {2}, fileFormat = {3}, formatVersion = {4}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        ORC,
        formatVersion()
      },
      {
        "testhadoop",
        SparkCatalog.class.getName(),
        ImmutableMap.of("type", "hadoop"),
        PARQUET,
        formatVersion()
      },
      {
        "spark_catalog",
        SparkSessionCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "clients", "1",
            "parquet-enabled", "false",
            "cache-enabled",
                "false" // Spark will delete tables using v1, leaving the cache out of sync
            ),
        AVRO,
        formatVersion()
      }
    };
  }

  private static int formatVersion() {
    return RANDOM.nextInt(2) + 1;
  }

  private static final Random RANDOM = ThreadLocalRandom.current();

  private final FileFormat fileFormat;
  private final int formatVersion;

  public TestMetadataTablesWithPartitionEvolution(
      String catalogName,
      String implementation,
      Map<String, String> config,
      FileFormat fileFormat,
      int formatVersion) {
    super(catalogName, implementation, config);
    this.fileFormat = fileFormat;
    this.formatVersion = formatVersion;
  }

  @After
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testFilesMetadataTable() throws ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, data string) USING iceberg",
        tableName);
    initTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      Dataset<Row> df = loadMetadataTable(tableType);
      Assert.assertTrue(
          "Partition must be skipped", df.schema().getFieldIndex("partition").isEmpty());
    }

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the first partition column
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row(new Object[] {null}), row("b1")), "STRUCT<data:STRING>", tableType);
    }

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the second partition column
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after dropping the first partition column
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().renameField("category_bucket_8", "category_bucket_8_another_name").commit();
    sql("REFRESH TABLE %s", tableName);

    // verify the metadata tables after renaming the second partition column
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
          tableType);
    }
  }

  @Test
  public void testEntriesMetadataTable() throws ParseException {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, data string) USING iceberg",
        tableName);
    initTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      Dataset<Row> df = loadMetadataTable(tableType);
      StructType dataFileType = (StructType) df.schema().apply("data_file").dataType();
      Assert.assertTrue("Partition must be skipped", dataFileType.getFieldIndex("").isEmpty());
    }

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the first partition column
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      assertPartitions(
          ImmutableList.of(row(new Object[] {null}), row("b1")), "STRUCT<data:STRING>", tableType);
    }

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the second partition column
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after dropping the first partition column
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().renameField("category_bucket_8", "category_bucket_8_another_name").commit();
    sql("REFRESH TABLE %s", tableName);

    // verify the metadata tables after renaming the second partition column
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
          tableType);
    }
  }

  @Test
  public void testMetadataTablesWithUnknownTransforms() {
    sql(
        "CREATE TABLE %s (id bigint NOT NULL, category string, data string) USING iceberg",
        tableName);
    initTable();

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    PartitionSpec unknownSpec =
        PartitionSpecParser.fromJson(
            table.schema(),
            "{ \"spec-id\": 1, \"fields\": [ { \"name\": \"id_zero\", \"transform\": \"zero\", \"source-id\": 1 } ] }");

    // replace the table spec to include an unknown transform
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.updatePartitionSpec(unknownSpec));

    sql("REFRESH TABLE %s", tableName);

    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES, ENTRIES, ALL_ENTRIES)) {
      AssertHelpers.assertThrows(
          "Should complain about the partition type",
          ValidationException.class,
          "Cannot build table partition type, unknown transforms",
          () -> loadMetadataTable(tableType));
    }
  }

  private void assertPartitions(
      List<Object[]> expectedPartitions, String expectedTypeAsString, MetadataTableType tableType)
      throws ParseException {
    Dataset<Row> df = loadMetadataTable(tableType);

    DataType expectedType = spark.sessionState().sqlParser().parseDataType(expectedTypeAsString);
    switch (tableType) {
      case FILES:
      case ALL_DATA_FILES:
        DataType actualFilesType = df.schema().apply("partition").dataType();
        Assert.assertEquals("Partition type must match", expectedType, actualFilesType);
        break;

      case ENTRIES:
      case ALL_ENTRIES:
        StructType dataFileType = (StructType) df.schema().apply("data_file").dataType();
        DataType actualEntriesType = dataFileType.apply("partition").dataType();
        Assert.assertEquals("Partition type must match", expectedType, actualEntriesType);
        break;

      default:
        throw new UnsupportedOperationException("Unsupported metadata table type: " + tableType);
    }

    switch (tableType) {
      case FILES:
      case ALL_DATA_FILES:
        List<Row> actualFilesPartitions =
            df.orderBy("partition").select("partition.*").collectAsList();
        assertEquals(
            "Partitions must match", expectedPartitions, rowsToJava(actualFilesPartitions));
        break;

      case ENTRIES:
      case ALL_ENTRIES:
        List<Row> actualEntriesPartitions =
            df.orderBy("data_file.partition").select("data_file.partition.*").collectAsList();
        assertEquals(
            "Partitions must match", expectedPartitions, rowsToJava(actualEntriesPartitions));
        break;

      default:
        throw new UnsupportedOperationException("Unsupported metadata table type: " + tableType);
    }
  }

  private Dataset<Row> loadMetadataTable(MetadataTableType tableType) {
    return spark.read().format("iceberg").load(tableName + "." + tableType.name());
  }

  private void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES('%s' '%s')",
        tableName, DEFAULT_FILE_FORMAT, fileFormat.name());
    sql("ALTER TABLE %s SET TBLPROPERTIES('%s' '%d')", tableName, FORMAT_VERSION, formatVersion);
  }
}
