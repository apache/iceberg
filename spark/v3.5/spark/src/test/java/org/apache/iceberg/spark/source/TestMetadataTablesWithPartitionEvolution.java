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
import static org.apache.iceberg.MetadataTableType.PARTITIONS;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.MANIFEST_MERGE_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TestHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestMetadataTablesWithPartitionEvolution extends CatalogTestBase {

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
        1
      },
      {
        "testhive",
        SparkCatalog.class.getName(),
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default"),
        ORC,
        2
      },
      {"testhadoop", SparkCatalog.class.getName(), ImmutableMap.of("type", "hadoop"), PARQUET, 1},
      {"testhadoop", SparkCatalog.class.getName(), ImmutableMap.of("type", "hadoop"), PARQUET, 2},
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
        1
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
        2
      }
    };
  }

  @Parameter(index = 3)
  private FileFormat fileFormat;

  @Parameter(index = 4)
  private int formatVersion;

  @AfterEach
  public void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testFilesMetadataTable() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      Dataset<Row> df = loadMetadataTable(tableType);
      assertThat(df.schema().getFieldIndex("partition").isEmpty())
          .as("Partition must be skipped")
          .isTrue();
    }

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the first partition column
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(new Object[] {null}), row("b1")), "STRUCT<data:STRING>", tableType);
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row(new Object[] {null}), row(new Object[] {null}), row("b1")),
          "STRUCT<data:STRING>",
          tableType);
    }

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the second partition column
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row(null, null),
              row(null, null),
              row(null, null),
              row("b1", null),
              row("b1", null),
              row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after dropping the first partition column
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row(null, null),
              row(null, null),
              row(null, null),
              row(null, null),
              row(null, 2),
              row("b1", null),
              row("b1", null),
              row("b1", null),
              row("b1", 2),
              row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8:INT>",
          tableType);
    }

    table.updateSpec().renameField("category_bucket_8", "category_bucket_8_another_name").commit();
    sql("REFRESH TABLE %s", tableName);

    // verify the metadata tables after renaming the second partition column
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
          tableType);
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row(null, null),
              row(null, null),
              row(null, null),
              row(null, null),
              row(null, 2),
              row("b1", null),
              row("b1", null),
              row("b1", null),
              row("b1", 2),
              row("b1", 2)),
          "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
          tableType);
    }
  }

  @TestTemplate
  public void testFilesMetadataTableFilter() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");
    sql("ALTER TABLE %s SET TBLPROPERTIES ('%s' 'false')", tableName, MANIFEST_MERGE_ENABLED);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      Dataset<Row> df = loadMetadataTable(tableType);
      assertThat(df.schema().getFieldIndex("partition").isEmpty())
          .as("Partition must be skipped")
          .isTrue();
    }

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables after adding the first partition column
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row("d2")), "STRUCT<data:STRING>", tableType, "partition.data = 'd2'");
    }

    table.updateSpec().addField("category").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables after adding the second partition column
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row("d2", null), row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.data = 'd2'");
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row("d2", null), row("d2", null), row("d2", null), row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.data = 'd2'");
    }
    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.category = 'c2'");
    }

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);

    // Verify new partitions do not show up for removed 'partition.data=d2' query
    sql("INSERT INTO TABLE %s VALUES (3, 'c3', 'd2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'c4', 'd2')", tableName);

    // Verify new partitions do show up for 'partition.category=c2' query
    sql("INSERT INTO TABLE %s VALUES (5, 'c2', 'd5')", tableName);

    // no new partition should show up for 'data' partition query as partition field has been
    // removed
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row("d2", null), row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.data = 'd2'");
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row("d2", null),
              row("d2", null),
              row("d2", null),
              row("d2", null),
              row("d2", null),
              row("d2", null),
              row("d2", "c2"),
              row("d2", "c2"),
              row("d2", "c2"),
              row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.data = 'd2'");
    }

    // new partition shows up from 'category' partition field query
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, "c2"), row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.category = 'c2'");
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row(null, "c2"), row("d2", "c2"), row("d2", "c2"), row("d2", "c2"), row("d2", "c2")),
          "STRUCT<data:STRING,category:STRING>",
          tableType,
          "partition.category = 'c2'");
    }

    table.updateSpec().renameField("category", "category_another_name").commit();
    sql("REFRESH TABLE %s", tableName);

    // Verify new partitions do show up for 'category=c2' query
    sql("INSERT INTO TABLE %s VALUES (6, 'c2', 'd6')", tableName);
    for (MetadataTableType tableType : Arrays.asList(FILES)) {
      assertPartitions(
          ImmutableList.of(row(null, "c2"), row(null, "c2"), row("d2", "c2")),
          "STRUCT<data:STRING,category_another_name:STRING>",
          tableType,
          "partition.category_another_name = 'c2'");
    }
    for (MetadataTableType tableType : Arrays.asList(ALL_DATA_FILES)) {
      assertPartitions(
          ImmutableList.of(
              row(null, "c2"),
              row(null, "c2"),
              row(null, "c2"),
              row("d2", "c2"),
              row("d2", "c2"),
              row("d2", "c2"),
              row("d2", "c2"),
              row("d2", "c2")),
          "STRUCT<data:STRING,category_another_name:STRING>",
          tableType,
          "partition.category_another_name = 'c2'");
    }
  }

  @TestTemplate
  public void testEntriesMetadataTable() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    for (MetadataTableType tableType : Arrays.asList(ENTRIES, ALL_ENTRIES)) {
      Dataset<Row> df = loadMetadataTable(tableType);
      StructType dataFileType = (StructType) df.schema().apply("data_file").dataType();
      assertThat(dataFileType.getFieldIndex("").isEmpty()).as("Partition must be skipped").isTrue();
    }

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the first partition column
    assertPartitions(
        ImmutableList.of(row(new Object[] {null}), row("b1")), "STRUCT<data:STRING>", ENTRIES);
    assertPartitions(
        ImmutableList.of(row(new Object[] {null}), row(new Object[] {null}), row("b1")),
        "STRUCT<data:STRING>",
        ALL_ENTRIES);

    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after adding the second partition column
    assertPartitions(
        ImmutableList.of(row(null, null), row("b1", null), row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8:INT>",
        ENTRIES);
    assertPartitions(
        ImmutableList.of(
            row(null, null),
            row(null, null),
            row(null, null),
            row("b1", null),
            row("b1", null),
            row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8:INT>",
        ALL_ENTRIES);

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    // verify the metadata tables after dropping the first partition column
    assertPartitions(
        ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8:INT>",
        ENTRIES);
    assertPartitions(
        ImmutableList.of(
            row(null, null),
            row(null, null),
            row(null, null),
            row(null, null),
            row(null, 2),
            row("b1", null),
            row("b1", null),
            row("b1", null),
            row("b1", 2),
            row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8:INT>",
        ALL_ENTRIES);

    table.updateSpec().renameField("category_bucket_8", "category_bucket_8_another_name").commit();
    sql("REFRESH TABLE %s", tableName);

    // verify the metadata tables after renaming the second partition column
    assertPartitions(
        ImmutableList.of(row(null, null), row(null, 2), row("b1", null), row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
        ENTRIES);
    assertPartitions(
        ImmutableList.of(
            row(null, null),
            row(null, null),
            row(null, null),
            row(null, null),
            row(null, 2),
            row("b1", null),
            row("b1", null),
            row("b1", null),
            row("b1", 2),
            row("b1", 2)),
        "STRUCT<data:STRING,category_bucket_8_another_name:INT>",
        ALL_ENTRIES);
  }

  @TestTemplate
  public void testPartitionsTableAddRemoveFields() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables while the current spec is still unpartitioned
    Dataset<Row> df = loadMetadataTable(PARTITIONS);
    assertThat(df.schema().getFieldIndex("partition").isEmpty())
        .as("Partition must be skipped")
        .isTrue();

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables after adding the first partition column
    assertPartitions(
        ImmutableList.of(row(new Object[] {null}), row("d1"), row("d2")),
        "STRUCT<data:STRING>",
        PARTITIONS);

    table.updateSpec().addField("category").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // verify the metadata tables after adding the second partition column
    assertPartitions(
        ImmutableList.of(
            row(null, null), row("d1", null), row("d1", "c1"), row("d2", null), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS);

    // verify the metadata tables after removing the first partition column
    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(
            row(null, null),
            row(null, "c1"),
            row(null, "c2"),
            row("d1", null),
            row("d1", "c1"),
            row("d2", null),
            row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS);
  }

  @TestTemplate
  public void testPartitionsTableRenameFields() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").addField("category").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d1", "c1"), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS);

    table.updateSpec().renameField("category", "category_another_name").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d1", "c1"), row("d2", "c2")),
        "STRUCT<data:STRING,category_another_name:STRING>",
        PARTITIONS);
  }

  @TestTemplate
  public void testPartitionsTableSwitchFields() throws Exception {
    createTable("id bigint NOT NULL, category string, data string");

    Table table = validationCatalog.loadTable(tableIdent);

    // verify the metadata tables after re-adding the first dropped column in the second location
    table.updateSpec().addField("data").addField("category").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d1", "c1"), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS);

    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row(null, "c1"), row(null, "c2"), row("d1", "c1"), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS);

    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (3, 'c3', 'd3')", tableName);

    if (formatVersion == 1) {
      assertPartitions(
          ImmutableList.of(
              row(null, "c1", null),
              row(null, "c1", "d1"),
              row(null, "c2", null),
              row(null, "c2", "d2"),
              row(null, "c3", "d3"),
              row("d1", "c1", null),
              row("d2", "c2", null)),
          "STRUCT<data_1000:STRING,category:STRING,data:STRING>",
          PARTITIONS);
    } else {
      // In V2 re-adding a former partition field that was part of an older spec will not change its
      // name or its
      // field ID either, thus values will be collapsed into a single common column (as opposed to
      // V1 where any new
      // partition field addition will result in a new column in this metadata table)
      assertPartitions(
          ImmutableList.of(
              row(null, "c1"), row(null, "c2"), row("d1", "c1"), row("d2", "c2"), row("d3", "c3")),
          "STRUCT<data:STRING,category:STRING>",
          PARTITIONS);
    }
  }

  @TestTemplate
  public void testPartitionTableFilterAddRemoveFields() throws ParseException {
    // Create un-partitioned table
    createTable("id bigint NOT NULL, category string, data string");

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // Partition Table with one partition column
    Table table = validationCatalog.loadTable(tableIdent);
    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d2")), "STRUCT<data:STRING>", PARTITIONS, "partition.data = 'd2'");

    // Partition Table with two partition column
    table.updateSpec().addField("category").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d2", null), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS,
        "partition.data = 'd2'");
    assertPartitions(
        ImmutableList.of(row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS,
        "partition.category = 'c2'");

    // Partition Table with first partition column removed
    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (3, 'c3', 'd2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (4, 'c4', 'd2')", tableName);
    sql("INSERT INTO TABLE %s VALUES (5, 'c2', 'd5')", tableName);
    assertPartitions(
        ImmutableList.of(row("d2", null), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS,
        "partition.data = 'd2'");
    assertPartitions(
        ImmutableList.of(row(null, "c2"), row("d2", "c2")),
        "STRUCT<data:STRING,category:STRING>",
        PARTITIONS,
        "partition.category = 'c2'");
  }

  @TestTemplate
  public void testPartitionTableFilterSwitchFields() throws Exception {
    // Re-added partition fields currently not re-associated:
    // https://github.com/apache/iceberg/issues/4292
    // In V1, dropped partition fields show separately when field is re-added
    // In V2, re-added field currently conflicts with its deleted form
    assumeThat(formatVersion).isEqualTo(1);

    createTable("id bigint NOT NULL, category string, data string");
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);
    Table table = validationCatalog.loadTable(tableIdent);

    // Two partition columns
    table.updateSpec().addField("data").addField("category").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // Drop first partition column
    table.updateSpec().removeField("data").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    // Re-add first partition column at the end
    table.updateSpec().addField("data").commit();
    sql("REFRESH TABLE %s", tableName);

    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row(null, "c2", null), row(null, "c2", "d2"), row("d2", "c2", null)),
        "STRUCT<data_1000:STRING,category:STRING,data:STRING>",
        PARTITIONS,
        "partition.category = 'c2'");

    assertPartitions(
        ImmutableList.of(row(null, "c1", "d1")),
        "STRUCT<data_1000:STRING,category:STRING,data:STRING>",
        PARTITIONS,
        "partition.data = 'd1'");
  }

  @TestTemplate
  public void testPartitionsTableFilterRenameFields() throws ParseException {
    createTable("id bigint NOT NULL, category string, data string");

    Table table = validationCatalog.loadTable(tableIdent);

    table.updateSpec().addField("data").addField("category").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    table.updateSpec().renameField("category", "category_another_name").commit();
    sql("REFRESH TABLE %s", tableName);
    sql("INSERT INTO TABLE %s VALUES (1, 'c1', 'd1')", tableName);
    sql("INSERT INTO TABLE %s VALUES (2, 'c2', 'd2')", tableName);

    assertPartitions(
        ImmutableList.of(row("d1", "c1")),
        "STRUCT<data:STRING,category_another_name:STRING>",
        PARTITIONS,
        "partition.category_another_name = 'c1'");
  }

  @TestTemplate
  public void testMetadataTablesWithUnknownTransforms() {
    createTable("id bigint NOT NULL, category string, data string");

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", tableName);

    Table table = validationCatalog.loadTable(tableIdent);

    PartitionSpec unknownSpec =
        TestHelpers.newExpectedSpecBuilder()
            .withSchema(table.schema())
            .withSpecId(1)
            .addField("zero", 1, "id_zero")
            .build();

    // replace the table spec to include an unknown transform
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.updatePartitionSpec(unknownSpec));

    sql("REFRESH TABLE %s", tableName);

    for (MetadataTableType tableType : Arrays.asList(FILES, ALL_DATA_FILES, ENTRIES, ALL_ENTRIES)) {
      assertThatThrownBy(() -> loadMetadataTable(tableType))
          .isInstanceOf(ValidationException.class)
          .hasMessage("Cannot build table partition type, unknown transforms: [zero]");
    }
  }

  @TestTemplate
  public void testPartitionColumnNamedPartition() {
    sql(
        "CREATE TABLE %s (id int, partition int) USING iceberg PARTITIONED BY (partition)",
        tableName);
    sql("INSERT INTO %s VALUES (1, 1), (2, 1), (3, 2), (2, 2)", tableName);
    List<Object[]> expected = ImmutableList.of(row(1, 1), row(2, 1), row(3, 2), row(2, 2));
    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
    assertThat(sql("SELECT * FROM %s.files", tableName)).hasSize(2);
  }

  private void assertPartitions(
      List<Object[]> expectedPartitions, String expectedTypeAsString, MetadataTableType tableType)
      throws ParseException {
    assertPartitions(expectedPartitions, expectedTypeAsString, tableType, null);
  }

  private void assertPartitions(
      List<Object[]> expectedPartitions,
      String expectedTypeAsString,
      MetadataTableType tableType,
      String filter)
      throws ParseException {
    Dataset<Row> df = loadMetadataTable(tableType);
    if (filter != null) {
      df = df.filter(filter);
    }

    DataType expectedType = spark.sessionState().sqlParser().parseDataType(expectedTypeAsString);
    switch (tableType) {
      case PARTITIONS:
      case FILES:
      case ALL_DATA_FILES:
        DataType actualFilesType = df.schema().apply("partition").dataType();
        assertThat(actualFilesType).as("Partition type must match").isEqualTo(expectedType);
        break;

      case ENTRIES:
      case ALL_ENTRIES:
        StructType dataFileType = (StructType) df.schema().apply("data_file").dataType();
        DataType actualEntriesType = dataFileType.apply("partition").dataType();
        assertThat(actualEntriesType).as("Partition type must match").isEqualTo(expectedType);
        break;

      default:
        throw new UnsupportedOperationException("Unsupported metadata table type: " + tableType);
    }

    switch (tableType) {
      case PARTITIONS:
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

  private void createTable(String schema) {
    sql(
        "CREATE TABLE %s (%s) USING iceberg TBLPROPERTIES ('%s' '%s', '%s' '%d')",
        tableName, schema, DEFAULT_FILE_FORMAT, fileFormat.name(), FORMAT_VERSION, formatVersion);
  }
}
