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

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.TableProperties.ORC_VECTORIZATION_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_VECTORIZATION_ENABLED;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.SparkTestBase;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestSparkMetadataColumns extends SparkTestBase {

  private static final String TABLE_NAME = "test_table";
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "category", Types.StringType.get()),
          Types.NestedField.optional(3, "data", Types.StringType.get()));
  private static final PartitionSpec UNKNOWN_SPEC =
      PartitionSpecParser.fromJson(
          SCHEMA,
          "{ \"spec-id\": 1, \"fields\": [ { \"name\": \"id_zero\", \"transform\": \"zero\", \"source-id\": 1 } ] }");

  @Parameterized.Parameters(name = "fileFormat = {0}, vectorized = {1}, formatVersion = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {FileFormat.PARQUET, false, 1},
      {FileFormat.PARQUET, true, 1},
      {FileFormat.PARQUET, false, 2},
      {FileFormat.PARQUET, true, 2},
      {FileFormat.AVRO, false, 1},
      {FileFormat.AVRO, false, 2},
      {FileFormat.ORC, false, 1},
      {FileFormat.ORC, true, 1},
      {FileFormat.ORC, false, 2},
      {FileFormat.ORC, true, 2},
    };
  }

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private final FileFormat fileFormat;
  private final boolean vectorized;
  private final int formatVersion;

  private Table table = null;

  public TestSparkMetadataColumns(FileFormat fileFormat, boolean vectorized, int formatVersion) {
    this.fileFormat = fileFormat;
    this.vectorized = vectorized;
    this.formatVersion = formatVersion;
  }

  @BeforeClass
  public static void setupSpark() {
    ImmutableMap<String, String> config =
        ImmutableMap.of(
            "type", "hive",
            "default-namespace", "default",
            "cache-enabled", "true");
    spark
        .conf()
        .set("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.source.TestSparkCatalog");
    config.forEach(
        (key, value) -> spark.conf().set("spark.sql.catalog.spark_catalog." + key, value));
  }

  @Before
  public void setupTable() throws IOException {
    createAndInitTable();
  }

  @After
  public void dropTable() {
    TestTables.clearTables();
  }

  @Test
  public void testSpecAndPartitionMetadataColumns() {
    // TODO: support metadata structs in vectorized ORC reads
    Assume.assumeFalse(fileFormat == FileFormat.ORC && vectorized);

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", TABLE_NAME);

    table.refresh();
    table.updateSpec().addField("data").commit();
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", TABLE_NAME);

    table.refresh();
    table.updateSpec().addField(Expressions.bucket("category", 8)).commit();
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", TABLE_NAME);

    table.refresh();
    table.updateSpec().removeField("data").commit();
    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1')", TABLE_NAME);

    table.refresh();
    table.updateSpec().renameField("category_bucket_8", "category_bucket_8_another_name").commit();

    List<Object[]> expected =
        ImmutableList.of(
            row(0, row(null, null)),
            row(1, row("b1", null)),
            row(2, row("b1", 2)),
            row(3, row(null, 2)));
    assertEquals(
        "Rows must match",
        expected,
        sql("SELECT _spec_id, _partition FROM %s ORDER BY _spec_id", TABLE_NAME));
  }

  @Test
  public void testPartitionMetadataColumnWithUnknownTransforms() {
    // replace the table spec to include an unknown transform
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata base = ops.current();
    ops.commit(base, base.updatePartitionSpec(UNKNOWN_SPEC));

    AssertHelpers.assertThrows(
        "Should fail to query the partition metadata column",
        ValidationException.class,
        "Cannot build table partition type, unknown transforms",
        () -> sql("SELECT _partition FROM %s", TABLE_NAME));
  }

  @Test
  public void testConflictingColumns() {
    table
        .updateSchema()
        .addColumn(MetadataColumns.SPEC_ID.name(), Types.IntegerType.get())
        .addColumn(MetadataColumns.FILE_PATH.name(), Types.StringType.get())
        .commit();

    sql("INSERT INTO TABLE %s VALUES (1, 'a1', 'b1', -1, 'path/to/file')", TABLE_NAME);

    assertEquals(
        "Rows must match",
        ImmutableList.of(row(1L, "a1")),
        sql("SELECT id, category FROM %s", TABLE_NAME));

    AssertHelpers.assertThrows(
        "Should fail to query conflicting columns",
        ValidationException.class,
        "column names conflict",
        () -> sql("SELECT * FROM %s", TABLE_NAME));

    table.refresh();

    table
        .updateSchema()
        .renameColumn(MetadataColumns.SPEC_ID.name(), "_renamed" + MetadataColumns.SPEC_ID.name())
        .renameColumn(
            MetadataColumns.FILE_PATH.name(), "_renamed" + MetadataColumns.FILE_PATH.name())
        .commit();

    assertEquals(
        "Rows must match",
        ImmutableList.of(row(0, null, -1)),
        sql("SELECT _spec_id, _partition, _renamed_spec_id FROM %s", TABLE_NAME));
  }

  private void createAndInitTable() throws IOException {
    this.table =
        TestTables.create(temp.newFolder(), TABLE_NAME, SCHEMA, PartitionSpec.unpartitioned());

    UpdateProperties updateProperties = table.updateProperties();
    updateProperties.set(FORMAT_VERSION, String.valueOf(formatVersion));
    updateProperties.set(DEFAULT_FILE_FORMAT, fileFormat.name());

    switch (fileFormat) {
      case PARQUET:
        updateProperties.set(PARQUET_VECTORIZATION_ENABLED, String.valueOf(vectorized));
        break;
      case ORC:
        updateProperties.set(ORC_VECTORIZATION_ENABLED, String.valueOf(vectorized));
        break;
      default:
        Preconditions.checkState(
            !vectorized, "File format %s does not support vectorized reads", fileFormat);
    }

    updateProperties.commit();
  }
}
