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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMetadataTableReadableMetrics extends SparkTestBaseWithCatalog {

  @Rule public TemporaryFolder temp = new TemporaryFolder();

  private static final Types.StructType LEAF_STRUCT_TYPE =
      Types.StructType.of(
          optional(1, "leafLongCol", Types.LongType.get()),
          optional(2, "leafDoubleCol", Types.DoubleType.get()));

  private static final Types.StructType NESTED_STRUCT_TYPE =
      Types.StructType.of(required(3, "leafStructCol", LEAF_STRUCT_TYPE));

  private static final Schema NESTED_SCHEMA =
      new Schema(required(4, "nestedStructCol", NESTED_STRUCT_TYPE));

  private static final Schema PRIMITIVE_SCHEMA =
      new Schema(
          required(1, "booleanCol", Types.BooleanType.get()),
          required(2, "intCol", Types.IntegerType.get()),
          required(3, "longCol", Types.LongType.get()),
          required(4, "floatCol", Types.FloatType.get()),
          required(5, "doubleCol", Types.DoubleType.get()),
          optional(6, "decimalCol", Types.DecimalType.of(10, 2)),
          optional(7, "stringCol", Types.StringType.get()),
          optional(8, "fixedCol", Types.FixedType.ofLength(3)),
          optional(9, "binaryCol", Types.BinaryType.get()));

  public TestMetadataTableReadableMetrics() {
    // only SparkCatalog supports metadata table sql queries
    super(SparkCatalogConfig.HIVE);
  }

  protected String tableName() {
    return tableName.split("\\.")[2];
  }

  protected String database() {
    return tableName.split("\\.")[1];
  }

  private Table createPrimitiveTable() throws IOException {
    Table table =
        catalog.createTable(
            TableIdentifier.of(Namespace.of(database()), tableName()),
            PRIMITIVE_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of());
    List<Record> records =
        Lists.newArrayList(
            createPrimitiveRecord(
                false,
                1,
                1L,
                0,
                1.0D,
                new BigDecimal("1.00"),
                "1",
                Base64.getDecoder().decode("1111"),
                ByteBuffer.wrap(Base64.getDecoder().decode("1111"))),
            createPrimitiveRecord(
                true,
                2,
                2L,
                0,
                2.0D,
                new BigDecimal("2.00"),
                "2",
                Base64.getDecoder().decode("2222"),
                ByteBuffer.wrap(Base64.getDecoder().decode("2222"))),
            createPrimitiveRecord(false, 1, 1, Float.NaN, Double.NaN, null, "1", null, null),
            createPrimitiveRecord(
                false, 2, 2L, Float.NaN, 2.0D, new BigDecimal("2.00"), "2", null, null));

    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();
    return table;
  }

  private Pair<Table, DataFile> createNestedTable() throws IOException {
    Table table =
        catalog.createTable(
            TableIdentifier.of(Namespace.of(database()), tableName()),
            NESTED_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of());

    List<Record> records =
        Lists.newArrayList(
            createNestedRecord(0L, 0.0),
            createNestedRecord(1L, Double.NaN),
            createNestedRecord(null, null));
    DataFile dataFile =
        FileHelpers.writeDataFile(table, Files.localOutput(temp.newFile()), records);
    table.newAppend().appendFile(dataFile).commit();
    return Pair.of(table, dataFile);
  }

  @After
  public void dropTable() {
    sql("DROP TABLE %s", tableName);
  }

  private Dataset<Row> filesDf() {
    return spark.read().format("iceberg").load(database() + "." + tableName() + ".files");
  }

  protected GenericRecord createPrimitiveRecord(
      boolean booleanCol,
      int intCol,
      long longCol,
      float floatCol,
      double doubleCol,
      BigDecimal decimalCol,
      String stringCol,
      byte[] fixedCol,
      ByteBuffer binaryCol) {
    GenericRecord record = GenericRecord.create(PRIMITIVE_SCHEMA);
    record.set(0, booleanCol);
    record.set(1, intCol);
    record.set(2, longCol);
    record.set(3, floatCol);
    record.set(4, doubleCol);
    record.set(5, decimalCol);
    record.set(6, stringCol);
    record.set(7, fixedCol);
    record.set(8, binaryCol);
    return record;
  }

  private GenericRecord createNestedRecord(Long longCol, Double doubleCol) {
    GenericRecord record = GenericRecord.create(NESTED_SCHEMA);
    GenericRecord nested = GenericRecord.create(NESTED_STRUCT_TYPE);
    GenericRecord leaf = GenericRecord.create(LEAF_STRUCT_TYPE);
    leaf.set(0, longCol);
    leaf.set(1, doubleCol);
    nested.set(0, leaf);
    record.set(0, nested);
    return record;
  }

  @Test
  public void testPrimitiveColumns() throws Exception {
    Table table = createPrimitiveTable();
    DataFile dataFile = table.currentSnapshot().addedDataFiles(table.io()).iterator().next();
    Map<Integer, Long> columnSizeStats = dataFile.columnSizes();

    Object[] binaryCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("binaryCol").fieldId()),
            4L,
            2L,
            null,
            Base64.getDecoder().decode("1111"),
            Base64.getDecoder().decode("2222"));
    Object[] booleanCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("booleanCol").fieldId()),
            4L,
            0L,
            null,
            false,
            true);
    Object[] decimalCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("decimalCol").fieldId()),
            4L,
            1L,
            null,
            new BigDecimal("1.00"),
            new BigDecimal("2.00"));
    Object[] doubleCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("doubleCol").fieldId()),
            4L,
            0L,
            1L,
            1.0D,
            2.0D);
    Object[] fixedCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("fixedCol").fieldId()),
            4L,
            2L,
            null,
            Base64.getDecoder().decode("1111"),
            Base64.getDecoder().decode("2222"));
    Object[] floatCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("floatCol").fieldId()),
            4L,
            0L,
            2L,
            0f,
            0f);
    Object[] intCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("intCol").fieldId()),
            4L,
            0L,
            null,
            1,
            2);
    Object[] longCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("longCol").fieldId()),
            4L,
            0L,
            null,
            1L,
            2L);
    Object[] stringCol =
        row(
            columnSizeStats.get(PRIMITIVE_SCHEMA.findField("stringCol").fieldId()),
            4L,
            0L,
            null,
            "1",
            "2");

    Object[] metrics =
        row(
            binaryCol,
            booleanCol,
            decimalCol,
            doubleCol,
            fixedCol,
            floatCol,
            intCol,
            longCol,
            stringCol);

    assertEquals(
        "Row should match",
        ImmutableList.of(new Object[] {metrics}),
        sql("SELECT readable_metrics FROM %s.files", tableName));
  }

  @Test
  public void testSelectPrimitiveValues() throws Exception {
    createPrimitiveTable();

    assertEquals(
        "select of primitive readable_metrics fields should work",
        ImmutableList.of(row(1, true)),
        sql(
            "SELECT readable_metrics.intCol.lower_bound, readable_metrics.booleanCol.upper_bound FROM %s.files",
            tableName));

    assertEquals(
        "mixed select of readable_metrics and other field should work",
        ImmutableList.of(row(0, 4L)),
        sql("SELECT content, readable_metrics.longCol.value_count FROM %s.files", tableName));

    assertEquals(
        "mixed select of readable_metrics and other field should work, in the other order",
        ImmutableList.of(row(4L, 0)),
        sql("SELECT readable_metrics.longCol.value_count, content FROM %s.files", tableName));
  }

  @Test
  public void testSelectNestedValues() throws Exception {
    createNestedTable();

    assertEquals(
        "select of nested readable_metrics fields should work",
        ImmutableList.of(row(0L, 3L)),
        sql(
            "SELECT readable_metrics.`nestedStructCol.leafStructCol.leafLongCol`.lower_bound, "
                + "readable_metrics.`nestedStructCol.leafStructCol.leafDoubleCol`.value_count FROM %s.files",
            tableName));
  }

  @Test
  public void testNestedValues() throws Exception {
    Pair<Table, DataFile> table = createNestedTable();
    int longColId =
        table.first().schema().findField("nestedStructCol.leafStructCol.leafLongCol").fieldId();
    int doubleColId =
        table.first().schema().findField("nestedStructCol.leafStructCol.leafDoubleCol").fieldId();

    Object[] leafDoubleCol =
        row(table.second().columnSizes().get(doubleColId), 3L, 1L, 1L, 0.0D, 0.0D);
    Object[] leafLongCol = row(table.second().columnSizes().get(longColId), 3L, 1L, null, 0L, 1L);
    Object[] metrics = row(leafDoubleCol, leafLongCol);

    List<Object[]> expected = ImmutableList.of(new Object[] {metrics});
    String sql = "SELECT readable_metrics FROM %s.%s";
    List<Object[]> filesReadableMetrics = sql(String.format(sql, tableName, "files"));
    List<Object[]> entriesReadableMetrics = sql(String.format(sql, tableName, "entries"));
    assertEquals("Row should match for files table", expected, filesReadableMetrics);
    assertEquals("Row should match for entries table", expected, entriesReadableMetrics);
  }
}
