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
package org.apache.iceberg.flink.source;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.flink.CatalogTestBase;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.io.TempDir;

public class TestMetadataTableReadableMetrics extends CatalogTestBase {
  private static final String TABLE_NAME = "test_table";

  @Parameters(name = "catalogName={0}, baseNamespace={1}")
  protected static List<Object[]> parameters() {
    List<Object[]> parameters = Lists.newArrayList();
    String catalogName = "testhive";
    Namespace baseNamespace = Namespace.empty();
    parameters.add(new Object[] {catalogName, baseNamespace});
    return parameters;
  }

  @Override
  protected TableEnvironment getTableEnv() {
    Configuration configuration = super.getTableEnv().getConfig().getConfiguration();
    configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
    return super.getTableEnv();
  }

  @TempDir Path temp;

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

  private Table createPrimitiveTable() throws IOException {
    Table table =
        catalog.createTable(
            TableIdentifier.of(DATABASE, TABLE_NAME),
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

    DataFile dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.toFile()), records);
    table.newAppend().appendFile(dataFile).commit();
    return table;
  }

  private void createNestedTable() throws IOException {
    Table table =
        validationCatalog.createTable(
            TableIdentifier.of(DATABASE, TABLE_NAME),
            NESTED_SCHEMA,
            PartitionSpec.unpartitioned(),
            ImmutableMap.of());

    List<Record> records =
        Lists.newArrayList(
            createNestedRecord(0L, 0.0),
            createNestedRecord(1L, Double.NaN),
            createNestedRecord(null, null));
    DataFile dataFile = FileHelpers.writeDataFile(table, Files.localOutput(temp.toFile()), records);
    table.newAppend().appendFile(dataFile).commit();
  }

  @BeforeEach
  public void before() {
    super.before();
    sql("USE CATALOG %s", catalogName);
    sql("CREATE DATABASE %s", DATABASE);
    sql("USE %s", DATABASE);
  }

  @Override
  @AfterEach
  public void clean() {
    sql("DROP TABLE IF EXISTS %s.%s", flinkDatabase, TABLE_NAME);
    sql("DROP DATABASE IF EXISTS %s", flinkDatabase);
    super.clean();
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

  protected Object[] row(Object... values) {
    return values;
  }

  @TestTemplate
  public void testPrimitiveColumns() throws Exception {
    createPrimitiveTable();
    List<Row> result = sql("SELECT readable_metrics FROM %s$files", TABLE_NAME);

    Row binaryCol =
        Row.of(
            52L,
            4L,
            2L,
            null,
            Base64.getDecoder().decode("1111"),
            Base64.getDecoder().decode("2222"));
    Row booleanCol = Row.of(32L, 4L, 0L, null, false, true);
    Row decimalCol = Row.of(85L, 4L, 1L, null, new BigDecimal("1.00"), new BigDecimal("2.00"));
    Row doubleCol = Row.of(85L, 4L, 0L, 1L, 1.0D, 2.0D);
    Row fixedCol =
        Row.of(
            44L,
            4L,
            2L,
            null,
            Base64.getDecoder().decode("1111"),
            Base64.getDecoder().decode("2222"));
    Row floatCol = Row.of(71L, 4L, 0L, 2L, 0f, 0f);
    Row intCol = Row.of(71L, 4L, 0L, null, 1, 2);
    Row longCol = Row.of(79L, 4L, 0L, null, 1L, 2L);
    Row stringCol = Row.of(79L, 4L, 0L, null, "1", "2");

    List<Row> expected =
        Lists.newArrayList(
            Row.of(
                Row.of(
                    binaryCol,
                    booleanCol,
                    decimalCol,
                    doubleCol,
                    fixedCol,
                    floatCol,
                    intCol,
                    longCol,
                    stringCol)));
    TestHelpers.assertRows(result, expected);
  }

  @TestTemplate
  public void testSelectPrimitiveValues() throws Exception {
    createPrimitiveTable();

    TestHelpers.assertRows(
        sql(
            "SELECT readable_metrics.intCol.lower_bound, readable_metrics.booleanCol.upper_bound FROM %s$files",
            TABLE_NAME),
        ImmutableList.of(Row.of(1, true)));

    TestHelpers.assertRows(
        sql("SELECT content, readable_metrics.longCol.value_count FROM %s$files", TABLE_NAME),
        ImmutableList.of(Row.of(0, 4L)));

    TestHelpers.assertRows(
        sql("SELECT readable_metrics.longCol.value_count, content FROM %s$files", TABLE_NAME),
        ImmutableList.of(Row.of(4L, 0)));
  }

  @TestTemplate
  public void testSelectNestedValues() throws Exception {
    createNestedTable();
    TestHelpers.assertRows(
        sql(
            "SELECT readable_metrics.`nestedStructCol.leafStructCol.leafLongCol`.lower_bound, "
                + "readable_metrics.`nestedStructCol.leafStructCol.leafDoubleCol`.value_count FROM %s$files",
            TABLE_NAME),
        ImmutableList.of(Row.of(0L, 3L)));
  }

  @TestTemplate
  public void testNestedValues() throws Exception {
    createNestedTable();

    Row leafDoubleCol = Row.of(46L, 3L, 1L, 1L, 0.0D, 0.0D);
    Row leafLongCol = Row.of(54L, 3L, 1L, null, 0L, 1L);
    Row metrics = Row.of(Row.of(leafDoubleCol, leafLongCol));

    TestHelpers.assertRows(
        sql("SELECT readable_metrics FROM %s$files", TABLE_NAME), ImmutableList.of(metrics));
  }
}
