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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.flink.types.Row;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericAppenderHelper;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

/**
 * Verifies Flink UUID filters with current and legacy Parquet UUID bounds. The legacy case rewrites
 * manifest lower/upper bounds to mimic files written with signed UUID ordering, while the data file
 * itself stays unchanged.
 */
final class UuidFlinkScanTestHelpers {
  private static final int UUID_FIELD_ID = 2;

  private static final UUID UUID_00 = UUID.fromString("00000000-0000-0000-0000-000000000001");
  private static final UUID UUID_20 = UUID.fromString("20000000-0000-0000-0000-000000000001");
  private static final UUID UUID_30 = UUID.fromString("30000000-0000-0000-0000-000000000001");
  private static final UUID UUID_40 = UUID.fromString("40000000-0000-0000-0000-000000000001");
  private static final UUID UUID_7F = UUID.fromString("7fffffff-ffff-ffff-ffff-ffffffffffff");
  private static final UUID UUID_80 = UUID.fromString("80000000-0000-0000-0000-000000000001");
  private static final UUID UUID_FF = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.required(UUID_FIELD_ID, "uuid_col", Types.UUIDType.get()),
          Types.NestedField.required(3, "label", Types.StringType.get()));

  private UuidFlinkScanTestHelpers() {}

  static void testUuidFiltersWithLegacySignedParquetMetrics(TestFlinkScan scan) throws Exception {
    assumeThat(scan.fileFormat).isEqualTo(FileFormat.PARQUET);
    assumeThat(scan).isNotInstanceOf(TestFlinkScanSql.class);
    assumeThat(scan).isNotInstanceOf(TestIcebergSourceBoundedSql.class);

    Table table =
        TestFlinkScan.CATALOG_EXTENSION
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    List<Record> records = records();
    DataFile file = writeParquetFileWithSignedUuidMetrics(scan, table, records);

    assertBounds(file, UUID_80, UUID_7F);

    new GenericAppenderHelper(table, FileFormat.PARQUET, scan.temporaryDirectory)
        .appendToTable(file);

    assertUuidFilters(scan, records);
  }

  static void testUuidFiltersWithUnsignedParquetMetrics(TestFlinkScan scan) throws Exception {
    assumeThat(scan.fileFormat).isEqualTo(FileFormat.PARQUET);
    assumeThat(scan).isNotInstanceOf(TestFlinkScanSql.class);
    assumeThat(scan).isNotInstanceOf(TestIcebergSourceBoundedSql.class);

    Table table =
        TestFlinkScan.CATALOG_EXTENSION
            .catalog()
            .createTable(TestFixtures.TABLE_IDENTIFIER, SCHEMA);
    List<Record> records = records();
    DataFile file =
        new GenericAppenderHelper(table, FileFormat.PARQUET, scan.temporaryDirectory)
            .writeFile(records);

    assertBounds(file, UUID_00, UUID_FF);

    new GenericAppenderHelper(table, FileFormat.PARQUET, scan.temporaryDirectory)
        .appendToTable(file);

    assertUuidFilters(scan, records);
  }

  private static DataFile writeParquetFileWithSignedUuidMetrics(
      TestFlinkScan scan, Table table, List<Record> records) throws IOException {
    DataFile file =
        new GenericAppenderHelper(table, FileFormat.PARQUET, scan.temporaryDirectory)
            .writeFile(records);

    Map<Integer, ByteBuffer> legacyLowerBounds = Maps.newHashMap(file.lowerBounds());
    legacyLowerBounds.put(UUID_FIELD_ID, toByteBuffer(UUID_80));
    Map<Integer, ByteBuffer> legacyUpperBounds = Maps.newHashMap(file.upperBounds());
    legacyUpperBounds.put(UUID_FIELD_ID, toByteBuffer(UUID_7F));

    Metrics legacyMetrics =
        new Metrics(
            file.recordCount(),
            file.columnSizes(),
            file.valueCounts(),
            file.nullValueCounts(),
            file.nanValueCounts(),
            legacyLowerBounds,
            legacyUpperBounds);

    return DataFiles.builder(table.spec()).copy(file).withMetrics(legacyMetrics).build();
  }

  private static void assertUuidFilters(TestFlinkScan scan, List<Record> records) throws Exception {
    assertFilter(scan, Expressions.equal("uuid_col", UUID_20), records.get(3));
    assertFilter(scan, Expressions.lessThan("uuid_col", UUID_20), records.get(2));
    assertFilter(
        scan, Expressions.lessThanOrEqual("uuid_col", UUID_20), records.get(2), records.get(3));
    assertFilter(
        scan,
        Expressions.greaterThan("uuid_col", UUID_20),
        records.get(0),
        records.get(1),
        records.get(4),
        records.get(5),
        records.get(6));
    assertFilter(
        scan, Expressions.greaterThanOrEqual("uuid_col", UUID_80), records.get(0), records.get(1));
    assertFilter(
        scan,
        Expressions.in("uuid_col", UUID_20, UUID_30, UUID_FF),
        records.get(1),
        records.get(3),
        records.get(4));
    assertFilter(
        scan,
        Expressions.notIn("uuid_col", UUID_00, UUID_40, UUID_FF),
        records.get(0),
        records.get(3),
        records.get(4),
        records.get(6));
  }

  private static void assertFilter(TestFlinkScan scan, Expression filter, Record... expected)
      throws Exception {
    List<Row> actual = scan.runWithFilter(filter, "");
    TestHelpers.assertRecords(actual, ImmutableList.copyOf(expected), SCHEMA);
  }

  private static void assertBounds(DataFile file, UUID expectedLower, UUID expectedUpper) {
    assertThat(file.lowerBounds()).containsKey(UUID_FIELD_ID);
    assertThat(file.upperBounds()).containsKey(UUID_FIELD_ID);

    assertThat(uuid(file.lowerBounds().get(UUID_FIELD_ID))).isEqualTo(expectedLower);
    assertThat(uuid(file.upperBounds().get(UUID_FIELD_ID))).isEqualTo(expectedUpper);
  }

  private static UUID uuid(ByteBuffer buffer) {
    return Conversions.fromByteBuffer(Types.UUIDType.get(), buffer.duplicate());
  }

  private static ByteBuffer toByteBuffer(UUID value) {
    return Conversions.toByteBuffer(Types.UUIDType.get(), value);
  }

  private static List<Record> records() {
    return ImmutableList.of(
        record(1, UUID_80, "uuid-80"),
        record(2, UUID_FF, "uuid-ff"),
        record(3, UUID_00, "uuid-00"),
        record(4, UUID_20, "uuid-20"),
        record(5, UUID_30, "uuid-30"),
        record(6, UUID_40, "uuid-40"),
        record(7, UUID_7F, "uuid-7f"));
  }

  private static Record record(int id, UUID uuid, String label) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("uuid_col", uuid);
    record.setField("label", label);
    return record;
  }
}
