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
package org.apache.iceberg.flink.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.util.Iterator;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.avro.DataReader;
import org.apache.iceberg.data.avro.DataWriter;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.DateTimeUtil;
import org.junit.jupiter.api.Test;

public class TestFlinkAvroReaderWriter extends DataTest {

  private static final int NUM_RECORDS = 100;

  private static final Schema SCHEMA_NUM_TYPE =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "int", Types.IntegerType.get()),
          Types.NestedField.optional(3, "float", Types.FloatType.get()),
          Types.NestedField.optional(4, "double", Types.DoubleType.get()),
          Types.NestedField.optional(5, "date", Types.DateType.get()),
          Types.NestedField.optional(6, "time", Types.TimeType.get()),
          Types.NestedField.optional(7, "timestamp", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(8, "bigint", Types.LongType.get()),
          Types.NestedField.optional(9, "decimal", Types.DecimalType.of(4, 2)));

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expectedRecords = RandomGenericData.generate(schema, NUM_RECORDS, 1991L);
    writeAndValidate(schema, expectedRecords, NUM_RECORDS);
  }

  private void writeAndValidate(Schema schema, List<Record> expectedRecords, int numRecord)
      throws IOException {
    RowType flinkSchema = FlinkSchemaUtil.convert(schema);
    List<RowData> expectedRows = Lists.newArrayList(RandomRowData.convert(schema, expectedRecords));

    File recordsFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(recordsFile.delete()).isTrue();

    // Write the expected records into AVRO file, then read them into RowData and assert with the
    // expected Record list.
    try (FileAppender<Record> writer =
        Avro.write(Files.localOutput(recordsFile))
            .schema(schema)
            .createWriterFunc(DataWriter::create)
            .build()) {
      writer.addAll(expectedRecords);
    }

    try (CloseableIterable<RowData> reader =
        Avro.read(Files.localInput(recordsFile))
            .project(schema)
            .createReaderFunc(FlinkAvroReader::new)
            .build()) {
      Iterator<Record> expected = expectedRecords.iterator();
      Iterator<RowData> rows = reader.iterator();
      for (int i = 0; i < numRecord; i++) {
        assertThat(rows).hasNext();
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, expected.next(), rows.next());
      }
      assertThat(rows).isExhausted();
    }

    File rowDataFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(rowDataFile.delete()).isTrue();

    // Write the expected RowData into AVRO file, then read them into Record and assert with the
    // expected RowData list.
    try (FileAppender<RowData> writer =
        Avro.write(Files.localOutput(rowDataFile))
            .schema(schema)
            .createWriterFunc(ignore -> new FlinkAvroWriter(flinkSchema))
            .build()) {
      writer.addAll(expectedRows);
    }

    try (CloseableIterable<Record> reader =
        Avro.read(Files.localInput(rowDataFile))
            .project(schema)
            .createReaderFunc(DataReader::create)
            .build()) {
      Iterator<RowData> expected = expectedRows.iterator();
      Iterator<Record> records = reader.iterator();
      for (int i = 0; i < numRecord; i += 1) {
        assertThat(records).hasNext();
        TestHelpers.assertRowData(schema.asStruct(), flinkSchema, records.next(), expected.next());
      }
      assertThat(records).isExhausted();
    }
  }

  private Record recordNumType(
      int id,
      int intV,
      float floatV,
      double doubleV,
      long date,
      long time,
      long timestamp,
      long bigint,
      double decimal) {
    Record record = GenericRecord.create(SCHEMA_NUM_TYPE);
    record.setField("id", id);
    record.setField("int", intV);
    record.setField("float", floatV);
    record.setField("double", doubleV);
    record.setField(
        "date", DateTimeUtil.dateFromDays((int) new Date(date).toLocalDate().toEpochDay()));
    record.setField("time", new Time(time).toLocalTime());
    record.setField("timestamp", DateTimeUtil.timestampFromMicros(timestamp * 1000));
    record.setField("bigint", bigint);
    record.setField("decimal", BigDecimal.valueOf(decimal));
    return record;
  }

  @Test
  public void testNumericTypes() throws IOException {

    List<Record> expected =
        ImmutableList.of(
            recordNumType(
                2,
                Integer.MAX_VALUE,
                Float.MAX_VALUE,
                Double.MAX_VALUE,
                Long.MAX_VALUE,
                1643811742000L,
                1643811742000L,
                1643811742000L,
                10.24d),
            recordNumType(
                2,
                Integer.MIN_VALUE,
                Float.MIN_VALUE,
                Double.MIN_VALUE,
                Long.MIN_VALUE,
                1643811742000L,
                1643811742000L,
                1643811742000L,
                10.24d));

    writeAndValidate(SCHEMA_NUM_TYPE, expected, 2);
  }
}
