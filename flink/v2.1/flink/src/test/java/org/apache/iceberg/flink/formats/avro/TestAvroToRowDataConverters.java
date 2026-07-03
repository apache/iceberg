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
package org.apache.iceberg.flink.formats.avro;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.junit.jupiter.api.Test;

public class TestAvroToRowDataConverters {

  private static final long MICROS_PER_MILLI = 1000L;

  // Avro carries timestamps as a plain long; the converter derives precision from the RowType.
  private static final Schema RECORD_SCHEMA =
      SchemaBuilder.record("test").fields().name("ts").type().longType().noDefault().endRecord();

  private static RowData convertMicros(long micros) {
    RowType rowType = RowType.of(new TimestampType(6));
    GenericRecord record = new GenericData.Record(RECORD_SCHEMA);
    record.put(0, micros);
    return (RowData) AvroToRowDataConverters.createRowConverter(rowType).convert(record);
  }

  @Test
  public void microsWithSubMillisecondComponent() {
    // micros value with a 123-microsecond leftover that is not aligned to a whole millisecond.
    long micros = 1_577_966_400_000_000L + 123L;

    RowData row = convertMicros(micros);

    long expectedMillis = Math.floorDiv(micros, MICROS_PER_MILLI);
    int expectedNanos = (int) Math.floorMod(micros, MICROS_PER_MILLI) * 1000;
    assertThat(row.getTimestamp(0, 6))
        .isEqualTo(TimestampData.fromEpochMillis(expectedMillis, expectedNanos));
  }

  @Test
  public void microsAlignedToMillisecond() {
    // micros value aligned to a whole millisecond (no sub-millisecond component).
    long micros = 1_577_966_400_000_000L;

    RowData row = convertMicros(micros);

    assertThat(row.getTimestamp(0, 6))
        .isEqualTo(TimestampData.fromEpochMillis(Math.floorDiv(micros, MICROS_PER_MILLI)));
  }
}
