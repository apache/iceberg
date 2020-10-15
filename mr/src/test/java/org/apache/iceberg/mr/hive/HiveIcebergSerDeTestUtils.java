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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;
import org.junit.Assert;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class HiveIcebergSerDeTestUtils {
  // TODO: Can this be a constant all around the Iceberg tests?
  public static final Schema FULL_SCHEMA = new Schema(
      optional(1, "boolean_type", Types.BooleanType.get()),
      optional(2, "integer_type", Types.IntegerType.get()),
      optional(3, "long_type", Types.LongType.get()),
      optional(4, "float_type", Types.FloatType.get()),
      optional(5, "double_type", Types.DoubleType.get()),
      optional(6, "date_type", Types.DateType.get()),
      // TimeType is not supported
      // required(7, "time_type", Types.TimeType.get()),
      optional(7, "tsTz", Types.TimestampType.withZone()),
      optional(8, "ts", Types.TimestampType.withoutZone()),
      optional(9, "string_type", Types.StringType.get()),
      optional(10, "uuid_type", Types.UUIDType.get()),
      optional(11, "fixed_type", Types.FixedType.ofLength(3)),
      optional(12, "binary_type", Types.BinaryType.get()),
      optional(13, "decimal_type", Types.DecimalType.of(38, 10)));

  private HiveIcebergSerDeTestUtils() {
    // Empty constructor for the utility class
  }

  public static Record getTestRecord(boolean uuidAsByte) {
    Record record = GenericRecord.create(HiveIcebergSerDeTestUtils.FULL_SCHEMA);
    record.set(0, true);
    record.set(1, 1);
    record.set(2, 2L);
    record.set(3, 3.1f);
    record.set(4, 4.2d);
    record.set(5, LocalDate.of(2020, 1, 21));
    // TimeType is not supported
    // record.set(6, LocalTime.of(11, 33));
    // Nano is not supported
    record.set(6, OffsetDateTime.of(2017, 11, 22, 11, 30, 7, 0, ZoneOffset.ofHours(2)));
    record.set(7, LocalDateTime.of(2019, 2, 22, 9, 44, 54));
    record.set(8, "kilenc");
    if (uuidAsByte) {
      // TODO: Parquet UUID expect byte[], others are expecting UUID
      record.set(9, UUIDUtil.convert(UUID.fromString("1-2-3-4-5")));
    } else {
      record.set(9, UUID.fromString("1-2-3-4-5"));
    }
    record.set(10, new byte[] {0, 1, 2});
    record.set(11, ByteBuffer.wrap(new byte[] {0, 1, 2, 3}));
    record.set(12, new BigDecimal("0.0000000013"));

    return record;
  }

  public static Record getNullTestRecord() {
    Record record = GenericRecord.create(HiveIcebergSerDeTestUtils.FULL_SCHEMA);

    for (int i = 0; i < HiveIcebergSerDeTestUtils.FULL_SCHEMA.columns().size(); i++) {
      record.set(i, null);
    }

    return record;
  }

  public static void assertEquals(Record expected, Record actual) {
    for (int i = 0; i < expected.size(); ++i) {
      if (expected.get(i) instanceof OffsetDateTime) {
        // For OffsetDateTime we just compare the actual instant
        Assert.assertEquals(((OffsetDateTime) expected.get(i)).toInstant(),
            ((OffsetDateTime) actual.get(i)).toInstant());
      } else {
        if (expected.get(i) instanceof byte[]) {
          Assert.assertArrayEquals((byte[]) expected.get(i), (byte[]) actual.get(i));
        } else {
          Assert.assertEquals(expected.get(i), actual.get(i));
        }
      }
    }
  }

  public static void validate(Table table, List<Record> expected, Integer sortBy) throws IOException {
    // Refresh the table, so we get the new data as well
    table.refresh();
    List<Record> records = new ArrayList<>();
    try (CloseableIterable<Record> iterable = IcebergGenerics.read(table).build()) {
      iterable.forEach(records::add);
    }

    // Sort if needed
    if (sortBy != null) {
      expected.sort(Comparator.comparingLong(record -> ((Long) record.get(sortBy.intValue())).longValue()));
      records.sort(Comparator.comparingLong(record -> ((Long) record.get(sortBy.intValue())).longValue()));
    }
    Assert.assertEquals(expected.size(), records.size());
    for (int i = 0; i < expected.size(); ++i) {
      HiveIcebergSerDeTestUtils.assertEquals(expected.get(i), records.get(i));
    }
  }
}
