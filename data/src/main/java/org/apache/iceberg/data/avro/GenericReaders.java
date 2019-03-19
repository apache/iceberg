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

package com.netflix.iceberg.data.avro;

import com.netflix.iceberg.avro.ValueReader;
import com.netflix.iceberg.avro.ValueReaders;
import com.netflix.iceberg.data.GenericRecord;
import com.netflix.iceberg.data.Record;
import com.netflix.iceberg.types.Types.StructType;
import org.apache.avro.io.Decoder;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;

class GenericReaders {
  private GenericReaders() {
  }

  static ValueReader<LocalDate> dates() {
    return DateReader.INSTANCE;
  }

  static ValueReader<LocalTime> times() {
    return TimeReader.INSTANCE;
  }

  static ValueReader<LocalDateTime> timestamps() {
    return TimestampReader.INSTANCE;
  }

  static ValueReader<OffsetDateTime> timestamptz() {
    return TimestamptzReader.INSTANCE;
  }

  static ValueReader<Record> struct(StructType struct, List<ValueReader<?>> readers) {
    return new GenericRecordReader(readers, struct);
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateReader implements ValueReader<LocalDate> {
    private static final DateReader INSTANCE = new DateReader();

    private DateReader() {
    }

    @Override
    public LocalDate read(Decoder decoder, Object reuse) throws IOException {
      return EPOCH_DAY.plusDays(decoder.readInt());
    }
  }

  private static class TimeReader implements ValueReader<LocalTime> {
    private static final TimeReader INSTANCE = new TimeReader();

    private TimeReader() {
    }

    @Override
    public LocalTime read(Decoder decoder, Object reuse) throws IOException {
      return LocalTime.ofNanoOfDay(decoder.readLong() * 1000);
    }
  }

  private static class TimestampReader implements ValueReader<LocalDateTime> {
    private static final TimestampReader INSTANCE = new TimestampReader();

    private TimestampReader() {
    }

    @Override
    public LocalDateTime read(Decoder decoder, Object reuse) throws IOException {
      return EPOCH.plus(decoder.readLong(), ChronoUnit.MICROS).toLocalDateTime();
    }
  }

  private static class TimestamptzReader implements ValueReader<OffsetDateTime> {
    private static final TimestamptzReader INSTANCE = new TimestamptzReader();

    private TimestamptzReader() {
    }

    @Override
    public OffsetDateTime read(Decoder decoder, Object reuse) throws IOException {
      return EPOCH.plus(decoder.readLong(), ChronoUnit.MICROS);
    }
  }

  private static class GenericRecordReader extends ValueReaders.StructReader<Record> {
    private final StructType struct;

    private GenericRecordReader(List<ValueReader<?>> readers, StructType struct) {
      super(readers);
      this.struct = struct;
    }

    @Override
    protected Record reuseOrCreate(Object reuse) {
      if (reuse instanceof Record) {
        return (Record) reuse;
      } else {
        return GenericRecord.create(struct);
      }
    }

    @Override
    protected Object get(Record struct, int pos) {
      return struct.get(pos);
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }
}
