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
package org.apache.iceberg.data.avro;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import org.apache.avro.io.Encoder;
import org.apache.iceberg.avro.ValueWriter;
import org.apache.iceberg.avro.ValueWriters;
import org.apache.iceberg.data.Record;

class GenericWriters {
  private GenericWriters() {}

  static ValueWriter<LocalDate> dates() {
    return DateWriter.INSTANCE;
  }

  static ValueWriter<LocalTime> times() {
    return TimeWriter.INSTANCE;
  }

  static ValueWriter<LocalDateTime> timestamps() {
    return TimestampWriter.INSTANCE;
  }

  static ValueWriter<OffsetDateTime> timestamptz() {
    return TimestamptzWriter.INSTANCE;
  }

  static ValueWriter<Record> struct(List<ValueWriter<?>> writers) {
    return new GenericRecordWriter(writers);
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateWriter implements ValueWriter<LocalDate> {
    private static final DateWriter INSTANCE = new DateWriter();

    private DateWriter() {}

    @Override
    public void write(LocalDate date, Encoder encoder) throws IOException {
      encoder.writeInt((int) ChronoUnit.DAYS.between(EPOCH_DAY, date));
    }
  }

  private static class TimeWriter implements ValueWriter<LocalTime> {
    private static final TimeWriter INSTANCE = new TimeWriter();

    private TimeWriter() {}

    @Override
    public void write(LocalTime time, Encoder encoder) throws IOException {
      encoder.writeLong(time.toNanoOfDay() / 1000);
    }
  }

  private static class TimestampWriter implements ValueWriter<LocalDateTime> {
    private static final TimestampWriter INSTANCE = new TimestampWriter();

    private TimestampWriter() {}

    @Override
    public void write(LocalDateTime timestamp, Encoder encoder) throws IOException {
      encoder.writeLong(ChronoUnit.MICROS.between(EPOCH, timestamp.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class TimestamptzWriter implements ValueWriter<OffsetDateTime> {
    private static final TimestamptzWriter INSTANCE = new TimestamptzWriter();

    private TimestamptzWriter() {}

    @Override
    public void write(OffsetDateTime timestamptz, Encoder encoder) throws IOException {
      encoder.writeLong(ChronoUnit.MICROS.between(EPOCH, timestamptz));
    }
  }

  private static class GenericRecordWriter extends ValueWriters.StructWriter<Record> {
    private GenericRecordWriter(List<ValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int pos) {
      return struct.get(pos);
    }
  }
}
