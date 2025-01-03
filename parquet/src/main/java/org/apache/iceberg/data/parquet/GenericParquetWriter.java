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
package org.apache.iceberg.data.parquet;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueWriter;
import org.apache.iceberg.parquet.ParquetValueWriters;
import org.apache.iceberg.parquet.ParquetValueWriters.StructWriter;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;

public class GenericParquetWriter extends BaseParquetWriter<Record> {
  private static final GenericParquetWriter INSTANCE = new GenericParquetWriter();

  private GenericParquetWriter() {}

  public static ParquetValueWriter<Record> buildWriter(MessageType type) {
    return INSTANCE.createWriter(type);
  }

  @Override
  protected StructWriter<Record> createStructWriter(List<ParquetValueWriter<?>> writers) {
    return new RecordWriter(writers);
  }

  @Override
  protected Optional<ParquetValueWriters.PrimitiveWriter<?>> dateWriter(ColumnDescriptor desc) {
    return Optional.of(new DateWriter(desc));
  }

  @Override
  protected Optional<ParquetValueWriters.PrimitiveWriter<?>> timeWriter(ColumnDescriptor desc) {
    return Optional.of(new TimeWriter(desc));
  }

  @Override
  protected Optional<ParquetValueWriters.PrimitiveWriter<?>> timestampWriter(
      ColumnDescriptor desc, boolean isAdjustedToUTC) {
    if (isAdjustedToUTC) {
      return Optional.of(new TimestamptzWriter(desc));
    } else {
      return Optional.of(new TimestampWriter(desc));
    }
  }

  private static class RecordWriter extends StructWriter<Record> {
    private RecordWriter(List<ParquetValueWriter<?>> writers) {
      super(writers);
    }

    @Override
    protected Object get(Record struct, int index) {
      return struct.get(index);
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateWriter extends ParquetValueWriters.PrimitiveWriter<LocalDate> {
    private DateWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDate value) {
      column.writeInteger(repetitionLevel, (int) ChronoUnit.DAYS.between(EPOCH_DAY, value));
    }
  }

  private static class TimeWriter extends ParquetValueWriters.PrimitiveWriter<LocalTime> {
    private TimeWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalTime value) {
      column.writeLong(repetitionLevel, value.toNanoOfDay() / 1000);
    }
  }

  private static class TimestampWriter extends ParquetValueWriters.PrimitiveWriter<LocalDateTime> {
    private TimestampWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, LocalDateTime value) {
      column.writeLong(
          repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value.atOffset(ZoneOffset.UTC)));
    }
  }

  private static class TimestamptzWriter
      extends ParquetValueWriters.PrimitiveWriter<OffsetDateTime> {
    private TimestamptzWriter(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public void write(int repetitionLevel, OffsetDateTime value) {
      column.writeLong(repetitionLevel, ChronoUnit.MICROS.between(EPOCH, value));
    }
  }
}
