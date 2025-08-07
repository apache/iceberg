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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDataUtil;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

public class GenericParquetReaders extends BaseParquetReaders<Record> {

  private static final GenericParquetReaders INSTANCE = new GenericParquetReaders();

  private GenericParquetReaders() {}

  public static ParquetValueReader<Record> buildReader(
      Schema expectedSchema, MessageType fileSchema) {
    return INSTANCE.createReader(expectedSchema, fileSchema);
  }

  public static ParquetValueReader<Record> buildReader(
      Schema expectedSchema, MessageType fileSchema, Map<Integer, ?> idToConstant) {
    return INSTANCE.createReader(expectedSchema, fileSchema, idToConstant);
  }

  @Override
  protected ParquetValueReader<Record> createStructReader(
      List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    return ParquetValueReaders.recordReader(fieldReaders, structType);
  }

  @Override
  protected ParquetValueReader<?> fixedReader(ColumnDescriptor desc) {
    return new GenericParquetReaders.FixedReader(desc);
  }

  @Override
  protected ParquetValueReader<?> dateReader(ColumnDescriptor desc) {
    return new GenericParquetReaders.DateReader(desc);
  }

  @Override
  protected ParquetValueReader<?> timeReader(ColumnDescriptor desc) {
    LogicalTypeAnnotation time = desc.getPrimitiveType().getLogicalTypeAnnotation();
    Preconditions.checkArgument(
        time instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation,
        "Invalid time logical type: " + time);

    LogicalTypeAnnotation.TimeUnit unit =
        ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) time).getUnit();
    switch (unit) {
      case MICROS:
        return new GenericParquetReaders.TimeReader(desc);
      case MILLIS:
        return new GenericParquetReaders.TimeMillisReader(desc);
      default:
        throw new UnsupportedOperationException("Unsupported unit for time: " + unit);
    }
  }

  @Override
  protected ParquetValueReader<?> timestampReader(ColumnDescriptor desc, boolean isAdjustedToUTC) {
    if (desc.getPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT96) {
      return new GenericParquetReaders.TimestampInt96Reader(desc);
    }

    LogicalTypeAnnotation timestamp = desc.getPrimitiveType().getLogicalTypeAnnotation();
    Preconditions.checkArgument(
        timestamp instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation,
        "Invalid timestamp logical type: " + timestamp);

    LogicalTypeAnnotation.TimeUnit unit =
        ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) timestamp).getUnit();
    switch (unit) {
      case NANOS:
        return isAdjustedToUTC
            ? new GenericParquetReaders.TimestamptzReader(desc, ChronoUnit.NANOS)
            : new GenericParquetReaders.TimestampReader(desc, ChronoUnit.NANOS);
      case MICROS:
        return isAdjustedToUTC
            ? new GenericParquetReaders.TimestamptzReader(desc, ChronoUnit.MICROS)
            : new GenericParquetReaders.TimestampReader(desc, ChronoUnit.MICROS);
      case MILLIS:
        return isAdjustedToUTC
            ? new GenericParquetReaders.TimestamptzMillisReader(desc)
            : new GenericParquetReaders.TimestampMillisReader(desc);
      default:
        throw new UnsupportedOperationException("Unsupported unit for timestamp: " + unit);
    }
  }

  @Override
  protected Object convertConstant(org.apache.iceberg.types.Type type, Object value) {
    return GenericDataUtil.internalToGeneric(type, value);
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateReader extends ParquetValueReaders.PrimitiveReader<LocalDate> {
    DateReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDate read(LocalDate reuse) {
      return EPOCH_DAY.plusDays(column.nextInteger());
    }
  }

  private static class TimestampReader extends ParquetValueReaders.PrimitiveReader<LocalDateTime> {
    private final ChronoUnit unit;

    TimestampReader(ColumnDescriptor desc, ChronoUnit unit) {
      super(desc);
      this.unit = unit;
    }

    @Override
    public LocalDateTime read(LocalDateTime reuse) {
      return EPOCH.plus(column.nextLong(), unit).toLocalDateTime();
    }
  }

  private static class TimestampMillisReader
      extends ParquetValueReaders.PrimitiveReader<LocalDateTime> {
    TimestampMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDateTime read(LocalDateTime reuse) {
      return EPOCH.plus(column.nextLong() * 1000, ChronoUnit.MICROS).toLocalDateTime();
    }
  }

  private static class TimestampInt96Reader
      extends ParquetValueReaders.PrimitiveReader<OffsetDateTime> {
    private static final long UNIX_EPOCH_JULIAN = 2_440_588L;

    TimestampInt96Reader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      final ByteBuffer byteBuffer =
          column.nextBinary().toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      final long timeOfDayNanos = byteBuffer.getLong();
      final int julianDay = byteBuffer.getInt();

      return Instant.ofEpochMilli(TimeUnit.DAYS.toMillis(julianDay - UNIX_EPOCH_JULIAN))
          .plusNanos(timeOfDayNanos)
          .atOffset(ZoneOffset.UTC);
    }
  }

  private static class TimestamptzReader
      extends ParquetValueReaders.PrimitiveReader<OffsetDateTime> {
    private final ChronoUnit unit;

    TimestamptzReader(ColumnDescriptor desc, ChronoUnit unit) {
      super(desc);
      this.unit = unit;
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong(), unit);
    }
  }

  private static class TimestamptzMillisReader
      extends ParquetValueReaders.PrimitiveReader<OffsetDateTime> {
    TimestamptzMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong() * 1000, ChronoUnit.MICROS);
    }
  }

  private static class TimeMillisReader extends ParquetValueReaders.PrimitiveReader<LocalTime> {
    TimeMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalTime read(LocalTime reuse) {
      return LocalTime.ofNanoOfDay(column.nextInteger() * 1000000L);
    }
  }

  private static class TimeReader extends ParquetValueReaders.PrimitiveReader<LocalTime> {
    TimeReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalTime read(LocalTime reuse) {
      return LocalTime.ofNanoOfDay(column.nextLong() * 1000L);
    }
  }

  private static class FixedReader extends ParquetValueReaders.PrimitiveReader<byte[]> {
    FixedReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public byte[] read(byte[] reuse) {
      if (reuse != null) {
        column.nextBinary().toByteBuffer().duplicate().get(reuse);
        return reuse;
      } else {
        return column.nextBinary().getBytes();
      }
    }
  }
}
