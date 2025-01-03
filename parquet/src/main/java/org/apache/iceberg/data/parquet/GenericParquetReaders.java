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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericDataUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.parquet.ParquetValueReader;
import org.apache.iceberg.parquet.ParquetValueReaders;
import org.apache.iceberg.parquet.ParquetValueReaders.StructReader;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.StructType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

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
      List<Type> types, List<ParquetValueReader<?>> fieldReaders, StructType structType) {
    return new RecordReader(types, fieldReaders, structType);
  }

  @Override
  protected LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ParquetValueReader<?>>
      logicalTypeReaderVisitor(
          ColumnDescriptor desc,
          org.apache.iceberg.types.Type.PrimitiveType expected,
          PrimitiveType primitive) {
    return new LogicalTypeAnnotationParquetValueReaderVisitor(desc, expected, primitive);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> fixedReader(ColumnDescriptor desc) {
    return new FixedReader(desc);
  }

  @Override
  protected ParquetValueReaders.PrimitiveReader<?> int96Reader(ColumnDescriptor desc) {
    return new TimestampInt96Reader(desc);
  }

  @Override
  protected Object convertConstant(org.apache.iceberg.types.Type type, Object value) {
    return GenericDataUtil.internalToGeneric(type, value);
  }

  private static class RecordReader extends StructReader<Record, Record> {
    private final GenericRecord template;

    RecordReader(List<Type> types, List<ParquetValueReader<?>> readers, StructType struct) {
      super(types, readers);
      this.template = struct != null ? GenericRecord.create(struct) : null;
    }

    @Override
    protected Record newStructData(Record reuse) {
      if (reuse != null) {
        return reuse;
      } else {
        // GenericRecord.copy() is more performant then GenericRecord.create(StructType) since
        // NAME_MAP_CACHE access
        // is eliminated. Using copy here to gain performance.
        return template.copy();
      }
    }

    @Override
    protected Object getField(Record intermediate, int pos) {
      return intermediate.get(pos);
    }

    @Override
    protected Record buildStruct(Record struct) {
      return struct;
    }

    @Override
    protected void set(Record struct, int pos, Object value) {
      struct.set(pos, value);
    }
  }

  private class LogicalTypeAnnotationParquetValueReaderVisitor
      implements LogicalTypeAnnotation.LogicalTypeAnnotationVisitor<ParquetValueReader<?>> {

    private final ColumnDescriptor desc;
    private final org.apache.iceberg.types.Type.PrimitiveType expected;
    private final PrimitiveType primitive;

    LogicalTypeAnnotationParquetValueReaderVisitor(
        ColumnDescriptor desc,
        org.apache.iceberg.types.Type.PrimitiveType expected,
        PrimitiveType primitive) {
      this.desc = desc;
      this.expected = expected;
      this.primitive = primitive;
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.StringLogicalTypeAnnotation stringLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.EnumLogicalTypeAnnotation enumLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalLogicalType) {
      switch (primitive.getPrimitiveTypeName()) {
        case BINARY:
        case FIXED_LEN_BYTE_ARRAY:
          return Optional.of(
              new ParquetValueReaders.BinaryAsDecimalReader(desc, decimalLogicalType.getScale()));
        case INT64:
          return Optional.of(
              new ParquetValueReaders.LongAsDecimalReader(desc, decimalLogicalType.getScale()));
        case INT32:
          return Optional.of(
              new ParquetValueReaders.IntegerAsDecimalReader(desc, decimalLogicalType.getScale()));
        default:
          throw new UnsupportedOperationException(
              "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
      }
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.DateLogicalTypeAnnotation dateLogicalType) {
      return Optional.of(new DateReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeLogicalType) {
      if (timeLogicalType.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
        return Optional.of(new TimeReader(desc));
      } else if (timeLogicalType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
        return Optional.of(new TimeMillisReader(desc));
      }

      return Optional.empty();
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampLogicalType) {
      if (timestampLogicalType.getUnit() == LogicalTypeAnnotation.TimeUnit.MICROS) {
        Types.TimestampType tsMicrosType = (Types.TimestampType) expected;
        return tsMicrosType.shouldAdjustToUTC()
            ? Optional.of(new TimestamptzReader(desc))
            : Optional.of(new TimestampReader(desc));
      } else if (timestampLogicalType.getUnit() == LogicalTypeAnnotation.TimeUnit.MILLIS) {
        Types.TimestampType tsMillisType = (Types.TimestampType) expected;
        return tsMillisType.shouldAdjustToUTC()
            ? Optional.of(new TimestamptzMillisReader(desc))
            : Optional.of(new TimestampMillisReader(desc));
      }

      return LogicalTypeAnnotation.LogicalTypeAnnotationVisitor.super.visit(timestampLogicalType);
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.IntLogicalTypeAnnotation intLogicalType) {
      if (intLogicalType.getBitWidth() == 64) {
        return Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
      }
      return (expected.typeId() == org.apache.iceberg.types.Type.TypeID.LONG)
          ? Optional.of(new ParquetValueReaders.IntAsLongReader(desc))
          : Optional.of(new ParquetValueReaders.UnboxedReader<>(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.JsonLogicalTypeAnnotation jsonLogicalType) {
      return Optional.of(new ParquetValueReaders.StringReader(desc));
    }

    @Override
    public Optional<ParquetValueReader<?>> visit(
        LogicalTypeAnnotation.BsonLogicalTypeAnnotation bsonLogicalType) {
      return Optional.of(new ParquetValueReaders.BytesReader(desc));
    }
  }

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  private static class DateReader extends ParquetValueReaders.PrimitiveReader<LocalDate> {
    private DateReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDate read(LocalDate reuse) {
      return EPOCH_DAY.plusDays(column.nextInteger());
    }
  }

  private static class TimestampReader extends ParquetValueReaders.PrimitiveReader<LocalDateTime> {
    private TimestampReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalDateTime read(LocalDateTime reuse) {
      return EPOCH.plus(column.nextLong(), ChronoUnit.MICROS).toLocalDateTime();
    }
  }

  private static class TimestampMillisReader
      extends ParquetValueReaders.PrimitiveReader<LocalDateTime> {
    private TimestampMillisReader(ColumnDescriptor desc) {
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

    private TimestampInt96Reader(ColumnDescriptor desc) {
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
    private TimestamptzReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong(), ChronoUnit.MICROS);
    }
  }

  private static class TimestamptzMillisReader
      extends ParquetValueReaders.PrimitiveReader<OffsetDateTime> {
    private TimestamptzMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public OffsetDateTime read(OffsetDateTime reuse) {
      return EPOCH.plus(column.nextLong() * 1000, ChronoUnit.MICROS);
    }
  }

  private static class TimeMillisReader extends ParquetValueReaders.PrimitiveReader<LocalTime> {
    private TimeMillisReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalTime read(LocalTime reuse) {
      return LocalTime.ofNanoOfDay(column.nextLong() * 1000000L);
    }
  }

  private static class TimeReader extends ParquetValueReaders.PrimitiveReader<LocalTime> {
    private TimeReader(ColumnDescriptor desc) {
      super(desc);
    }

    @Override
    public LocalTime read(LocalTime reuse) {
      return LocalTime.ofNanoOfDay(column.nextLong() * 1000L);
    }
  }

  private static class FixedReader extends ParquetValueReaders.PrimitiveReader<byte[]> {
    private FixedReader(ColumnDescriptor desc) {
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
