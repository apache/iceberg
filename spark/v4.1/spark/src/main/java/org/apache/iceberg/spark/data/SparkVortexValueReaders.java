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
package org.apache.iceberg.spark.data;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.ExtensionTypeVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.iceberg.data.vortex.GenericVortexReaders;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.iceberg.util.UUIDUtil;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class SparkVortexValueReaders {
  private SparkVortexValueReaders() {}

  /**
   * Returns a string reader specialized for the field's vector type: Vortex scans return string
   * columns as Utf8View, while locally-built batches use regular Utf8 vectors.
   */
  public static VortexValueReader<UTF8String> utf8String(ArrowType arrowType) {
    return arrowType instanceof ArrowType.Utf8View ? UTF8ViewReader.INSTANCE : UTF8Reader.INSTANCE;
  }

  /** Spark represents BinaryType as byte[], unlike the generic reader which yields a ByteBuffer. */
  public static VortexValueReader<byte[]> bytes(ArrowType arrowType) {
    return arrowType instanceof ArrowType.BinaryView
        ? BytesViewReader.INSTANCE
        : BytesReader.INSTANCE;
  }

  /** Spark represents DecimalType as {@link Decimal}, not {@link java.math.BigDecimal}. */
  public static VortexValueReader<Decimal> decimals() {
    return DecimalReader.INSTANCE;
  }

  public static VortexValueReader<Integer> date() {
    return DateReader.INSTANCE;
  }

  public static VortexValueReader<UTF8String> uuid() {
    // Iceberg's UUID maps to Spark StringType; emit the canonical UUID string.
    return UuidReader.INSTANCE;
  }

  public static VortexValueReader<Long> timestamp(TimeUnit timeUnit) {
    // Spark timestamp has µs precision
    return new TimestampReader(timeUnit);
  }

  public static VortexValueReader<Long> time(TimeUnit timeUnit) {
    // Spark's TimeType is stored as microseconds since midnight (Long).
    return new TimeReader(timeUnit);
  }

  public static VortexValueReader<VariantVal> variants() {
    return VariantReader.INSTANCE;
  }

  static VortexValueReader<ArrayData> list(VortexValueReader<?> elementReader) {
    return new ListReader(elementReader);
  }

  private static class ListReader implements VortexValueReader<ArrayData> {
    private final VortexValueReader<?> elementReader;

    private ListReader(VortexValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    public ArrayData readNonNull(FieldVector vector, int row) {
      ListVector list = (ListVector) vector;
      int start = list.getElementStartIndex(row);
      int end = list.getElementEndIndex(row);
      Object[] elements = new Object[end - start];
      FieldVector elementVector = list.getDataVector();
      for (int index = start; index < end; index++) {
        elements[index - start] = elementReader.read(elementVector, index);
      }
      return new GenericArrayData(elements);
    }
  }

  static class UTF8Reader implements VortexValueReader<UTF8String> {
    static final UTF8Reader INSTANCE = new UTF8Reader();

    private UTF8Reader() {}

    @Override
    public UTF8String readNonNull(FieldVector vector, int row) {
      return UTF8String.fromBytes(((VarCharVector) vector).get(row));
    }
  }

  static class UTF8ViewReader implements VortexValueReader<UTF8String> {
    static final UTF8ViewReader INSTANCE = new UTF8ViewReader();

    private UTF8ViewReader() {}

    @Override
    public UTF8String readNonNull(FieldVector vector, int row) {
      return UTF8String.fromBytes(((ViewVarCharVector) vector).get(row));
    }
  }

  static class BytesReader implements VortexValueReader<byte[]> {
    static final BytesReader INSTANCE = new BytesReader();

    private BytesReader() {}

    @Override
    public byte[] readNonNull(FieldVector vector, int row) {
      return ((VarBinaryVector) vector).get(row);
    }
  }

  static class BytesViewReader implements VortexValueReader<byte[]> {
    static final BytesViewReader INSTANCE = new BytesViewReader();

    private BytesViewReader() {}

    @Override
    public byte[] readNonNull(FieldVector vector, int row) {
      return ((ViewVarBinaryVector) vector).get(row);
    }
  }

  static class DecimalReader implements VortexValueReader<Decimal> {
    static final DecimalReader INSTANCE = new DecimalReader();

    private DecimalReader() {}

    @Override
    public Decimal readNonNull(FieldVector vector, int row) {
      return Decimal.apply(((DecimalVector) vector).getObject(row));
    }
  }

  static class UuidReader implements VortexValueReader<UTF8String> {
    static final UuidReader INSTANCE = new UuidReader();

    private UuidReader() {}

    @Override
    public UTF8String readNonNull(FieldVector vector, int row) {
      FixedSizeBinaryVector storage =
          vector instanceof ExtensionTypeVector<?> ext
              ? (FixedSizeBinaryVector) ext.getUnderlyingVector()
              : (FixedSizeBinaryVector) vector;
      return UTF8String.fromString(UUIDUtil.convert(storage.get(row)).toString());
    }
  }

  // Spark expects DateType as Integer number of days since UNIX epoch.
  static class DateReader implements VortexValueReader<Integer> {
    static final DateReader INSTANCE = new DateReader();

    private DateReader() {}

    @Override
    public Integer readNonNull(FieldVector vector, int row) {
      ArrowType arrowType = vector.getField().getType();
      if (arrowType instanceof ArrowType.Date dateType
          && dateType.getUnit() == org.apache.arrow.vector.types.DateUnit.MILLISECOND) {
        long millis = ((DateMilliVector) vector).get(row);
        return DateTimeUtil.microsToDays(millis * 1000L);
      }
      return ((DateDayVector) vector).get(row);
    }
  }

  static class TimestampReader implements VortexValueReader<Long> {
    private final TimeUnit unit;

    private TimestampReader(TimeUnit unit) {
      this.unit = unit;
    }

    @Override
    public Long readNonNull(FieldVector vector, int row) {
      long measure;
      if (vector instanceof TimeStampVector ts) {
        measure = ts.get(row);
      } else {
        measure = ((BigIntVector) vector).get(row);
      }
      return switch (unit) {
        case NANOSECOND -> Math.floorDiv(measure, 1_000L);
        case MICROSECOND -> measure;
        case MILLISECOND -> Math.multiplyExact(measure, 1_000L);
        case SECOND -> Math.multiplyExact(measure, 1_000_000L);
      };
    }
  }

  static class TimeReader implements VortexValueReader<Long> {
    private final TimeUnit unit;

    private TimeReader(TimeUnit unit) {
      this.unit = unit;
    }

    @Override
    public Long readNonNull(FieldVector vector, int row) {
      long measure;
      if (vector instanceof TimeMicroVector tm) {
        measure = tm.get(row);
      } else if (vector instanceof TimeNanoVector tn) {
        measure = tn.get(row);
      } else {
        measure = ((BigIntVector) vector).get(row);
      }
      return switch (unit) {
        case NANOSECOND -> Math.floorDiv(measure, 1_000L);
        case MICROSECOND -> measure;
        case MILLISECOND -> Math.multiplyExact(measure, 1_000L);
        case SECOND -> Math.multiplyExact(measure, 1_000_000L);
      };
    }
  }

  // Converts the Iceberg Variant produced by the shared Vortex reader into Spark's VariantVal by
  // re-serializing metadata and value to little-endian buffers (mirrors SparkParquetReaders).
  static class VariantReader implements VortexValueReader<VariantVal> {
    static final VariantReader INSTANCE = new VariantReader();

    private final VortexValueReader<Variant> delegate = GenericVortexReaders.variants();

    private VariantReader() {}

    @Override
    public VariantVal read(FieldVector vector, int row) {
      Variant variant = delegate.read(vector, row);
      return variant == null ? null : toVariantVal(variant);
    }

    @Override
    public VariantVal readNonNull(FieldVector vector, int row) {
      return toVariantVal(delegate.readNonNull(vector, row));
    }

    private static VariantVal toVariantVal(Variant variant) {
      byte[] metadataBytes = new byte[variant.metadata().sizeInBytes()];
      variant.metadata().writeTo(ByteBuffer.wrap(metadataBytes).order(ByteOrder.LITTLE_ENDIAN), 0);

      byte[] valueBytes = new byte[variant.value().sizeInBytes()];
      variant.value().writeTo(ByteBuffer.wrap(valueBytes).order(ByteOrder.LITTLE_ENDIAN), 0);

      return new VariantVal(valueBytes, metadataBytes);
    }
  }
}
