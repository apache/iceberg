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
import org.apache.iceberg.vortex.BoundVortexReader;
import org.apache.iceberg.vortex.VortexArrowProperties;
import org.apache.iceberg.vortex.VortexValueReader;
import org.apache.spark.sql.catalyst.util.ArrayData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.apache.spark.unsafe.types.VariantVal;

public class SparkVortexValueReaders {
  static {
    // Configure Arrow's unsafe-memory-access and null-check-for-get properties like
    // VectorizedSparkParquetReaders does; Spark always calls isNullAt before value getters, so
    // Arrow's per-get null checks and bounds checks are redundant on this path.
    VortexArrowProperties.ensureConfigured();
  }

  private SparkVortexValueReaders() {}

  /**
   * Returns a string reader specialized for the field's vector type: Vortex scans return string
   * columns as Utf8View, while locally-built batches use regular Utf8 vectors.
   */
  public static VortexValueReader<UTF8String> utf8String(ArrowType arrowType) {
    return arrowType instanceof ArrowType.Utf8View ? new UTF8ViewReader() : new UTF8Reader();
  }

  /** Spark represents BinaryType as byte[], unlike the generic reader which yields a ByteBuffer. */
  public static VortexValueReader<byte[]> bytes(ArrowType arrowType) {
    return arrowType instanceof ArrowType.BinaryView ? new BytesViewReader() : new BytesReader();
  }

  /** Spark represents DecimalType as {@link Decimal}, not {@link java.math.BigDecimal}. */
  public static VortexValueReader<Decimal> decimals() {
    return new DecimalReader();
  }

  public static VortexValueReader<Integer> date() {
    return new DateReader();
  }

  public static VortexValueReader<UTF8String> uuid() {
    // Iceberg's UUID maps to Spark StringType; emit the canonical UUID string.
    return new UuidReader();
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
    return new VariantReader();
  }

  static VortexValueReader<ArrayData> list(VortexValueReader<?> elementReader) {
    return new ListReader(elementReader);
  }

  private static class ListReader extends BoundVortexReader<ArrayData> {
    private final VortexValueReader<?> elementReader;
    private ListVector listVector;

    private ListReader(VortexValueReader<?> elementReader) {
      this.elementReader = elementReader;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      this.listVector = (ListVector) vector;
      elementReader.bind(listVector.getDataVector());
    }

    @Override
    public ArrayData readNonNull(int row) {
      int start = listVector.getElementStartIndex(row);
      int end = listVector.getElementEndIndex(row);
      Object[] elements = new Object[end - start];
      for (int index = start; index < end; index++) {
        elements[index - start] = elementReader.read(index);
      }
      return new GenericArrayData(elements);
    }
  }

  static class UTF8Reader extends BoundVortexReader<UTF8String> {
    private VarCharVector stringVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.stringVector = (VarCharVector) vector;
    }

    @Override
    public UTF8String readNonNull(int row) {
      return UTF8String.fromBytes(stringVector.get(row));
    }
  }

  static class UTF8ViewReader extends BoundVortexReader<UTF8String> {
    private ViewVarCharVector stringVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.stringVector = (ViewVarCharVector) vector;
    }

    @Override
    public UTF8String readNonNull(int row) {
      return UTF8String.fromBytes(stringVector.get(row));
    }
  }

  static class BytesReader extends BoundVortexReader<byte[]> {
    private VarBinaryVector binaryVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.binaryVector = (VarBinaryVector) vector;
    }

    @Override
    public byte[] readNonNull(int row) {
      return binaryVector.get(row);
    }
  }

  static class BytesViewReader extends BoundVortexReader<byte[]> {
    private ViewVarBinaryVector binaryVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.binaryVector = (ViewVarBinaryVector) vector;
    }

    @Override
    public byte[] readNonNull(int row) {
      return binaryVector.get(row);
    }
  }

  static class DecimalReader extends BoundVortexReader<Decimal> {
    private DecimalVector decimalVector;

    @Override
    protected void bindVector(FieldVector vector) {
      this.decimalVector = (DecimalVector) vector;
    }

    @Override
    public Decimal readNonNull(int row) {
      return Decimal.apply(decimalVector.getObjectNotNull(row));
    }
  }

  static class UuidReader extends BoundVortexReader<UTF8String> {
    private FixedSizeBinaryVector storage;

    @Override
    protected void bindVector(FieldVector vector) {
      this.storage =
          vector instanceof ExtensionTypeVector<?> ext
              ? (FixedSizeBinaryVector) ext.getUnderlyingVector()
              : (FixedSizeBinaryVector) vector;
    }

    @Override
    public UTF8String readNonNull(int row) {
      return UTF8String.fromString(UUIDUtil.convert(storage.get(row)).toString());
    }
  }

  // Spark expects DateType as Integer number of days since UNIX epoch.
  static class DateReader extends BoundVortexReader<Integer> {
    private DateDayVector dayVector;
    private DateMilliVector milliVector;

    @Override
    protected void bindVector(FieldVector vector) {
      if (vector instanceof DateMilliVector dateMilliVector) {
        this.milliVector = dateMilliVector;
        this.dayVector = null;
      } else {
        this.dayVector = (DateDayVector) vector;
        this.milliVector = null;
      }
    }

    @Override
    public Integer readNonNull(int row) {
      if (milliVector != null) {
        return DateTimeUtil.microsToDays(milliVector.get(row) * 1000L);
      }
      return dayVector.get(row);
    }
  }

  static class TimestampReader extends BoundVortexReader<Long> {
    private final TimeUnit unit;
    private TimeStampVector timestampVector;
    private BigIntVector longVector;

    private TimestampReader(TimeUnit unit) {
      this.unit = unit;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      if (vector instanceof TimeStampVector ts) {
        this.timestampVector = ts;
        this.longVector = null;
      } else {
        this.longVector = (BigIntVector) vector;
        this.timestampVector = null;
      }
    }

    @Override
    public Long readNonNull(int row) {
      long measure = timestampVector != null ? timestampVector.get(row) : longVector.get(row);
      return switch (unit) {
        case NANOSECOND -> Math.floorDiv(measure, 1_000L);
        case MICROSECOND -> measure;
        case MILLISECOND -> Math.multiplyExact(measure, 1_000L);
        case SECOND -> Math.multiplyExact(measure, 1_000_000L);
      };
    }
  }

  static class TimeReader extends BoundVortexReader<Long> {
    private final TimeUnit unit;
    private TimeMicroVector microVector;
    private TimeNanoVector nanoVector;
    private BigIntVector longVector;

    private TimeReader(TimeUnit unit) {
      this.unit = unit;
    }

    @Override
    protected void bindVector(FieldVector vector) {
      this.microVector = null;
      this.nanoVector = null;
      this.longVector = null;
      if (vector instanceof TimeMicroVector tm) {
        this.microVector = tm;
      } else if (vector instanceof TimeNanoVector tn) {
        this.nanoVector = tn;
      } else {
        this.longVector = (BigIntVector) vector;
      }
    }

    @Override
    public Long readNonNull(int row) {
      long measure;
      if (microVector != null) {
        measure = microVector.get(row);
      } else if (nanoVector != null) {
        measure = nanoVector.get(row);
      } else {
        measure = longVector.get(row);
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
    private final VortexValueReader<Variant> delegate = GenericVortexReaders.variants();

    private VariantReader() {}

    @Override
    public void bind(FieldVector vector) {
      delegate.bind(vector);
    }

    @Override
    public VariantVal read(int row) {
      Variant variant = delegate.read(row);
      return variant == null ? null : toVariantVal(variant);
    }

    @Override
    public VariantVal readNonNull(int row) {
      return toVariantVal(delegate.readNonNull(row));
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
