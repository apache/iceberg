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
package org.apache.iceberg.spark.data.vectorized;

import java.util.List;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarBinaryVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ViewVarBinaryVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.ListViewVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * A fork of Spark's {@code ArrowColumnVector} for Arrow batches produced by Vortex scans.
 *
 * <p>Spark's own {@code ArrowColumnVector} (and the {@code ArrowUtils} type mapping it relies on)
 * rejects the Arrow view types ({@code Utf8View}, {@code BinaryView}, {@code ListView}) that Vortex
 * returns natively. This fork supports the same vector types as Spark's implementation plus the
 * view types, using per-type accessors so values are read without boxing.
 */
class VortexArrowColumnVector extends ColumnVector {
  private ArrowVectorAccessor accessor;
  private VortexArrowColumnVector[] childColumns;

  VortexArrowColumnVector(ValueVector vector) {
    super(fromArrowField(vector.getField()));
    initAccessor(vector);
  }

  @Override
  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  @Override
  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (VortexArrowColumnVector child : childColumns) {
        child.close();
      }
      childColumns = null;
    }
    accessor.vector.close();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  @Override
  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  @Override
  public byte getByte(int rowId) {
    return accessor.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    return accessor.getShort(rowId);
  }

  @Override
  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  @Override
  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  @Override
  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  @Override
  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getDecimal(rowId, precision, scale);
  }

  @Override
  public UTF8String getUTF8String(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getUTF8String(rowId);
  }

  @Override
  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getBinary(rowId);
  }

  @Override
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getMap(rowId);
  }

  @Override
  public VortexArrowColumnVector getChild(int ordinal) {
    return childColumns[ordinal];
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void initAccessor(ValueVector vector) {
    if (vector instanceof BitVector bitVector) {
      accessor = new BooleanAccessor(bitVector);
    } else if (vector instanceof TinyIntVector tinyIntVector) {
      accessor = new ByteAccessor(tinyIntVector);
    } else if (vector instanceof SmallIntVector smallIntVector) {
      accessor = new ShortAccessor(smallIntVector);
    } else if (vector instanceof IntVector intVector) {
      accessor = new IntAccessor(intVector);
    } else if (vector instanceof BigIntVector bigIntVector) {
      accessor = new LongAccessor(bigIntVector);
    } else if (vector instanceof Float4Vector float4Vector) {
      accessor = new FloatAccessor(float4Vector);
    } else if (vector instanceof Float8Vector float8Vector) {
      accessor = new DoubleAccessor(float8Vector);
    } else if (vector instanceof DecimalVector decimalVector) {
      accessor = new DecimalAccessor(decimalVector);
    } else if (vector instanceof VarCharVector varCharVector) {
      accessor = new StringAccessor(varCharVector);
    } else if (vector instanceof LargeVarCharVector largeVarCharVector) {
      accessor = new LargeStringAccessor(largeVarCharVector);
    } else if (vector instanceof ViewVarCharVector viewVarCharVector) {
      accessor = new StringViewAccessor(viewVarCharVector);
    } else if (vector instanceof VarBinaryVector varBinaryVector) {
      accessor = new BinaryAccessor(varBinaryVector);
    } else if (vector instanceof LargeVarBinaryVector largeVarBinaryVector) {
      accessor = new LargeBinaryAccessor(largeVarBinaryVector);
    } else if (vector instanceof ViewVarBinaryVector viewVarBinaryVector) {
      accessor = new BinaryViewAccessor(viewVarBinaryVector);
    } else if (vector instanceof DateDayVector dateDayVector) {
      accessor = new DateAccessor(dateDayVector);
    } else if (vector instanceof TimeStampVector timeStampVector) {
      // Covers all unit/timezone variants; values are normalized to microseconds.
      accessor = new TimestampAccessor(timeStampVector);
    } else if (vector instanceof MapVector mapVector) {
      // MapVector extends ListVector, so this check must come first.
      accessor = new MapAccessor(mapVector);
    } else if (vector instanceof ListVector listVector) {
      accessor = new ArrayAccessor(listVector);
    } else if (vector instanceof ListViewVector listViewVector) {
      accessor = new ListViewAccessor(listViewVector);
    } else if (vector instanceof StructVector structVector) {
      accessor = new StructAccessor(structVector);
      childColumns = new VortexArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new VortexArrowColumnVector(structVector.getVectorById(i));
      }
    } else if (vector instanceof NullVector nullVector) {
      accessor = new NullAccessor(nullVector);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported Arrow vector type: "
              + vector.getClass().getSimpleName()
              + " for field "
              + vector.getField());
    }
  }

  /**
   * Maps an Arrow field to the Spark type this vector exposes. This mirrors Spark's {@code
   * ArrowUtils.fromArrowField}, extended with the Arrow view types Vortex produces natively ({@code
   * Utf8View} to string, {@code BinaryView} to binary, {@code ListView} to array).
   */
  static DataType fromArrowField(Field field) {
    switch (field.getType().getTypeID()) {
      case Struct:
        StructField[] structFields = new StructField[field.getChildren().size()];
        List<Field> children = field.getChildren();
        for (int i = 0; i < children.size(); i++) {
          Field child = children.get(i);
          structFields[i] =
              new StructField(
                  child.getName(), fromArrowField(child), child.isNullable(), Metadata.empty());
        }
        return DataTypes.createStructType(structFields);
      case List:
      case ListView:
        Field elementField = field.getChildren().get(0);
        return DataTypes.createArrayType(fromArrowField(elementField), elementField.isNullable());
      case Map:
        Field entries = field.getChildren().get(0);
        Field keyField = entries.getChildren().get(0);
        Field valueField = entries.getChildren().get(1);
        return DataTypes.createMapType(
            fromArrowField(keyField), fromArrowField(valueField), valueField.isNullable());
      default:
        return fromArrowType(field.getType());
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static DataType fromArrowType(ArrowType arrowType) {
    switch (arrowType.getTypeID()) {
      case Bool:
        return DataTypes.BooleanType;
      case Int:
        ArrowType.Int intType = (ArrowType.Int) arrowType;
        if (intType.getIsSigned()) {
          switch (intType.getBitWidth()) {
            case 8:
              return DataTypes.ByteType;
            case 16:
              return DataTypes.ShortType;
            case 32:
              return DataTypes.IntegerType;
            case 64:
              return DataTypes.LongType;
          }
        }
        break;
      case FloatingPoint:
        ArrowType.FloatingPoint floatType = (ArrowType.FloatingPoint) arrowType;
        switch (floatType.getPrecision()) {
          case SINGLE:
            return DataTypes.FloatType;
          case DOUBLE:
            return DataTypes.DoubleType;
          default:
            break;
        }
        break;
      case Decimal:
        ArrowType.Decimal decimalType = (ArrowType.Decimal) arrowType;
        return DataTypes.createDecimalType(decimalType.getPrecision(), decimalType.getScale());
      case Utf8:
      case LargeUtf8:
      case Utf8View:
        return DataTypes.StringType;
      case Binary:
      case LargeBinary:
      case BinaryView:
        return DataTypes.BinaryType;
      case Date:
        if (((ArrowType.Date) arrowType).getUnit() == DateUnit.DAY) {
          return DataTypes.DateType;
        }
        break;
      case Timestamp:
        // Spark timestamps are physically microseconds; TimestampAccessor normalizes
        // second/millisecond/nanosecond values on read.
        ArrowType.Timestamp ts = (ArrowType.Timestamp) arrowType;
        return ts.getTimezone() != null ? DataTypes.TimestampType : DataTypes.TimestampNTZType;
      case Null:
        return DataTypes.NullType;
      default:
        break;
    }

    throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
  }

  /**
   * Base accessor reading vector memory directly by address (Comet-style). The null count is
   * computed once per batch column (Arrow rescans the validity bitmap on every {@code
   * getNullCount()} call) and null checks read the validity bit directly, skipping Arrow's per-call
   * bounds checks. Value getters may return garbage for null slots, which is allowed by Spark's
   * {@link ColumnVector} contract: callers check {@code isNullAt} (or {@code hasNull}) first.
   */
  private abstract static class ArrowVectorAccessor {
    // Marks a vector with null slots but no validity buffer (NullVector): every slot is null.
    private static final long ALL_NULL = -1L;

    private final ValueVector vector;
    private final int nullCount;
    private final long validityAddress;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
      this.nullCount = vector.getNullCount();
      if (vector instanceof NullVector) {
        this.validityAddress = ALL_NULL;
      } else if (nullCount > 0) {
        this.validityAddress = vector.getValidityBuffer().memoryAddress();
      } else {
        this.validityAddress = 0L;
      }
    }

    final boolean isNullAt(int rowId) {
      if (nullCount == 0) {
        return false;
      } else if (validityAddress == ALL_NULL) {
        return true;
      }
      return (Platform.getByte(null, validityAddress + (rowId >>> 3)) & (1 << (rowId & 7))) == 0;
    }

    final int getNullCount() {
      return nullCount;
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }

    ColumnarMap getMap(int rowId) {
      throw new UnsupportedOperationException(getClass().getName());
    }
  }

  private static final class BooleanAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    boolean getBoolean(int rowId) {
      return (Platform.getByte(null, dataAddress + (rowId >>> 3)) & (1 << (rowId & 7))) != 0;
    }
  }

  private static final class ByteAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    byte getByte(int rowId) {
      return Platform.getByte(null, dataAddress + rowId);
    }
  }

  private static final class ShortAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    short getShort(int rowId) {
      return Platform.getShort(null, dataAddress + ((long) rowId << 1));
    }
  }

  private static final class IntAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    IntAccessor(IntVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    int getInt(int rowId) {
      return Platform.getInt(null, dataAddress + ((long) rowId << 2));
    }
  }

  private static final class LongAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    long getLong(int rowId) {
      return Platform.getLong(null, dataAddress + ((long) rowId << 3));
    }
  }

  private static final class FloatAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    float getFloat(int rowId) {
      return Platform.getFloat(null, dataAddress + ((long) rowId << 2));
    }
  }

  private static final class DoubleAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    double getDouble(int rowId) {
      return Platform.getDouble(null, dataAddress + ((long) rowId << 3));
    }
  }

  private static final class DecimalAccessor extends ArrowVectorAccessor {
    private static final int TYPE_WIDTH = 16;

    private final DecimalVector accessor;
    private final long dataAddress;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    Decimal getDecimal(int rowId, int precision, int scale) {
      if (precision <= Decimal.MAX_LONG_DIGITS()) {
        // Arrow decimal128 values are little-endian two's complement; a value that fits in
        // precision <= 18 digits fits in a long, so its low 8 bytes are the full value with the
        // upper half being sign extension. Only valid on little-endian hardware.
        long unscaled = Platform.getLong(null, dataAddress + (long) rowId * TYPE_WIDTH);
        return Decimal.createUnsafe(unscaled, precision, scale);
      }
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static final class StringAccessor extends ArrowVectorAccessor {
    private final long offsetAddress;
    private final long dataAddress;

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.offsetAddress = vector.getOffsetBuffer().memoryAddress();
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      int start = Platform.getInt(null, offsetAddress + ((long) rowId << 2));
      int end = Platform.getInt(null, offsetAddress + ((long) (rowId + 1) << 2));
      return UTF8String.fromAddress(null, dataAddress + start, end - start);
    }
  }

  private static final class LargeStringAccessor extends ArrowVectorAccessor {
    private final long offsetAddress;
    private final long dataAddress;

    LargeStringAccessor(LargeVarCharVector vector) {
      super(vector);
      this.offsetAddress = vector.getOffsetBuffer().memoryAddress();
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      long start = Platform.getLong(null, offsetAddress + ((long) rowId << 3));
      long end = Platform.getLong(null, offsetAddress + ((long) (rowId + 1) << 3));
      // A single string cannot be larger than the max integer size, so the cast is safe.
      return UTF8String.fromAddress(null, dataAddress + start, (int) (end - start));
    }
  }

  private static final class StringViewAccessor extends ArrowVectorAccessor {
    // See the Arrow Utf8View spec: 16-byte views of {length, prefix|inline data, buffer id,
    // offset}; values of 12 bytes or fewer are inlined into the view itself.
    private static final int VIEW_SIZE = 16;
    private static final int INLINE_SIZE = 12;

    private final long viewAddress;
    private final long[] dataBufferAddresses;

    StringViewAccessor(ViewVarCharVector vector) {
      super(vector);
      this.viewAddress = vector.getDataBuffer().memoryAddress();
      List<ArrowBuf> dataBuffers = vector.getDataBuffers();
      this.dataBufferAddresses = new long[dataBuffers.size()];
      for (int i = 0; i < dataBuffers.size(); i++) {
        this.dataBufferAddresses[i] = dataBuffers.get(i).memoryAddress();
      }
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      long view = viewAddress + (long) rowId * VIEW_SIZE;
      int length = Platform.getInt(null, view);
      if (length <= INLINE_SIZE) {
        // Inline values live in the view buffer itself, which shares the batch's lifetime, so
        // aliasing it is as safe as aliasing a data buffer.
        return UTF8String.fromAddress(null, view + 4, length);
      }

      int bufferIndex = Platform.getInt(null, view + 8);
      int dataOffset = Platform.getInt(null, view + 12);
      return UTF8String.fromAddress(null, dataBufferAddresses[bufferIndex] + dataOffset, length);
    }
  }

  private static final class BinaryAccessor extends ArrowVectorAccessor {
    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static final class LargeBinaryAccessor extends ArrowVectorAccessor {
    private final LargeVarBinaryVector accessor;

    LargeBinaryAccessor(LargeVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static final class BinaryViewAccessor extends ArrowVectorAccessor {
    private final ViewVarBinaryVector accessor;

    BinaryViewAccessor(ViewVarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static final class DateAccessor extends ArrowVectorAccessor {
    private final long dataAddress;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
    }

    @Override
    int getInt(int rowId) {
      return Platform.getInt(null, dataAddress + ((long) rowId << 2));
    }
  }

  /**
   * Reads any timestamp vector variant (all units, with or without timezone), normalizing values to
   * the microseconds Spark expects for TimestampType and TimestampNTZType. Seconds and milliseconds
   * fail on overflow rather than silently wrapping; nanoseconds floor towards negative infinity,
   * matching Spark's nanosecond-to-microsecond conversions.
   */
  private static final class TimestampAccessor extends ArrowVectorAccessor {
    private final long dataAddress;
    private final org.apache.arrow.vector.types.TimeUnit unit;

    TimestampAccessor(TimeStampVector vector) {
      super(vector);
      this.dataAddress = vector.getDataBuffer().memoryAddress();
      this.unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
    }

    @Override
    long getLong(int rowId) {
      long value = Platform.getLong(null, dataAddress + ((long) rowId << 3));
      return switch (unit) {
        case SECOND -> Math.multiplyExact(value, 1_000_000L);
        case MILLISECOND -> Math.multiplyExact(value, 1_000L);
        case MICROSECOND -> value;
        case NANOSECOND -> Math.floorDiv(value, 1_000L);
      };
    }
  }

  private static final class ArrayAccessor extends ArrowVectorAccessor {
    private final ListVector accessor;
    private final VortexArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new VortexArrowColumnVector(vector.getDataVector());
    }

    @Override
    ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  private static final class ListViewAccessor extends ArrowVectorAccessor {
    private final ListViewVector accessor;
    private final VortexArrowColumnVector arrayData;

    ListViewAccessor(ListViewVector vector) {
      super(vector);
      this.accessor = vector;
      this.arrayData = new VortexArrowColumnVector(vector.getDataVector());
    }

    @Override
    ColumnarArray getArray(int rowId) {
      int start = accessor.getElementStartIndex(rowId);
      int end = accessor.getElementEndIndex(rowId);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Struct values are read through {@link ColumnVector#getStruct(int)}, which uses the child
   * columns; this accessor only provides null tracking.
   */
  private static final class StructAccessor extends ArrowVectorAccessor {
    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

  private static final class MapAccessor extends ArrowVectorAccessor {
    private final MapVector accessor;
    private final VortexArrowColumnVector keys;
    private final VortexArrowColumnVector values;

    MapAccessor(MapVector vector) {
      super(vector);
      this.accessor = vector;
      StructVector entries = (StructVector) vector.getDataVector();
      this.keys = new VortexArrowColumnVector(entries.getChild(MapVector.KEY_NAME));
      this.values = new VortexArrowColumnVector(entries.getChild(MapVector.VALUE_NAME));
    }

    @Override
    ColumnarMap getMap(int rowId) {
      int index = rowId * MapVector.OFFSET_WIDTH;
      int offset = accessor.getOffsetBuffer().getInt(index);
      int length = accessor.getInnerValueCountAt(rowId);
      return new ColumnarMap(keys, values, offset, length);
    }
  }

  private static final class NullAccessor extends ArrowVectorAccessor {
    NullAccessor(NullVector vector) {
      super(vector);
    }
  }
}
