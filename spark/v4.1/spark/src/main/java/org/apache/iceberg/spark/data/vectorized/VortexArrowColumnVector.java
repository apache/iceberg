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
import org.apache.arrow.vector.holders.NullableLargeVarCharHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
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

  private abstract static class ArrowVectorAccessor {
    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    final boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
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
    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static final class ByteAccessor extends ArrowVectorAccessor {
    private final TinyIntVector accessor;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    byte getByte(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class ShortAccessor extends ArrowVectorAccessor {
    private final SmallIntVector accessor;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    short getShort(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class IntAccessor extends ArrowVectorAccessor {
    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class LongAccessor extends ArrowVectorAccessor {
    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class FloatAccessor extends ArrowVectorAccessor {
    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class DoubleAccessor extends ArrowVectorAccessor {
    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static final class DecimalAccessor extends ArrowVectorAccessor {
    private final DecimalVector accessor;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    Decimal getDecimal(int rowId, int precision, int scale) {
      return Decimal.apply(accessor.getObject(rowId), precision, scale);
    }
  }

  private static final class StringAccessor extends ArrowVectorAccessor {
    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      }
      return UTF8String.fromAddress(
          null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          stringResult.end - stringResult.start);
    }
  }

  private static final class LargeStringAccessor extends ArrowVectorAccessor {
    private final LargeVarCharVector accessor;
    private final NullableLargeVarCharHolder stringResult = new NullableLargeVarCharHolder();

    LargeStringAccessor(LargeVarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      }
      // A single string cannot be larger than the max integer size, so the cast is safe.
      return UTF8String.fromAddress(
          null,
          stringResult.buffer.memoryAddress() + stringResult.start,
          (int) (stringResult.end - stringResult.start));
    }
  }

  private static final class StringViewAccessor extends ArrowVectorAccessor {
    private final ViewVarCharVector accessor;

    StringViewAccessor(ViewVarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    UTF8String getUTF8String(int rowId) {
      // View values may be inlined in the view buffer rather than contiguous in a data buffer,
      // so copy out rather than aliasing vector memory.
      return UTF8String.fromBytes(accessor.get(rowId));
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
    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  /**
   * Reads any timestamp vector variant (all units, with or without timezone), normalizing values to
   * the microseconds Spark expects for TimestampType and TimestampNTZType. Seconds and milliseconds
   * fail on overflow rather than silently wrapping; nanoseconds floor towards negative infinity,
   * matching Spark's nanosecond-to-microsecond conversions.
   */
  private static final class TimestampAccessor extends ArrowVectorAccessor {
    private final TimeStampVector accessor;
    private final org.apache.arrow.vector.types.TimeUnit unit;

    TimestampAccessor(TimeStampVector vector) {
      super(vector);
      this.accessor = vector;
      this.unit = ((ArrowType.Timestamp) vector.getField().getType()).getUnit();
    }

    @Override
    long getLong(int rowId) {
      long value = accessor.get(rowId);
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
