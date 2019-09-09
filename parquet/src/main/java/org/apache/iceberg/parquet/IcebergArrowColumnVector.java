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
package org.apache.iceberg.parquet;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class IcebergArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor accessor;
  private final NullabilityHolder nullabilityHolder;
  private ArrowColumnVector[] childColumns;

  public IcebergArrowColumnVector(ValueVector vector, NullabilityHolder nulls) {
    super(ArrowUtils.fromArrowField(vector.getField()));
    this.nullabilityHolder = nulls;
    this.accessor = initAccessor(vector);
  }

  @Override
  public void close() {
    if (childColumns != null) {
      for (int i = 0; i < childColumns.length; i++) {
        childColumns[i].close();
        childColumns[i] = null;
      }
      childColumns = null;
    }
    accessor.close();
  }

  @Override
  public boolean hasNull() {
    return nullabilityHolder.hasNulls();
  }

  @Override
  public int numNulls() {
    return nullabilityHolder.numNulls();
  }

  @Override
  public boolean isNullAt(int rowId) {
    return nullabilityHolder.isNullAt(rowId);
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
  public ColumnarArray getArray(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getArray(rowId);
  }

  @Override
  public ColumnarMap getMap(int rowId) {
    throw new UnsupportedOperationException();
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
  public ArrowColumnVector getChild(int ordinal) { return childColumns[ordinal]; }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte getByte(int rowId) {
      throw new UnsupportedOperationException();
    }

    short getShort(int rowId) {
      throw new UnsupportedOperationException();
    }

    int getInt(int rowId) {
      throw new UnsupportedOperationException();
    }

    long getLong(int rowId) {
      throw new UnsupportedOperationException();
    }

    float getFloat(int rowId) {
      throw new UnsupportedOperationException();
    }

    double getDouble(int rowId) {
      throw new UnsupportedOperationException();
    }

    Decimal getDecimal(int rowId, int precision, int scale) {
      throw new UnsupportedOperationException();
    }

    UTF8String getUTF8String(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }

    ColumnarArray getArray(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  public ArrowVectorAccessor initAccessor(ValueVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanAccessor((BitVector) vector);
    } else if (vector instanceof TinyIntVector) {
      return new ByteAccessor((TinyIntVector) vector);
    } else if (vector instanceof SmallIntVector) {
      return new ShortAccessor((SmallIntVector) vector);
    } else if (vector instanceof IntVector) {
      return new IntAccessor((IntVector) vector);
    } else if (vector instanceof BigIntVector) {
      return new LongAccessor((BigIntVector) vector);
    } else if (vector instanceof Float4Vector) {
      return new FloatAccessor((Float4Vector) vector);
    } else if (vector instanceof Float8Vector) {
      return new DoubleAccessor((Float8Vector) vector);
    } else if (vector instanceof DecimalVector) {
      return new DecimalAccessor((DecimalVector) vector);
    } else if (vector instanceof IcebergVarcharVector) {
      return new StringAccessor((IcebergVarcharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return new TimestampAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof ListVector) {
      ListVector listVector = (ListVector) vector;
      return new ArrayAccessor(listVector);
    } else if (vector instanceof StructVector) {
      StructVector structVector = (StructVector) vector;
      ArrowVectorAccessor accessor = new StructAccessor(structVector);
      childColumns = new ArrowColumnVector[structVector.size()];
      for (int i = 0; i < childColumns.length; ++i) {
        childColumns[i] = new ArrowColumnVector(structVector.getVectorById(i));
      }
      return accessor;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector vector;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return vector.get(rowId) == 1;
    }
  }

  private static class ByteAccessor extends ArrowVectorAccessor {

    private final TinyIntVector vector;

    ByteAccessor(TinyIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final byte getByte(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class ShortAccessor extends ArrowVectorAccessor {

    private final SmallIntVector vector;

    ShortAccessor(SmallIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final short getShort(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    IntAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final int getInt(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector vector;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector vector;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector vector;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class DecimalAccessor extends ArrowVectorAccessor {

    private final DecimalVector vector;

    DecimalAccessor(DecimalVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(vector.getObject(rowId), precision, scale);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final IcebergVarcharVector vector;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(IcebergVarcharVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      vector.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        return UTF8String.fromAddress(null,
            stringResult.buffer.memoryAddress() + stringResult.start,
            stringResult.end - stringResult.start);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector vector;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return vector.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector vector;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final int getInt(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class TimestampAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector vector;

    TimestampAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final long getLong(int rowId) {
      return vector.get(rowId);
    }
  }

  private static class ArrayAccessor extends ArrowVectorAccessor {

    private final ListVector vector;
    private final ArrowColumnVector arrayData;

    ArrayAccessor(ListVector vector) {
      super(vector);
      this.vector = vector;
      this.arrayData = new ArrowColumnVector(vector.getDataVector());
    }

    @Override
    final boolean isNullAt(int rowId) {
      // TODO: Workaround if vector has all non-null values, see ARROW-1948
      if (vector.getValueCount() > 0 && vector.getValidityBuffer().capacity() == 0) {
        return false;
      } else {
        return super.isNullAt(rowId);
      }
    }

    @Override
    final ColumnarArray getArray(int rowId) {
      ArrowBuf offsets = vector.getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Any call to "get" method will throw UnsupportedOperationException.
   *
   * Access struct values in a ArrowColumnVector doesn't use this vector. Instead, it uses
   * getStruct() method defined in the parent class. Any call to "get" method in this class is a
   * bug in the code.
   *
   */
  private static class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

}
