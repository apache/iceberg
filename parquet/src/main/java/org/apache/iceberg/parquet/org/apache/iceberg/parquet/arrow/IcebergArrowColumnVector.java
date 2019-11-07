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
package org.apache.iceberg.parquet.org.apache.iceberg.parquet.arrow;

import io.netty.buffer.ArrowBuf;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.iceberg.parquet.NullabilityHolder;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.VectorReader;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.DecimalMetadata;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.execution.arrow.ArrowUtils;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

import java.math.BigInteger;

/**
 * Implementation of Spark's {@link ColumnVector} interface. The main purpose
 * of this class is to prevent the expensive nullability checks made by Spark's
 * {@link ArrowColumnVector} implementation by delegating those calls to the
 * Iceberg's {@link NullabilityHolder}.
 */

public class IcebergArrowColumnVector extends ColumnVector {

  private final ArrowVectorAccessor accessor;
  private final NullabilityHolder nullabilityHolder;
  private final ColumnDescriptor columnDescriptor;
  private final Dictionary dictionary;
  private ArrowColumnVector[] childColumns;

  public IcebergArrowColumnVector(VectorReader.VectorHolder holder, NullabilityHolder nulls) {
    super(ArrowUtils.fromArrowField(holder.getVector().getField()));
    this.nullabilityHolder = nulls;
    this.columnDescriptor = holder.getDescriptor();
    this.dictionary = holder.getDictionary();
    this.accessor = getVectorAccessor(columnDescriptor, holder.getVector());
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

  private abstract class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    // TODO: should be final after removing ArrayAccessor workaround
    boolean isNullAt(int rowId) {
      return nullabilityHolder.isNullAt(rowId);
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

  private ArrowVectorAccessor getVectorAccessor(ColumnDescriptor desc, ValueVector vector) {
    PrimitiveType primitive = desc.getPrimitiveType();
    boolean isDictionaryEncoded = dictionary != null;
    if (isDictionaryEncoded) {
      Preconditions.checkState(vector instanceof IntVector, "Dictionary ids should be stored in IntVectors only");
      if (primitive.getOriginalType() != null) {
        switch (desc.getPrimitiveType().getOriginalType()) {
          case ENUM:
          case JSON:
          case UTF8:
          case BSON:
            return new DictionaryStringAccessor((IntVector) vector);
          case INT_8:
          case INT_16:
          case INT_32:
          case DATE:
            return new DictionaryIntAccessor((IntVector) vector);
          case INT_64:
          case TIMESTAMP_MILLIS:
          case TIMESTAMP_MICROS:
            return new DictionaryLongAccessor((IntVector) vector);
          case DECIMAL:
            DecimalMetadata decimal = primitive.getDecimalMetadata();
            switch (primitive.getPrimitiveTypeName()) {
              case BINARY:
              case FIXED_LEN_BYTE_ARRAY:
                return new DictionaryDecimalBinaryAccessor((IntVector) vector, decimal.getPrecision(), decimal.getScale());
              case INT64:
                return new DictionaryDecimalLongAccessor((IntVector) vector, decimal.getPrecision(), decimal.getScale());
              case INT32:
                return new DictionaryDecimalIntAccessor((IntVector) vector, decimal.getPrecision(), decimal.getScale());
              default:
                throw new UnsupportedOperationException(
                        "Unsupported base type for decimal: " + primitive.getPrimitiveTypeName());
            }
          default:
            throw new UnsupportedOperationException(
                    "Unsupported logical type: " + primitive.getOriginalType());
        }
      } else {
        switch (primitive.getPrimitiveTypeName()) {
          case FIXED_LEN_BYTE_ARRAY:
          case BINARY:
            return new DictionaryBinaryAccessor((IntVector) vector);
          case INT32:
            return new DictionaryIntAccessor((IntVector) vector);
          case FLOAT:
            return new DictionaryFloatAccessor((IntVector) vector);
//        case BOOLEAN:
//          this.vec = ArrowSchemaUtil.convert(icebergField).createVector(rootAlloc);
//          ((BitVector) vec).allocateNew(batchSize);
//          return UNKNOWN_WIDTH;
          case INT64:
            return new DictionaryLongAccessor((IntVector) vector);
          case DOUBLE:
            return new DictionaryDoubleAccessor((IntVector) vector);
          default:
            throw new UnsupportedOperationException("Unsupported type: " + primitive);
        }
      }
    } else {
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
      } else if (vector instanceof IcebergDecimalArrowVector) {
        return new DecimalAccessor((IcebergDecimalArrowVector) vector);
      } else if (vector instanceof IcebergVarcharArrowVector) {
        return new StringAccessor((IcebergVarcharArrowVector) vector);
      } else if (vector instanceof IcebergVarBinaryArrowVector) {
        return new BinaryAccessor((IcebergVarBinaryArrowVector) vector);
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
      }
    }
    throw new UnsupportedOperationException();
  }

  private class BooleanAccessor extends ArrowVectorAccessor {

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

  private class ByteAccessor extends ArrowVectorAccessor {

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

  private class ShortAccessor extends ArrowVectorAccessor {

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

  private class IntAccessor extends ArrowVectorAccessor {

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

  private class DictionaryIntAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryIntAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final int getInt(int rowId) {
      return dictionary.decodeToInt(vector.get(rowId));
    }
  }

  private class LongAccessor extends ArrowVectorAccessor {

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

  private class DictionaryLongAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryLongAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final long getLong(int rowId) {
      return dictionary.decodeToLong(vector.get(rowId));
    }
  }

  private class FloatAccessor extends ArrowVectorAccessor {

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

  private class DictionaryFloatAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryFloatAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return dictionary.decodeToFloat(vector.get(rowId));
    }
  }

  private class DoubleAccessor extends ArrowVectorAccessor {

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

  private class DictionaryDoubleAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryDoubleAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return dictionary.decodeToDouble(vector.get(rowId));
    }
  }

  private class DecimalAccessor extends ArrowVectorAccessor {

    private final IcebergDecimalArrowVector vector;

    DecimalAccessor(IcebergDecimalArrowVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      return Decimal.apply(vector.getObject(rowId), precision, scale);
    }
  }

  private class StringAccessor extends ArrowVectorAccessor {

    private final IcebergVarcharArrowVector vector;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(IcebergVarcharArrowVector vector) {
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

  private class DictionaryStringAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryStringAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      if (isNullAt(rowId)) {
        return null;
      }
      Binary binary = dictionary.decodeToBinary(vector.get(rowId));
      return UTF8String.fromBytes(binary.getBytesUnsafe());
    }
  }

  private class FixedSizeBinaryAccessor extends ArrowVectorAccessor {

    private final FixedSizeBinaryVector vector;

    FixedSizeBinaryAccessor(FixedSizeBinaryVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return vector.getObject(rowId);
    }
  }

  private class BinaryAccessor extends ArrowVectorAccessor {

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

  private class DictionaryBinaryAccessor extends ArrowVectorAccessor {

    private final IntVector vector;

    DictionaryBinaryAccessor(IntVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      Binary binary = dictionary.decodeToBinary(vector.get(rowId));
      return binary.getBytesUnsafe();
    }
  }


  private class DateAccessor extends ArrowVectorAccessor {

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

  private class DictionaryDateAccessor extends DictionaryIntAccessor {
    DictionaryDateAccessor(IntVector vector) {
      super(vector);
    }
  }

  private class TimestampAccessor extends ArrowVectorAccessor {

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

  private class DictionaryTimestampAccessor extends DictionaryLongAccessor {
    DictionaryTimestampAccessor(IntVector vector) {
      super(vector);
    }
  }

  private class ArrayAccessor extends ArrowVectorAccessor {

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
  private class StructAccessor extends ArrowVectorAccessor {

    StructAccessor(StructVector vector) {
      super(vector);
    }
  }

  private class DictionaryDecimalBinaryAccessor extends ArrowVectorAccessor {
    private final IntVector vector;

    public DictionaryDecimalBinaryAccessor(IntVector vector, int precision, int scale) {
      super(vector);
      this.vector = vector;
    }

    //TODO: samarth not sure this is efficient or correct.
    //TODO: samarth refer to decodeDictionaryIds in VectorizedColumnReader
    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      Binary value = dictionary.decodeToBinary(vector.get(rowId));
      BigInteger unscaledValue = new BigInteger(value.getBytesUnsafe());
      return Decimal.apply(unscaledValue.longValue(), precision, scale);
    }
  }

  private class DictionaryDecimalLongAccessor extends ArrowVectorAccessor {
    private final IntVector vector;

    public DictionaryDecimalLongAccessor(IntVector vector, int precision, int scale) {
      super(vector);
      this.vector = vector;
    }

    //TODO: samarth not sure this is efficient or correct
    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      long unscaledValue = dictionary.decodeToLong(vector.get(rowId));
      return Decimal.apply(unscaledValue, precision, scale);
    }
  }

  private class DictionaryDecimalIntAccessor extends ArrowVectorAccessor {
    private final IntVector vector;

    public DictionaryDecimalIntAccessor(IntVector vector, int precision, int scale) {
      super(vector);
      this.vector = vector;
    }

    //TODO: samarth not sure this is efficient or correct
    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      if (isNullAt(rowId)) return null;
      int unscaledValue = dictionary.decodeToInt(vector.get(rowId));
      return Decimal.apply(unscaledValue, precision, scale);
    }
  }
}
