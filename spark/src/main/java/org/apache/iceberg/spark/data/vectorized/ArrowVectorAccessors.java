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

import java.math.BigInteger;
import java.util.stream.IntStream;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.util.DecimalUtility;
import org.apache.iceberg.arrow.vectorized.VectorHolder;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.unsafe.types.UTF8String;
import org.jetbrains.annotations.NotNull;

public class ArrowVectorAccessors {

  private ArrowVectorAccessors() {
  }

  static ArrowVectorAccessor getVectorAccessor(VectorHolder holder) {
    Dictionary dictionary = holder.dictionary();
    boolean isVectorDictEncoded = holder.isDictionaryEncoded();
    FieldVector vector = holder.vector();
    if (isVectorDictEncoded) {
      ColumnDescriptor desc = holder.descriptor();
      PrimitiveType primitive = desc.getPrimitiveType();
      return getDictionaryVectorAccessor(dictionary, desc, vector, primitive);
    } else {
      return getPlainVectorAccessor(vector);
    }
  }

  @NotNull
  private static ArrowVectorAccessor getDictionaryVectorAccessor(
      Dictionary dictionary,
      ColumnDescriptor desc,
      FieldVector vector, PrimitiveType primitive) {
    Preconditions.checkState(vector instanceof IntVector, "Dictionary ids should be stored in IntVectors only");
    if (primitive.getOriginalType() != null) {
      switch (desc.getPrimitiveType().getOriginalType()) {
        case ENUM:
        case JSON:
        case UTF8:
        case BSON:
          return new DictionaryStringAccessor((IntVector) vector, dictionary);
        case INT_64:
        case TIMESTAMP_MILLIS:
        case TIMESTAMP_MICROS:
          return new DictionaryLongAccessor((IntVector) vector, dictionary);
        case DECIMAL:
          switch (primitive.getPrimitiveTypeName()) {
            case BINARY:
            case FIXED_LEN_BYTE_ARRAY:
              return new DictionaryDecimalBinaryAccessor(
                  (IntVector) vector,
                  dictionary);
            case INT64:
              return new DictionaryDecimalLongAccessor(
                  (IntVector) vector,
                  dictionary);
            case INT32:
              return new DictionaryDecimalIntAccessor(
                  (IntVector) vector,
                  dictionary);
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
          return new DictionaryBinaryAccessor((IntVector) vector, dictionary);
        case FLOAT:
          return new DictionaryFloatAccessor((IntVector) vector, dictionary);
        case INT64:
          return new DictionaryLongAccessor((IntVector) vector, dictionary);
        case DOUBLE:
          return new DictionaryDoubleAccessor((IntVector) vector, dictionary);
        default:
          throw new UnsupportedOperationException("Unsupported type: " + primitive);
      }
    }
  }

  @NotNull
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private static ArrowVectorAccessor getPlainVectorAccessor(FieldVector vector) {
    if (vector instanceof BitVector) {
      return new BooleanAccessor((BitVector) vector);
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
    } else if (vector instanceof VarCharVector) {
      return new StringAccessor((VarCharVector) vector);
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
      return new StructAccessor(structVector);
    }
    throw new UnsupportedOperationException("Unsupported vector: " + vector.getClass());
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

    @Override
    final long getLong(int rowId) {
      return getInt(rowId);
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

  private static class DictionaryLongAccessor extends ArrowVectorAccessor {
    private final IntVector offsetVector;
    private final long[] decodedDictionary;

    DictionaryLongAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToLong(dictionary::decodeToLong)
          .toArray();
    }

    @Override
    final long getLong(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
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

    @Override
    final double getDouble(int rowId) {
      return getFloat(rowId);
    }
  }

  private static class DictionaryFloatAccessor extends ArrowVectorAccessor {
    private final IntVector offsetVector;
    private final float[] decodedDictionary;

    DictionaryFloatAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = new float[dictionary.getMaxId() + 1];
      for (int i = 0; i <= dictionary.getMaxId(); i++) {
        decodedDictionary[i] = dictionary.decodeToFloat(i);
      }
    }

    @Override
    final float getFloat(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
    }

    @Override
    final double getDouble(int rowId) {
      return getFloat(rowId);
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

  private static class DictionaryDoubleAccessor extends ArrowVectorAccessor {
    private final IntVector offsetVector;
    private final double[] decodedDictionary;

    DictionaryDoubleAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToDouble(dictionary::decodeToDouble)
          .toArray();
    }

    @Override
    final double getDouble(int rowId) {
      return decodedDictionary[offsetVector.get(rowId)];
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector vector;

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.vector = vector;
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      int start = vector.getStartOffset(rowId);
      int end = vector.getEndOffset(rowId);

      return UTF8String.fromAddress(
          null,
          vector.getDataBuffer().memoryAddress() + start,
          end - start);
    }
  }

  private static class DictionaryStringAccessor extends ArrowVectorAccessor {
    private final UTF8String[] decodedDictionary;
    private final IntVector offsetVector;

    DictionaryStringAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToObj(dictionary::decodeToBinary)
          .map(binary -> UTF8String.fromBytes(binary.getBytes()))
          .toArray(UTF8String[]::new);
    }

    @Override
    final UTF8String getUTF8String(int rowId) {
      int offset = offsetVector.get(rowId);
      return decodedDictionary[offset];
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
      return vector.get(rowId);
    }
  }

  private static class DictionaryBinaryAccessor extends ArrowVectorAccessor {
    private final IntVector offsetVector;
    private final byte[][] decodedDictionary;

    DictionaryBinaryAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToObj(dictionary::decodeToBinary)
          .map(Binary::getBytes)
          .toArray(byte[][]::new);
    }

    @Override
    final byte[] getBinary(int rowId) {
      int offset = offsetVector.get(rowId);
      return decodedDictionary[offset];
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
    final ColumnarArray getArray(int rowId) {
      ArrowBuf offsets = vector.getOffsetBuffer();
      int index = rowId * ListVector.OFFSET_WIDTH;
      int start = offsets.getInt(index);
      int end = offsets.getInt(index + ListVector.OFFSET_WIDTH);
      return new ColumnarArray(arrayData, start, end - start);
    }
  }

  /**
   * Use {@link IcebergArrowColumnVector#getChild(int)} to get hold of the {@link ArrowColumnVector} vectors holding the
   * struct values.
   */
  private static class StructAccessor extends ArrowVectorAccessor {
    StructAccessor(StructVector structVector) {
      super(structVector, IntStream.range(0, structVector.size())
          .mapToObj(structVector::getVectorById)
          .map(ArrowColumnVector::new)
          .toArray(ArrowColumnVector[]::new));
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
      return Decimal.apply(DecimalUtility.getBigDecimalFromArrowBuf(vector.getDataBuffer(), rowId, scale),
          precision, scale);
    }
  }

  @SuppressWarnings("checkstyle:VisibilityModifier")
  private abstract static class DictionaryDecimalAccessor extends ArrowVectorAccessor {
    final Decimal[] cache;
    Dictionary parquetDictionary;
    final IntVector offsetVector;

    private DictionaryDecimalAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.parquetDictionary = dictionary;
      this.cache = new Decimal[dictionary.getMaxId() + 1];
    }
  }

  private static class DictionaryDecimalBinaryAccessor extends DictionaryDecimalAccessor {

    DictionaryDecimalBinaryAccessor(IntVector vector, Dictionary dictionary) {
      super(vector, dictionary);
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      int dictId = offsetVector.get(rowId);
      if (cache[dictId] == null) {
        cache[dictId] = Decimal.apply(
            new BigInteger(parquetDictionary.decodeToBinary(dictId).getBytes()).longValue(),
            precision,
            scale);
      }
      return cache[dictId];
    }
  }

  private static class DictionaryDecimalLongAccessor extends DictionaryDecimalAccessor {

    DictionaryDecimalLongAccessor(IntVector vector, Dictionary dictionary) {
      super(vector, dictionary);
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      int dictId = offsetVector.get(rowId);
      if (cache[dictId] == null) {
        cache[dictId] = Decimal.apply(parquetDictionary.decodeToLong(dictId), precision, scale);
      }
      return cache[dictId];
    }
  }

  private static class DictionaryDecimalIntAccessor extends DictionaryDecimalAccessor {

    DictionaryDecimalIntAccessor(IntVector vector, Dictionary dictionary) {
      super(vector, dictionary);
    }

    @Override
    final Decimal getDecimal(int rowId, int precision, int scale) {
      int dictId = offsetVector.get(rowId);
      if (cache[dictId] == null) {
        cache[dictId] = Decimal.apply(parquetDictionary.decodeToInt(dictId), precision, scale);
      }
      return cache[dictId];
    }
  }
}
