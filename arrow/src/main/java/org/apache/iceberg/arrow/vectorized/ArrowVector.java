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

package org.apache.iceberg.arrow.vectorized;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.iceberg.types.Types;
import org.apache.parquet.Preconditions;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.PrimitiveType;

/**
 * This class is inspired by Spark's {@code ColumnVector}.
 * This class represents the column data for an Iceberg table query.
 * It wraps an arrow {@link FieldVector} and provides simple
 * accessors for the row values. Advanced users can access
 * the {@link FieldVector}.
 * <p>
 *   Supported Iceberg data types:
 *   <ul>
 *     <li>{@link Types.BooleanType}</li>
 *     <li>{@link Types.IntegerType}</li>
 *     <li>{@link Types.LongType}</li>
 *     <li>{@link Types.FloatType}</li>
 *     <li>{@link Types.DoubleType}</li>
 *     <li>{@link Types.StringType}</li>
 *     <li>{@link Types.BinaryType}</li>
 *     <li>{@link Types.TimestampType} (with and without timezone)</li>
 *     <li>{@link Types.DateType}</li>
 *   </ul>
 */
public class ArrowVector implements AutoCloseable {
  private final VectorHolder vectorHolder;
  private final ArrowVectorAccessor accessor;

  ArrowVector(VectorHolder vectorHolder) {
    this.vectorHolder = vectorHolder;
    this.accessor = getVectorAccessor(vectorHolder);
  }

  public FieldVector getFieldVector() {
    // TODO Convert dictionary encoded vectors to correctly typed arrow vector.
    //   e.g. convert long dictionary encoded vector to a BigIntVector.
    return vectorHolder.vector();
  }

  public boolean hasNull() {
    return accessor.getNullCount() > 0;
  }

  public int numNulls() {
    return accessor.getNullCount();
  }

  @Override
  public void close() {
    accessor.close();
  }

  public boolean isNullAt(int rowId) {
    return accessor.isNullAt(rowId);
  }

  public boolean getBoolean(int rowId) {
    return accessor.getBoolean(rowId);
  }

  public int getInt(int rowId) {
    return accessor.getInt(rowId);
  }

  public long getLong(int rowId) {
    return accessor.getLong(rowId);
  }

  public float getFloat(int rowId) {
    return accessor.getFloat(rowId);
  }

  public double getDouble(int rowId) {
    return accessor.getDouble(rowId);
  }

  public String getString(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getString(rowId);
  }

  public byte[] getBinary(int rowId) {
    if (isNullAt(rowId)) {
      return null;
    }
    return accessor.getBinary(rowId);
  }

  private static ArrowVectorAccessor getVectorAccessor(VectorHolder holder) {
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

  private static ArrowVectorAccessor getDictionaryVectorAccessor(
      Dictionary dictionary,
      ColumnDescriptor desc,
      FieldVector vector,
      PrimitiveType primitive) {
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
    } else if (vector instanceof VarCharVector) {
      return new StringAccessor((VarCharVector) vector);
    } else if (vector instanceof VarBinaryVector) {
      return new BinaryAccessor((VarBinaryVector) vector);
    } else if (vector instanceof DateDayVector) {
      return new DateAccessor((DateDayVector) vector);
    } else if (vector instanceof TimeStampMicroTZVector) {
      return new TimestampMicroTzAccessor((TimeStampMicroTZVector) vector);
    } else if (vector instanceof TimeStampMicroVector) {
      return new TimestampMicroAccessor((TimeStampMicroVector) vector);
    }
    throw new UnsupportedOperationException("Unsupported vector: " + vector.getClass());
  }

  private abstract static class ArrowVectorAccessor {

    private final ValueVector vector;

    ArrowVectorAccessor(ValueVector vector) {
      this.vector = vector;
    }

    boolean isNullAt(int rowId) {
      return vector.isNull(rowId);
    }

    final int getNullCount() {
      return vector.getNullCount();
    }

    final void close() {
      vector.close();
    }

    boolean getBoolean(int rowId) {
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

    String getString(int rowId) {
      throw new UnsupportedOperationException();
    }

    byte[] getBinary(int rowId) {
      throw new UnsupportedOperationException();
    }
  }

  private static class BooleanAccessor extends ArrowVectorAccessor {

    private final BitVector accessor;

    BooleanAccessor(BitVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final boolean getBoolean(int rowId) {
      return accessor.get(rowId) == 1;
    }
  }

  private static class IntAccessor extends ArrowVectorAccessor {

    private final IntVector accessor;

    IntAccessor(IntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class LongAccessor extends ArrowVectorAccessor {

    private final BigIntVector accessor;

    LongAccessor(BigIntVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class FloatAccessor extends ArrowVectorAccessor {

    private final Float4Vector accessor;

    FloatAccessor(Float4Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final float getFloat(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class DoubleAccessor extends ArrowVectorAccessor {

    private final Float8Vector accessor;

    DoubleAccessor(Float8Vector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final double getDouble(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class StringAccessor extends ArrowVectorAccessor {

    private final VarCharVector accessor;
    private final NullableVarCharHolder stringResult = new NullableVarCharHolder();

    StringAccessor(VarCharVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final String getString(int rowId) {
      accessor.get(rowId, stringResult);
      if (stringResult.isSet == 0) {
        return null;
      } else {
        int strlen = stringResult.end - stringResult.start + 1;
        byte[] dst = new byte[strlen];
        stringResult.buffer.getBytes(stringResult.start, dst, 0, strlen);
        return new String(dst, StandardCharsets.UTF_8);
      }
    }
  }

  private static class BinaryAccessor extends ArrowVectorAccessor {

    private final VarBinaryVector accessor;

    BinaryAccessor(VarBinaryVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final byte[] getBinary(int rowId) {
      return accessor.getObject(rowId);
    }
  }

  private static class DateAccessor extends ArrowVectorAccessor {

    private final DateDayVector accessor;

    DateAccessor(DateDayVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final int getInt(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampMicroTzAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroTZVector accessor;

    TimestampMicroTzAccessor(TimeStampMicroTZVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
    }
  }

  private static class TimestampMicroAccessor extends ArrowVectorAccessor {

    private final TimeStampMicroVector accessor;

    TimestampMicroAccessor(TimeStampMicroVector vector) {
      super(vector);
      this.accessor = vector;
    }

    @Override
    final long getLong(int rowId) {
      return accessor.get(rowId);
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

  private static class DictionaryStringAccessor extends ArrowVectorAccessor {
    private final String[] decodedDictionary;
    private final IntVector offsetVector;

    DictionaryStringAccessor(IntVector vector, Dictionary dictionary) {
      super(vector);
      this.offsetVector = vector;
      this.decodedDictionary = IntStream.rangeClosed(0, dictionary.getMaxId())
          .mapToObj(dictionary::decodeToBinary)
          .map(binary -> new String(binary.getBytes(), StandardCharsets.UTF_8))
          .toArray(String[]::new);
    }

    @Override
    final String getString(int rowId) {
      int offset = offsetVector.get(rowId);
      return decodedDictionary[offset];
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
}
