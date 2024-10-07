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
package org.apache.iceberg.arrow.vectorized.parquet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.column.Dictionary;

/**
 * This decoder reads Parquet dictionary encoded data in a vectorized fashion. Unlike other
 * vectorized readers, methods in this decoder don't need to read definition levels. In other words,
 * these methods are called when there are non-null values to be read.
 */
public class VectorizedDictionaryEncodedParquetValuesReader
    extends BaseVectorizedParquetValuesReader {

  public VectorizedDictionaryEncodedParquetValuesReader(
      int maxDefLevel, boolean setValidityVector) {
    super(maxDefLevel, setValidityVector);
  }

  abstract class BaseDictEncodedReader {
    public void nextBatch(
        FieldVector vector,
        int startOffset,
        int numValuesToRead,
        Dictionary dict,
        NullabilityHolder nullabilityHolder,
        int typeWidth) {
      int left = numValuesToRead;
      int idx = startOffset;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);
        for (int i = 0; i < numValues; i++) {
          if (Mode.RLE.equals(mode)) {
            nextVal(vector, dict, idx, currentValue, typeWidth);
          } else if (Mode.PACKED.equals(mode)) {
            nextVal(vector, dict, idx, packedValuesBuffer[packedValuesBufferIdx++], typeWidth);
          }
          nullabilityHolder.setNotNull(idx);
          if (setArrowValidityVector) {
            BitVectorHelper.setBit(vector.getValidityBuffer(), idx);
          }
          idx++;
        }
        left -= numValues;
        currentCount -= numValues;
      }
    }

    protected abstract void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth);
  }

  class DictionaryIdReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ((IntVector) vector).set(idx, currentVal);
    }
  }

  class LongDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      vector.getDataBuffer().setLong((long) idx * typeWidth, dict.decodeToLong(currentVal));
    }
  }

  class TimestampMillisDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      vector.getDataBuffer().setLong((long) idx * typeWidth, dict.decodeToLong(currentVal) * 1000);
    }
  }

  class TimestampInt96DictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ByteBuffer buffer =
          dict.decodeToBinary(currentVal).toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      long timestampInt96 = ParquetUtil.extractTimestampInt96(buffer);
      vector.getDataBuffer().setLong((long) idx * typeWidth, timestampInt96);
    }
  }

  class IntegerDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      vector.getDataBuffer().setInt((long) idx * typeWidth, dict.decodeToInt(currentVal));
    }
  }

  class FloatDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      vector.getDataBuffer().setFloat((long) idx * typeWidth, dict.decodeToFloat(currentVal));
    }
  }

  class DoubleDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      vector.getDataBuffer().setDouble((long) idx * typeWidth, dict.decodeToDouble(currentVal));
    }
  }

  class FixedWidthBinaryDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ByteBuffer buffer = dict.decodeToBinary(currentVal).toByteBuffer();
      vector.getDataBuffer().setBytes((long) idx * typeWidth, buffer);
    }
  }

  class FixedLengthDecimalDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      byte[] bytes = dict.decodeToBinary(currentVal).getBytesUnsafe();
      DecimalVectorUtil.setBigEndian((DecimalVector) vector, idx, bytes);
    }
  }

  class VarWidthBinaryDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ByteBuffer buffer = dict.decodeToBinary(currentVal).toByteBuffer();
      ((BaseVariableWidthVector) vector)
          .setSafe(
              idx,
              buffer.array(),
              buffer.position() + buffer.arrayOffset(),
              buffer.limit() - buffer.position());
    }
  }

  class IntBackedDecimalDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ((DecimalVector) vector).set(idx, dict.decodeToInt(currentVal));
    }
  }

  class LongBackedDecimalDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      ((DecimalVector) vector).set(idx, dict.decodeToLong(currentVal));
    }
  }

  class FixedSizeBinaryDictEncodedReader extends BaseDictEncodedReader {
    @Override
    protected void nextVal(
        FieldVector vector, Dictionary dict, int idx, int currentVal, int typeWidth) {
      byte[] bytes = dict.decodeToBinary(currentVal).getBytesUnsafe();
      byte[] vectorBytes = new byte[typeWidth];
      System.arraycopy(bytes, 0, vectorBytes, 0, typeWidth);
      ((FixedSizeBinaryVector) vector).set(idx, vectorBytes);
    }
  }

  public DictionaryIdReader dictionaryIdReader() {
    return new DictionaryIdReader();
  }

  public LongDictEncodedReader longDictEncodedReader() {
    return new LongDictEncodedReader();
  }

  public TimestampMillisDictEncodedReader timestampMillisDictEncodedReader() {
    return new TimestampMillisDictEncodedReader();
  }

  public TimestampInt96DictEncodedReader timestampInt96DictEncodedReader() {
    return new TimestampInt96DictEncodedReader();
  }

  public IntegerDictEncodedReader integerDictEncodedReader() {
    return new IntegerDictEncodedReader();
  }

  public FloatDictEncodedReader floatDictEncodedReader() {
    return new FloatDictEncodedReader();
  }

  public DoubleDictEncodedReader doubleDictEncodedReader() {
    return new DoubleDictEncodedReader();
  }

  public FixedWidthBinaryDictEncodedReader fixedWidthBinaryDictEncodedReader() {
    return new FixedWidthBinaryDictEncodedReader();
  }

  public FixedLengthDecimalDictEncodedReader fixedLengthDecimalDictEncodedReader() {
    return new FixedLengthDecimalDictEncodedReader();
  }

  public VarWidthBinaryDictEncodedReader varWidthBinaryDictEncodedReader() {
    return new VarWidthBinaryDictEncodedReader();
  }

  public IntBackedDecimalDictEncodedReader intBackedDecimalDictEncodedReader() {
    return new IntBackedDecimalDictEncodedReader();
  }

  public LongBackedDecimalDictEncodedReader longBackedDecimalDictEncodedReader() {
    return new LongBackedDecimalDictEncodedReader();
  }

  public FixedSizeBinaryDictEncodedReader fixedSizeBinaryDictEncodedReader() {
    return new FixedSizeBinaryDictEncodedReader();
  }
}
