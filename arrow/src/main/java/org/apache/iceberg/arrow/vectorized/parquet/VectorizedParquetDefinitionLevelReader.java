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
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;

public final class VectorizedParquetDefinitionLevelReader
    extends BaseVectorizedParquetValuesReader {

  public VectorizedParquetDefinitionLevelReader(
      int bitWidth, int maxDefLevel, boolean setArrowValidityVector) {
    super(bitWidth, maxDefLevel, setArrowValidityVector);
  }

  public VectorizedParquetDefinitionLevelReader(
      int bitWidth, int maxDefLevel, boolean readLength, boolean setArrowValidityVector) {
    super(bitWidth, maxDefLevel, readLength, setArrowValidityVector);
  }

  @FunctionalInterface
  interface ReaderFunction {
    void apply(Mode mode, int idx, int numValues, byte[] byteArray, ArrowBuf validityBuffer);
  }

  @FunctionalInterface
  interface SkipValuesFunction {
    void apply(Mode mode, int total);
  }

  abstract class CommonReader {
    private void nextBatch(
        final FieldVector vector,
        final int typeWidth,
        ReaderFunction consumer,
        SkipValuesFunction skipValuesFunction,
        ParquetReadState state) {
      int leftInBatch = state.getRowsToReadInBatch();
      int leftInPage = state.getValuesToReadInPage();
      long rowId = state.getCurrentRowIndex();
      int rowsWithSkipsInThisBatch = 0;
      while (leftInBatch > 0 && leftInPage > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(leftInBatch, Math.min(leftInPage, currentCount));
        long rangeStart = state.currentRangeStart();
        long rangeEnd = state.currentRangeEnd();

        byte[] byteArray = null;
        if (typeWidth > -1) {
          byteArray = new byte[typeWidth];
        }
        ArrowBuf validityBuffer = vector.getValidityBuffer();
        // If [rowId, rowId + numValues) is wholly before or after the current row range,
        // we skip to the start of the current row range or advance the current row range
        if (rowId + numValues < rangeStart) {
          skipValuesFunction.apply(mode, numValues);
          rowId += numValues;
          leftInPage -= numValues;
          rowsWithSkipsInThisBatch += numValues;
        } else if (rowId > rangeEnd) {
          state.nextRange();
        } else {
          // The range [rowId, rowId + numValues) overlaps with the current row range in state
          long start = Math.max(rangeStart, rowId);
          long end = Math.min(rangeEnd, rowId + numValues - 1);
          // Skip the part [rowId, start)
          int toSkip = (int) (start - rowId);
          if (toSkip > 0) {
            skipValuesFunction.apply(mode, toSkip);
            rowId += toSkip;
            leftInPage -= toSkip;
            rowsWithSkipsInThisBatch += toSkip;
          }
          // Read the part [start, end]
          numValues = (int) (end - start + 1);
          consumer.apply(mode, state.getValueOffset(), numValues, byteArray, validityBuffer);
          state.setValueOffset(state.getValueOffset() + numValues);
          leftInBatch -= numValues;
          leftInPage -= numValues;
          rowId += numValues;
          currentCount -= numValues;
          state.setRowsToReadInBatch(state.getRowsToReadInBatch() - numValues);
          rowsWithSkipsInThisBatch += numValues;
        }
      }
      state.setValuesToReadInPage(leftInPage);
      state.setCurrentRowIndex(rowId);
      state.setRowsWithSkipsInThisBatch(rowsWithSkipsInThisBatch);
    }

    public void nextBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        ParquetReadState readState) {
      nextBatch(
          vector,
          typeWidth,
          (mode, idx, numValues, byteArray, validityBuffer) -> {
            switch (mode) {
              case RLE:
                nextRleBatch(
                    vector, typeWidth, nullabilityHolder, valuesReader, idx, numValues, byteArray);
                break;
              case PACKED:
                nextPackedBatch(
                    vector, typeWidth, nullabilityHolder, valuesReader, idx, numValues, byteArray);
            }
          },
          (mode, n) -> skipValues(mode, (ValuesReader) valuesReader, n, typeWidth),
          readState);
    }

    public void nextDictEncodedBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        ParquetReadState readState) {
      nextBatch(
          vector,
          typeWidth,
          (mode, idx, numValues, byteArray, validityBuffer) -> {
            switch (mode) {
              case RLE:
                nextRleDictEncodedBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    valuesReader,
                    dict,
                    idx,
                    numValues,
                    validityBuffer);
                break;
              case PACKED:
                nextPackedDictEncodedBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    valuesReader,
                    dict,
                    idx,
                    numValues,
                    validityBuffer);
            }
          },
          (mode, n) -> skipValues(mode, valuesReader, n, typeWidth),
          readState);
    }

    protected abstract void nextRleBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray);

    protected abstract void nextPackedBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray);

    protected void nextRleDictEncodedBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        int idx,
        int numValues,
        ArrowBuf validityBuffer) {
      if (currentValue == maxDefLevel) {
        nextDictEncodedVal(
            vector, idx, valuesReader, dict, mode, numValues, nullabilityHolder, typeWidth);
      } else {
        setNulls(nullabilityHolder, idx, numValues, validityBuffer);
      }
    }

    protected void nextPackedDictEncodedBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        int idx,
        int numValues,
        ArrowBuf validityBuffer) {
      int bufferIdx = idx;
      for (int i = 0; i < numValues; i++) {
        if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
          nextDictEncodedVal(
              vector, bufferIdx, valuesReader, dict, mode, numValues, nullabilityHolder, typeWidth);
          nullabilityHolder.setNotNull(bufferIdx);
          if (setArrowValidityVector) {
            BitVectorHelper.setBit(vector.getValidityBuffer(), bufferIdx);
          }
        } else {
          setNull(nullabilityHolder, bufferIdx, validityBuffer);
        }
        bufferIdx++;
      }
    }

    protected abstract void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth);

    /**
     * Skip the next `n` values (either null or non-null) from this definition level reader and
     * `valueReader`.
     */
    public void skipValues(Mode mode, ValuesReader valuesReader, int total, int typeWidth) {
      int left = total;
      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int num = Math.min(left, currentCount);
        switch (mode) {
          case RLE:
            // we only need to skip non-null values from `valuesReader` since nulls are represented
            // via definition levels which are skipped here via decrementing `currentCount`.
            if (currentValue == maxDefLevel) {
              if (valuesReader instanceof VectorizedValuesReader) {
                skipValues((VectorizedValuesReader) valuesReader, num, typeWidth);
              } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
                ((VectorizedDictionaryEncodedParquetValuesReader) valuesReader).skipValues(num);
              }
            }
            break;
          case PACKED:
            int totalSkipNum = 0;
            for (int i = 0; i < num; ++i) {
              // same as above, only skip non-null values from `valuesReader`
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                ++totalSkipNum;
              }
            }
            if (valuesReader instanceof VectorizedValuesReader) {
              skipValues((VectorizedValuesReader) valuesReader, totalSkipNum, typeWidth);
            } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
              ((VectorizedDictionaryEncodedParquetValuesReader) valuesReader).skipValues(num);
            }
            break;
        }
        currentCount -= num;
        left -= num;
      }
    }

    public abstract void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth);
  }

  abstract class NumericBaseReader extends CommonReader {
    @Override
    protected void nextRleBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      setNextNValuesInVector(nullabilityHolder, valuesReader, idx, vector, numValues);
    }

    @Override
    protected void nextPackedBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      int bufferIdx = idx;
      for (int i = 0; i < numValues; ++i) {
        if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
          nextVal(vector, bufferIdx * typeWidth, valuesReader, mode);
          nullabilityHolder.setNotNull(bufferIdx);
          if (setArrowValidityVector) {
            BitVectorHelper.setBit(vector.getValidityBuffer(), bufferIdx);
          }
        } else {
          setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
        }
        bufferIdx++;
      }
    }

    protected abstract void nextVal(
        FieldVector vector, int idx, VectorizedValuesReader valuesReader, Mode mode);

    public abstract void nextVals(
        FieldVector vector, int rowId, VectorizedValuesReader valuesReader, int total);

    private void setNextNValuesInVector(
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int bufferIdx,
        FieldVector vector,
        int numValues) {
      ArrowBuf validityBuffer = vector.getValidityBuffer();
      if (currentValue == maxDefLevel) {
        nextVals(vector, bufferIdx, valuesReader, numValues);
        nullabilityHolder.setNotNulls(bufferIdx, numValues);
        if (setArrowValidityVector) {
          for (int i = 0; i < numValues; i++) {
            BitVectorHelper.setBit(validityBuffer, bufferIdx + i);
          }
        }
      } else {
        setNulls(nullabilityHolder, bufferIdx, numValues, validityBuffer);
      }
    }
  }

  class LongReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, VectorizedValuesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setLong(idx, valuesReader.readLong());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader.longDictEncodedReader().nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setLong((long) idx * typeWidth, dict.decodeToLong(reader.readInteger()));
      }
    }

    @Override
    public void nextVals(
        FieldVector vector, int rowId, VectorizedValuesReader valuesReader, int total) {
      valuesReader.readLongs(total, vector, rowId);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipLongs(total);
    }
  }

  class DoubleReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, VectorizedValuesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setDouble(idx, valuesReader.readDouble());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader.doubleDictEncodedReader().nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setDouble((long) idx * typeWidth, dict.decodeToDouble(reader.readInteger()));
      }
    }

    @Override
    public void nextVals(
        FieldVector vector, int rowId, VectorizedValuesReader valuesReader, int total) {
      valuesReader.readDoubles(total, vector, rowId);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipDoubles(total);
    }
  }

  class FloatReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, VectorizedValuesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setFloat(idx, valuesReader.readFloat());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader.floatDictEncodedReader().nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setFloat((long) idx * typeWidth, dict.decodeToFloat(reader.readInteger()));
      }
    }

    @Override
    public void nextVals(
        FieldVector vector, int rowId, VectorizedValuesReader valuesReader, int total) {
      valuesReader.readFloats(total, vector, rowId);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipFloats(total);
    }
  }

  class IntegerReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, VectorizedValuesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setInt(idx, valuesReader.readInteger());
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader
            .integerDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setInt((long) idx * typeWidth, dict.decodeToInt(reader.readInteger()));
      }
    }

    @Override
    public void nextVals(
        FieldVector vector, int rowId, VectorizedValuesReader valuesReader, int total) {
      valuesReader.readIntegers(total, vector, rowId);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipIntegers(total);
    }
  }

  abstract class BaseReader extends CommonReader {
    @Override
    protected void nextRleBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      int bufferIdx = idx;
      if (currentValue == maxDefLevel) {
        for (int i = 0; i < numValues; i++) {
          nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
          nullabilityHolder.setNotNull(bufferIdx);
          if (setArrowValidityVector) {
            BitVectorHelper.setBit(vector.getValidityBuffer(), bufferIdx);
          }

          bufferIdx++;
        }
      } else {
        setNulls(nullabilityHolder, bufferIdx, numValues, vector.getValidityBuffer());
      }
    }

    @Override
    protected void nextPackedBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        VectorizedValuesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      int bufferIdx = idx;
      for (int i = 0; i < numValues; i++) {
        if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
          nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
          nullabilityHolder.setNotNull(bufferIdx);
          if (setArrowValidityVector) {
            BitVectorHelper.setBit(vector.getValidityBuffer(), bufferIdx);
          }
        } else {
          setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
        }
        bufferIdx++;
      }
    }

    protected abstract void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray);
  }

  class TimestampMillisReader extends BaseReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      vector.getDataBuffer().setLong((long) idx * typeWidth, valuesReader.readLong() * 1000);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipLongs(total);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader
            .timestampMillisDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setLong((long) idx * typeWidth, dict.decodeToLong(reader.readInteger()) * 1000);
      }
    }
  }

  class TimestampInt96Reader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      // 8 bytes (time of day nanos) + 4 bytes(julianDay) = 12 bytes
      ByteBuffer buffer = valuesReader.readBinary(12).toByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
      long timestampInt96 = ParquetUtil.extractTimestampInt96(buffer);
      vector.getDataBuffer().setLong((long) idx * typeWidth, timestampInt96);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipFixedSizeBinary(total, 12);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      switch (mode) {
        case RLE:
          reader
              .timestampInt96DictEncodedReader()
              .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
          break;
        case PACKED:
          ByteBuffer buffer =
              dict.decodeToBinary(reader.readInteger())
                  .toByteBuffer()
                  .order(ByteOrder.LITTLE_ENDIAN);
          long timestampInt96 = ParquetUtil.extractTimestampInt96(buffer);
          vector.getDataBuffer().setLong(idx, timestampInt96);
          break;
        default:
          throw new UnsupportedOperationException(
              "Unsupported mode for timestamp int96 reader: " + mode);
      }
    }
  }

  class FixedSizeBinaryReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      valuesReader.readBinary(typeWidth).toByteBuffer().get(byteArray, 0, typeWidth);
      ((FixedSizeBinaryVector) vector).set(idx, byteArray);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipFixedSizeBinary(total, typeWidth);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader
            .fixedSizeBinaryDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        byte[] bytes = dict.decodeToBinary(reader.readInteger()).getBytes();
        byte[] vectorBytes = new byte[typeWidth];
        System.arraycopy(bytes, 0, vectorBytes, 0, typeWidth);
        ((FixedSizeBinaryVector) vector).set(idx, vectorBytes);
      }
    }
  }

  class VarWidthReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      int len = valuesReader.readInteger();
      ByteBuffer buffer = valuesReader.readBinary(len).toByteBuffer();
      // Calling setValueLengthSafe takes care of allocating a larger buffer if
      // running out of space.
      ((BaseVariableWidthVector) vector).setValueLengthSafe(idx, len);
      int startOffset = ((BaseVariableWidthVector) vector).getStartOffset(idx);
      // It is possible that the data buffer was reallocated. So it is important to
      // not cache the data buffer reference but instead use vector.getDataBuffer().
      vector.getDataBuffer().setBytes(startOffset, buffer);
      // Similarly, we need to get the latest reference to the validity buffer as well
      // since reallocation changes reference of the validity buffers as well.
      if (setArrowValidityVector) {
        BitVectorHelper.setBit(vector.getValidityBuffer(), idx);
      }
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipBinary(total);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader
            .varWidthBinaryDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((BaseVariableWidthVector) vector)
            .setSafe(idx, dict.decodeToBinary(reader.readInteger()).getBytesUnsafe());
      }
    }
  }

  class BooleanReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      ((BitVector) vector).setSafe(idx, valuesReader.readBoolean() ? 1 : 0);
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      valuesReader.skipBooleans(total);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      throw new UnsupportedOperationException();
    }
  }

  class DictionaryIdReader extends BaseReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        VectorizedValuesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipValues(VectorizedValuesReader valuesReader, int total, int typeWidth) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader reader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        reader.dictionaryIdReader().nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector.getDataBuffer().setInt((long) idx * IntVector.TYPE_WIDTH, reader.readInteger());
      }
    }
  }

  private void setNull(
      NullabilityHolder nullabilityHolder, int bufferIdx, ArrowBuf validityBuffer) {
    nullabilityHolder.setNull(bufferIdx);
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBit(validityBuffer, bufferIdx, 0);
    }
  }

  private void setNulls(
      NullabilityHolder nullabilityHolder, int idx, int numValues, ArrowBuf validityBuffer) {
    nullabilityHolder.setNulls(idx, numValues);
    if (setArrowValidityVector) {
      for (int i = 0; i < numValues; i++) {
        BitVectorHelper.setValidityBit(validityBuffer, idx + i, 0);
      }
    }
  }

  LongReader longReader() {
    return new LongReader();
  }

  DoubleReader doubleReader() {
    return new DoubleReader();
  }

  FloatReader floatReader() {
    return new FloatReader();
  }

  IntegerReader integerReader() {
    return new IntegerReader();
  }

  TimestampMillisReader timestampMillisReader() {
    return new TimestampMillisReader();
  }

  TimestampInt96Reader timestampInt96Reader() {
    return new TimestampInt96Reader();
  }

  FixedSizeBinaryReader fixedSizeBinaryReader() {
    return new FixedSizeBinaryReader();
  }

  VarWidthReader varWidthReader() {
    return new VarWidthReader();
  }

  BooleanReader booleanReader() {
    return new BooleanReader();
  }

  DictionaryIdReader dictionaryIdReader() {
    return new DictionaryIdReader();
  }
}
