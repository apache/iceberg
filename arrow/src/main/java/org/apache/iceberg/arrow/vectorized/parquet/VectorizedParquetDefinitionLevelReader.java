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
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedSizeBinaryVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.arrow.vectorized.parquet.VectorizedColumnIterator.ReadState;
import org.apache.iceberg.parquet.ParquetUtil;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
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

  abstract class CommonBaseReader {

    private void nextCommonBatch(
        final FieldVector vector,
        final int typeWidth,
        final int numValsToRead,
        NullabilityHolder nullabilityHolder,
        ValuesReader valuesReader,
        Dictionary dict,
        ReadState readState) {
      int idx = readState.currentOffset();
      int left = numValsToRead;
      long rowIdx = readState.currentRowIndex();

      while (left > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int numValues = Math.min(left, currentCount);

        byte[] byteArray = null;
        if (typeWidth > -1) {
          byteArray = new byte[typeWidth];
        }
        ArrowBuf validityBuffer = vector.getValidityBuffer();

        long rangeStart = readState.currentRangeStart();
        long rangeEnd = readState.currentRangeEnd();

        // If [rowIdx, rowIdx + numValues) is wholly before or after the current row range,
        // we skip to the start of the current row range or advance the current row range
        if (rowIdx + numValues < rangeStart) {
          skipValues(numValues, typeWidth, valuesReader);
          rowIdx += numValues;
          left -= numValues;
        } else if (rowIdx > rangeEnd) {
          readState.nextRange();
        } else {
          // [rowIdx, rowIdx + numValues) overlaps with the current row range
          long start = Math.max(rangeStart, rowIdx);
          long end = Math.min(rangeEnd, rowIdx + numValues - 1);

          // skip [rowIdx, start)
          int toSkip = (int) (start - rowIdx);
          if (toSkip > 0) {
            skipValues(toSkip, typeWidth, valuesReader);
            rowIdx += toSkip;
            left -= toSkip;
          }

          // read [start, end]
          numValues = (int) (end - start + 1);

          switch (mode) {
            case RLE:
              if (valuesReader instanceof ValuesAsBytesReader) {
                nextRleBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    (ValuesAsBytesReader) valuesReader,
                    idx,
                    numValues,
                    byteArray);
              } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
                nextRleDictEncodedBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    (VectorizedDictionaryEncodedParquetValuesReader) valuesReader,
                    dict,
                    idx,
                    numValues,
                    validityBuffer);
              }
              idx += numValues;
              break;
            case PACKED:
              if (valuesReader instanceof ValuesAsBytesReader) {
                nextPackedBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    (ValuesAsBytesReader) valuesReader,
                    idx,
                    numValues,
                    byteArray);
              } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
                nextPackedDictEncodedBatch(
                    vector,
                    typeWidth,
                    nullabilityHolder,
                    (VectorizedDictionaryEncodedParquetValuesReader) valuesReader,
                    dict,
                    idx,
                    numValues,
                    validityBuffer);
              }
              idx += numValues;
              break;
          }
          rowIdx += numValues;
          left -= numValues;
          currentCount -= numValues;
        }
      }
      readState.advanceOffsetAndRowIndex(idx, rowIdx);
    }

    private void skipValues(int numValuesToSkip, int typeWidth, ValuesReader valuesReader) {
      int numValues = numValuesToSkip;
      while (numValues > 0) {
        if (currentCount == 0) {
          readNextGroup();
        }
        int num = Math.min(numValues, currentCount);
        switch (mode) {
          case RLE:
            // we only need to skip non-null values from `valuesReader` since nulls are represented
            // via definition levels which are skipped here via decrementing `currentCount`.
            if (currentValue == maxDefLevel) {
              if (valuesReader instanceof ValuesAsBytesReader) {
                for (int i = 0; i < num; i++) {
                  skipVal((ValuesAsBytesReader) valuesReader, typeWidth);
                }
              } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
                ((VectorizedDictionaryEncodedParquetValuesReader) valuesReader).skipValues(num);
              }
            }
            break;
          case PACKED:
            for (int i = 0; i < num; ++i) {
              // same as above, only skip non-null values from `valuesReader`
              if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
                if (valuesReader instanceof ValuesAsBytesReader) {
                  skipVal((ValuesAsBytesReader) valuesReader, typeWidth);
                } else if (valuesReader instanceof VectorizedDictionaryEncodedParquetValuesReader) {
                  ((VectorizedDictionaryEncodedParquetValuesReader) valuesReader).skipValues(1);
                }
              }
            }
            break;
        }
        currentCount -= num;
        numValues -= num;
      }
    }

    public void nextBatch(
        final FieldVector vector,
        final int typeWidth,
        final int numValsToRead,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
        ReadState readState) {
      nextCommonBatch(
          vector, typeWidth, numValsToRead, nullabilityHolder, valuesReader, null, readState);
    }

    public void nextDictEncodedBatch(
        final FieldVector vector,
        final int typeWidth,
        final int numValsToRead,
        NullabilityHolder nullabilityHolder,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        ReadState readState) {
      nextCommonBatch(
          vector, typeWidth, numValsToRead, nullabilityHolder, valuesReader, dict, readState);
    }

    protected abstract void nextRleBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray);

    protected abstract void nextPackedBatch(
        FieldVector vector,
        int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
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

    protected abstract void skipVal(ValuesAsBytesReader valuesReader, int typeWidth);
  }

  abstract class NumericBaseReader extends CommonBaseReader {

    @Override
    protected void nextRleBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      setNextNValuesInVector(typeWidth, nullabilityHolder, valuesReader, idx, vector, numValues);
    }

    @Override
    protected void nextPackedBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
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
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode);
  }

  class LongReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setLong(idx, valuesReader.readLong());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(8);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        valuesReader
            .longDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setLong((long) idx * typeWidth, dict.decodeToLong(valuesReader.readInteger()));
      }
    }
  }

  class DoubleReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setDouble(idx, valuesReader.readDouble());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(8);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        valuesReader
            .doubleDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setDouble((long) idx * typeWidth, dict.decodeToDouble(valuesReader.readInteger()));
      }
    }
  }

  class FloatReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setFloat(idx, valuesReader.readFloat());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(4);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        valuesReader
            .floatDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setFloat((long) idx * typeWidth, dict.decodeToFloat(valuesReader.readInteger()));
      }
    }
  }

  class IntegerReader extends NumericBaseReader {
    @Override
    protected void nextVal(
        FieldVector vector, int idx, ValuesAsBytesReader valuesReader, Mode mode) {
      vector.getDataBuffer().setInt(idx, valuesReader.readInteger());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(4);
    }

    @Override
    protected void nextDictEncodedVal(
        FieldVector vector,
        int idx,
        VectorizedDictionaryEncodedParquetValuesReader valuesReader,
        Dictionary dict,
        Mode mode,
        int numValues,
        NullabilityHolder holder,
        int typeWidth) {
      if (Mode.RLE.equals(mode)) {
        valuesReader
            .integerDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        vector
            .getDataBuffer()
            .setInt((long) idx * typeWidth, dict.decodeToInt(valuesReader.readInteger()));
      }
    }
  }

  abstract class BaseReader extends CommonBaseReader {

    @Override
    protected void nextRleBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      int bufferIdx = idx;
      if (currentValue == maxDefLevel) {
        for (int i = 0; i < numValues; i++) {
          nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
          nullabilityHolder.setNotNull(bufferIdx);
          bufferIdx++;
        }
      } else {
        setNulls(nullabilityHolder, bufferIdx, numValues, vector.getValidityBuffer());
      }
    }

    @Override
    protected void nextPackedBatch(
        final FieldVector vector,
        final int typeWidth,
        NullabilityHolder nullabilityHolder,
        ValuesAsBytesReader valuesReader,
        int idx,
        int numValues,
        byte[] byteArray) {
      int bufferIdx = idx;
      for (int i = 0; i < numValues; i++) {
        if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
          nextVal(vector, bufferIdx, valuesReader, typeWidth, byteArray);
          nullabilityHolder.setNotNull(bufferIdx);
        } else {
          setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
        }
        bufferIdx++;
      }
    }

    protected abstract void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray);
  }

  class TimestampMillisReader extends BaseReader {

    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      vector.getDataBuffer().setLong((long) idx * typeWidth, valuesReader.readLong() * 1000);
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(8);
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
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      // 8 bytes (time of day nanos) + 4 bytes(julianDay) = 12 bytes
      ByteBuffer buffer = valuesReader.getBuffer(12).order(ByteOrder.LITTLE_ENDIAN);
      long timestampInt96 = ParquetUtil.extractTimestampInt96(buffer);
      vector.getDataBuffer().setLong((long) idx * typeWidth, timestampInt96);
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(12);
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

  class FixedWidthBinaryReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
      ((VarBinaryVector) vector)
          .setSafe(
              idx,
              buffer.array(),
              buffer.position() + buffer.arrayOffset(),
              buffer.limit() - buffer.position());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(typeWidth);
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
            .fixedWidthBinaryDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ByteBuffer buffer = dict.decodeToBinary(reader.readInteger()).toByteBuffer();
        vector.getDataBuffer().setBytes((long) idx * typeWidth, buffer);
      }
    }
  }

  class FixedLengthDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
      DecimalVectorUtil.setBigEndian((DecimalVector) vector, idx, byteArray);
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(typeWidth);
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
            .fixedLengthDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        byte[] bytes = dict.decodeToBinary(reader.readInteger()).getBytesUnsafe();
        DecimalVectorUtil.setBigEndian((DecimalVector) vector, idx, bytes);
      }
    }
  }

  class FixedSizeBinaryReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
      ((FixedSizeBinaryVector) vector).set(idx, byteArray);
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(typeWidth);
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
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      int len = valuesReader.readInteger();
      ByteBuffer buffer = valuesReader.getBuffer(len);
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
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      int len = valuesReader.readInteger();
      valuesReader.skipBytes(len);
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

  class IntBackedDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      ((DecimalVector) vector).set(idx, valuesReader.getBuffer(Integer.BYTES).getInt());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(Integer.BYTES);
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
            .intBackedDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((DecimalVector) vector).set(idx, dict.decodeToInt(reader.readInteger()));
      }
    }
  }

  class LongBackedDecimalReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      ((DecimalVector) vector).set(idx, valuesReader.getBuffer(Long.BYTES).getLong());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.skipBytes(Long.BYTES);
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
            .longBackedDecimalDictEncodedReader()
            .nextBatch(vector, idx, numValues, dict, holder, typeWidth);
      } else if (Mode.PACKED.equals(mode)) {
        ((DecimalVector) vector).set(idx, dict.decodeToLong(reader.readInteger()));
      }
    }
  }

  class BooleanReader extends BaseReader {
    @Override
    protected void nextVal(
        FieldVector vector,
        int idx,
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      ((BitVector) vector).setSafe(idx, valuesReader.readBooleanAsInt());
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
      valuesReader.readBooleanAsInt();
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
        ValuesAsBytesReader valuesReader,
        int typeWidth,
        byte[] byteArray) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected void skipVal(ValuesAsBytesReader valuesReader, int typeWidth) {
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

  private void setNextNValuesInVector(
      int typeWidth,
      NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader,
      int bufferIdx,
      FieldVector vector,
      int numValues) {
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    if (currentValue == maxDefLevel) {
      ByteBuffer buffer = valuesReader.getBuffer(numValues * typeWidth);
      vector.getDataBuffer().setBytes((long) bufferIdx * typeWidth, buffer);
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

  FixedWidthBinaryReader fixedWidthBinaryReader() {
    return new FixedWidthBinaryReader();
  }

  FixedLengthDecimalReader fixedLengthDecimalReader() {
    return new FixedLengthDecimalReader();
  }

  FixedSizeBinaryReader fixedSizeBinaryReader() {
    return new FixedSizeBinaryReader();
  }

  VarWidthReader varWidthReader() {
    return new VarWidthReader();
  }

  IntBackedDecimalReader intBackedDecimalReader() {
    return new IntBackedDecimalReader();
  }

  LongBackedDecimalReader longBackedDecimalReader() {
    return new LongBackedDecimalReader();
  }

  BooleanReader booleanReader() {
    return new BooleanReader();
  }

  DictionaryIdReader dictionaryIdReader() {
    return new DictionaryIdReader();
  }
}
