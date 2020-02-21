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

import io.netty.buffer.ArrowBuf;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.column.Dictionary;

public final class VectorizedParquetValuesReader extends BaseVectorizedParquetValuesReader {

  public VectorizedParquetValuesReader(int bitWidth, int maxDefLevel, boolean setArrowValidityVector) {
    super(bitWidth, maxDefLevel, setArrowValidityVector);
  }

  public void readBatchOfDictionaryIds(
      final IntVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryIds(vector, idx, numValues, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, numValues, vector.getValidityBuffer());
          }
          idx += numValues;
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.set(idx, dictionaryEncodedValuesReader.readInteger());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  public void readBatchOfLongs(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          setNextNValuesInVector(
              typeWidth,
              nullabilityHolder,
              valuesReader,
              bufferIdx,
              vector,
              numValues);
          bufferIdx += numValues;
          break;
        case PACKED:
          for (int i = 0; i < numValues; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setLong(bufferIdx * typeWidth, valuesReader.readLong());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
              } else {
                nullabilityHolder.setNotNull(bufferIdx);
              }
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  public void readBatchOfDictionaryEncodedLongs(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      ArrowBuf validityBuffer = vector.getValidityBuffer();
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedLongs(vector,
                idx, numValues, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, numValues, validityBuffer);
          }
          idx += numValues;
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setLong(idx, dict.decodeToLong(dictionaryEncodedValuesReader.readInteger()));
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, validityBuffer);
            }
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  public void readBatchOfIntegers(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          setNextNValuesInVector(
              typeWidth,
              nullabilityHolder,
              valuesReader,
              bufferIdx,
              vector,
              num);
          bufferIdx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setInt(bufferIdx * typeWidth, valuesReader.readInteger());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
              } else {
                nullabilityHolder.setNotNull(bufferIdx);
              }
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDictionaryEncodedIntegers(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedIntegers(vector, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setInt(idx, dict.decodeToInt(dictionaryEncodedValuesReader.readInteger()));
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfFloats(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          setNextNValuesInVector(
              typeWidth,
              nullabilityHolder,
              valuesReader,
              bufferIdx,
              vector,
              num);
          bufferIdx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setFloat(bufferIdx * typeWidth, valuesReader.readFloat());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
              } else {
                nullabilityHolder.setNotNull(bufferIdx);
              }
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDictionaryEncodedFloats(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      ArrowBuf validityBuffer = vector.getValidityBuffer();
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedFloats(vector, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, validityBuffer);
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setFloat(idx, dict.decodeToFloat(dictionaryEncodedValuesReader.readInteger()));
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, validityBuffer);
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDoubles(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          setNextNValuesInVector(
              typeWidth,
              nullabilityHolder,
              valuesReader,
              bufferIdx,
              vector,
              num);
          bufferIdx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setDouble(bufferIdx * typeWidth, valuesReader.readDouble());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(),  bufferIdx);
              } else {
                nullabilityHolder.setNotNull(bufferIdx);
              }
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDictionaryEncodedDoubles(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedDoubles(vector, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setDouble(idx, dict.decodeToDouble(dictionaryEncodedValuesReader.readInteger()));
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfFixedWidthBinary(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < num; i++) {
              setBinaryInVector((VarBinaryVector) vector, typeWidth, valuesReader, bufferIdx, nullabilityHolder);
              bufferIdx++;
            }
          } else {
            setNulls(nullabilityHolder, bufferIdx, num, vector.getValidityBuffer());
            bufferIdx += num;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              setBinaryInVector((VarBinaryVector) vector, typeWidth, valuesReader, bufferIdx, nullabilityHolder);
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDictionaryEncodedFixedWidthBinary(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedFixedWidthBinary(vector, typeWidth, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              ByteBuffer buffer = dict.decodeToBinary(dictionaryEncodedValuesReader.readInteger()).toByteBuffer();
              vector.getDataBuffer().setBytes(idx * typeWidth, buffer.array(),
                  buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
              if (setArrowValidityVector) {
                BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
              } else {
                nullabilityHolder.setNotNull(idx);
              }
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfFixedLengthDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < num; i++) {
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(bufferIdx, byteArray);
              nullabilityHolder.setNotNull(bufferIdx);
              bufferIdx++;
            }
          } else {
            setNulls(nullabilityHolder, bufferIdx, num, vector.getValidityBuffer());
            bufferIdx += num;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(bufferIdx, byteArray);
              nullabilityHolder.setNotNull(bufferIdx);
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfDictionaryEncodedFixedLengthDecimals(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedFixedLengthDecimals(vector, typeWidth, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              ByteBuffer decimalBytes = dict.decodeToBinary(dictionaryEncodedValuesReader.readInteger()).toByteBuffer();
              byte[] vectorBytes = new byte[DecimalVector.TYPE_WIDTH];
              System.arraycopy(decimalBytes, 0, vectorBytes, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
              nullabilityHolder.setNotNull(idx);
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP) This
   * method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow vector. It
   * appropriately sets the validity buffer in the Arrow vector.
   */
  public void readBatchVarWidth(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < num; i++) {
              setVarWidthBinaryValue(vector, valuesReader, bufferIdx, nullabilityHolder);
              bufferIdx++;
            }
          } else {
            setNulls(nullabilityHolder, bufferIdx, num, vector.getValidityBuffer());
            bufferIdx += num;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              setVarWidthBinaryValue(vector, valuesReader, bufferIdx, nullabilityHolder);
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  private void setVarWidthBinaryValue(FieldVector vector, ValuesAsBytesReader valuesReader,
                                      int bufferIdx, NullabilityHolder nullabilityHolder) {
    int len = valuesReader.readInteger();
    ByteBuffer buffer = valuesReader.getBuffer(len);
    // Calling setValueLengthSafe takes care of allocating a larger buffer if
    // running out of space.
    ((BaseVariableWidthVector) vector).setValueLengthSafe(bufferIdx, len);
    // It is possible that the data buffer was reallocated. So it is important to
    // not cache the data buffer reference but instead use vector.getDataBuffer().
    vector.getDataBuffer().writeBytes(buffer.array(), buffer.position() + buffer.arrayOffset(),
        buffer.limit() - buffer.position());
    // Similarly, we need to get the latest reference to the validity buffer as well
    // since reallocation changes reference of the validity buffers as well.
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
    } else {
      nullabilityHolder.setNotNull(bufferIdx);
    }
  }

  public void readBatchOfDictionaryEncodedVarWidth(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedVarWidthBinary(vector, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              ((BaseVariableWidthVector) vector).setSafe(
                  idx,
                  dict.decodeToBinary(dictionaryEncodedValuesReader.readInteger()).getBytesUnsafe());
              nullabilityHolder.setNotNull(idx);
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfIntLongBackedDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < num; i++) {
              setIntLongBackedDecimal(vector, typeWidth, nullabilityHolder, valuesReader, bufferIdx, byteArray);
              bufferIdx++;
            }
          } else {
            setNulls(nullabilityHolder, bufferIdx, num, vector.getValidityBuffer());
            bufferIdx += num;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              setIntLongBackedDecimal(vector, typeWidth, nullabilityHolder, valuesReader, bufferIdx, byteArray);
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  private void setIntLongBackedDecimal(FieldVector vector, int typeWidth, NullabilityHolder nullabilityHolder,
                                       ValuesAsBytesReader valuesReader, int bufferIdx, byte[] byteArray) {
    valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
    vector.getDataBuffer().setBytes(bufferIdx * DecimalVector.TYPE_WIDTH, byteArray);
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
    } else {
      nullabilityHolder.setNotNull(bufferIdx);
    }
  }

  public void readBatchOfDictionaryEncodedIntLongBackedDecimals(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedDictionaryEncodedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
    int idx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            dictionaryEncodedValuesReader.readBatchOfDictionaryEncodedIntLongBackedDecimals(vector, typeWidth, idx,
                num, dict, nullabilityHolder);
          } else {
            setNulls(nullabilityHolder, idx, num, vector.getValidityBuffer());
          }
          idx += num;
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              ((DecimalVector) vector).set(
                  idx,
                  typeWidth == Integer.BYTES ?
                      dict.decodeToInt(dictionaryEncodedValuesReader.readInteger())
                      : dict.decodeToLong(dictionaryEncodedValuesReader.readInteger()));
              nullabilityHolder.setNotNull(idx);
            } else {
              setNull(nullabilityHolder, idx, vector.getValidityBuffer());
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  public void readBatchOfBooleans(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < num; i++) {
              ((BitVector) vector).setSafe(bufferIdx, valuesReader.readBooleanAsInt());
              nullabilityHolder.setNotNull(bufferIdx);
              bufferIdx++;
            }
          } else {
            setNulls(nullabilityHolder, bufferIdx, num, vector.getValidityBuffer());
            bufferIdx += num;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; ++i) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              ((BitVector) vector).setSafe(bufferIdx, valuesReader.readBooleanAsInt());
              nullabilityHolder.setNotNull(bufferIdx);
            } else {
              setNull(nullabilityHolder, bufferIdx, vector.getValidityBuffer());
            }
            bufferIdx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  private static void setBinaryInVector(
      VarBinaryVector vector,
      int typeWidth,
      ValuesAsBytesReader valuesReader,
      int bufferIdx, NullabilityHolder nullabilityHolder) {
    ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
    vector.setSafe(bufferIdx, buffer.array(), buffer.position() + buffer.arrayOffset(),
        buffer.limit() - buffer.position());
    nullabilityHolder.setNotNull(bufferIdx);
  }

  private void setNextNValuesInVector(
      int typeWidth, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader, int bufferIdx, FieldVector vector, int numValues) {
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    if (currentValue == maxDefLevel) {
      ByteBuffer buffer = valuesReader.getBuffer(numValues * typeWidth);
      vector.getDataBuffer().setBytes(bufferIdx * typeWidth, buffer);
      if (setArrowValidityVector) {
        for (int i = 0; i < numValues; i++) {
          BitVectorHelper.setValidityBitToOne(validityBuffer, bufferIdx + i);
        }
      } else {
        nullabilityHolder.setNotNulls(bufferIdx, numValues);
      }
    } else {
      setNulls(nullabilityHolder, bufferIdx, numValues, validityBuffer);
    }
  }

  private void setNull(NullabilityHolder nullabilityHolder, int bufferIdx, ArrowBuf validityBuffer) {
    if (setArrowValidityVector) {
      BitVectorHelper.setValidityBit(validityBuffer, bufferIdx, 0);
    } else {
      nullabilityHolder.setNull(bufferIdx);
    }
  }

  private void setNulls(NullabilityHolder nullabilityHolder, int idx, int numValues, ArrowBuf validityBuffer) {
    if (setArrowValidityVector) {
      for (int i = 0; i < numValues; i++) {
        BitVectorHelper.setValidityBit(validityBuffer, idx + i, 0);
      }
    } else {
      nullabilityHolder.setNulls(idx, numValues);
    }
  }

}
