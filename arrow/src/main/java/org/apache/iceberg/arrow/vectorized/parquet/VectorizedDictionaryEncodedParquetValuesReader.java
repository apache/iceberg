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
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.iceberg.arrow.vectorized.NullabilityHolder;
import org.apache.parquet.column.Dictionary;

public class VectorizedDictionaryEncodedParquetValuesReader extends BaseVectorizedParquetValuesReader {

  public VectorizedDictionaryEncodedParquetValuesReader(int maxDefLevel, boolean setValidityVector) {
    super(maxDefLevel, setValidityVector);
  }

  // Used for reading dictionary ids in a vectorized fashion. Unlike other methods, this doesn't
  // check definition level.
  void readBatchOfDictionaryIds(
      final IntVector intVector,
      final int numValsInVector,
      final int numValuesToRead,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = numValsInVector;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < numValues; i++) {
            intVector.set(idx, currentValue);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            intVector.set(idx, packedValuesBuffer[packedValuesBufferIdx]);
            nullabilityHolder.setNotNull(idx);
            packedValuesBufferIdx++;
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  void readBatchOfDictionaryEncodedLongs(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int numValues = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer().setLong(idx, dict.decodeToLong(currentValue));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer()
                .setLong(idx, dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  void readBatchOfDictionaryEncodedIntegers(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      ArrowBuf dataBuffer = vector.getDataBuffer();
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            dataBuffer.setInt(idx, dict.decodeToInt(currentValue));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            dataBuffer.setInt(idx, dict.decodeToInt(packedValuesBuffer[packedValuesBufferIdx++]));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFloats(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setFloat(idx, dict.decodeToFloat(currentValue));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setFloat(idx, dict.decodeToFloat(packedValuesBuffer[packedValuesBufferIdx++]));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedDoubles(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setDouble(idx, dict.decodeToDouble(currentValue));
            nullabilityHolder.setNotNull(idx);
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            vector.getDataBuffer().setDouble(idx, dict.decodeToDouble(packedValuesBuffer[packedValuesBufferIdx++]));
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFixedWidthBinary(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict,
      NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(currentValue).toByteBuffer();
            vector.getDataBuffer().setBytes(idx * typeWidth, buffer.array(),
                buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).toByteBuffer();
            vector.getDataBuffer()
                .setBytes(idx * typeWidth, buffer.array(),
                    buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            if (setArrowValidityVector) {
              BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), idx);
            } else {
              nullabilityHolder.setNotNull(idx);
            }
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedFixedLengthDecimals(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            byte[] decimalBytes = dict.decodeToBinary(currentValue).getBytesUnsafe();
            byte[] vectorBytes = new byte[DecimalVector.TYPE_WIDTH];
            System.arraycopy(decimalBytes, 0, vectorBytes, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
            ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            byte[] decimalBytes = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).getBytesUnsafe();
            byte[] vectorBytes = new byte[DecimalVector.TYPE_WIDTH];
            System.arraycopy(decimalBytes, 0, vectorBytes, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
            ((DecimalVector) vector).setBigEndian(idx, vectorBytes);
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedVarWidthBinary(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(currentValue).toByteBuffer();
            ((BaseVariableWidthVector) vector).setSafe(idx, buffer.array(),
                buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ByteBuffer buffer = dict.decodeToBinary(packedValuesBuffer[packedValuesBufferIdx++]).toByteBuffer();
            ((BaseVariableWidthVector) vector).setSafe(idx, buffer.array(),
                buffer.position() + buffer.arrayOffset(), buffer.limit() - buffer.position());
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }

  void readBatchOfDictionaryEncodedIntLongBackedDecimals(
      FieldVector vector,
      final int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict, NullabilityHolder nullabilityHolder) {
    int left = numValuesToRead;
    int idx = index;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int num = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx,
                typeWidth == Integer.BYTES ? dict.decodeToInt(currentValue) : dict.decodeToLong(currentValue));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < num; i++) {
            ((DecimalVector) vector).set(
                idx,
                typeWidth == Integer.BYTES ?
                    dict.decodeToInt(currentValue)
                    : dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]));
            nullabilityHolder.setNotNull(idx);
            idx++;
          }
          break;
      }
      left -= num;
      currentCount -= num;
    }
  }
}
