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

package org.apache.iceberg.parquet.vectorized;

import io.netty.buffer.ArrowBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.iceberg.parquet.ValuesAsBytesReader;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.Dictionary;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * A values reader for Parquet's run-length encoded data that reads column data in batches instead of one value at a
 * time. This is based off of the version in Apache Spark with these changes:
 * <p>
 * <tr>Writes batches of values retrieved to Arrow vectors</tr>
 * <tr>If all pages of a column within the row group are not dictionary encoded, then
 * dictionary ids are eagerly decoded into actual values before writing them to the Arrow vectors</tr>
 * </p>
 */
public final class VectorizedParquetValuesReader extends ValuesReader {

  // Current decoding mode. The encoded data contains groups of either run length encoded data
  // (RLE) or bit packed data. Each group contains a header that indicates which group it is and
  // the number of values in the group.
  private enum MODE {
    RLE,
    PACKED
  }

  // Encoded data.
  private ByteBufferInputStream inputStream;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  private MODE mode;
  private int currentCount;
  private int currentValue;

  // Buffer of decoded values if the values are PACKED.
  private int[] packedValuesBuffer = new int[16];
  private int packedValuesBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;
  private final boolean readLength;
  private final int maxDefLevel;

  public VectorizedParquetValuesReader(int maxDefLevel) {
    this.maxDefLevel = maxDefLevel;
    this.fixedWidth = false;
    this.readLength = false;
  }

  public VectorizedParquetValuesReader(
      int bitWidth,
      int maxDefLevel) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    this.maxDefLevel = maxDefLevel;
    init(bitWidth);
  }

  public VectorizedParquetValuesReader(
      int bw,
      boolean rl,
      int mdl) {
    this.fixedWidth = true;
    this.readLength = rl;
    this.maxDefLevel = mdl;
    init(bw);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.inputStream = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.inputStream = in.sliceStream(length);
      }
    } else {
      // initialize for values
      if (in.available() > 0) {
        init(in.read());
      }
    }
    if (bitWidth == 0) {
      // 0 bit width, treat this as an RLE run of valueCount number of 0's.
      this.mode = MODE.RLE;
      this.currentCount = valueCount;
      this.currentValue = 0;
    } else {
      this.currentCount = 0;
    }
  }

  /**
   * Initializes the internal state for decoding ints of `bitWidth`.
   */
  private void init(int bw) {
    Preconditions.checkArgument(bw >= 0 && bw <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bw;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bw);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bw);
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int byteRead;
    do {
      byteRead = inputStream.read();
      value |= (byteRead & 0x7F) << shift;
      shift += 7;
    } while ((byteRead & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = inputStream.read();
    int ch3 = inputStream.read();
    int ch2 = inputStream.read();
    int ch1 = inputStream.read();
    return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return inputStream.read();
      case 2: {
        int ch2 = inputStream.read();
        int ch1 = inputStream.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = inputStream.read();
        int ch2 = inputStream.read();
        int ch1 = inputStream.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

  /**
   * Reads the next group.
   */
  private void readNextGroup() {
    try {
      int header = readUnsignedVarInt();
      this.mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
      switch (mode) {
        case RLE:
          this.currentCount = header >>> 1;
          this.currentValue = readIntLittleEndianPaddedOnBitWidth();
          return;
        case PACKED:
          int numGroups = header >>> 1;
          this.currentCount = numGroups * 8;

          if (this.packedValuesBuffer.length < this.currentCount) {
            this.packedValuesBuffer = new int[this.currentCount];
          }
          packedValuesBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = inputStream.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.packedValuesBuffer, valueIndex);
            valueIndex += 8;
          }
          return;
        default:
          throw new ParquetDecodingException("not a valid mode " + this.mode);
      }
    } catch (IOException e) {
      throw new ParquetDecodingException("Failed to read from input stream", e);
    }
  }

  @Override
  public boolean readBoolean() {
    return this.readInteger() != 0;
  }

  @Override
  public void skip() {
    this.readInteger();
  }

  @Override
  public int readValueDictionaryId() {
    return readInteger();
  }

  @Override
  public int readInteger() {
    if (this.currentCount == 0) {
      this.readNextGroup();
    }

    this.currentCount--;
    switch (mode) {
      case RLE:
        return this.currentValue;
      case PACKED:
        return this.packedValuesBuffer[packedValuesBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  public void readBatchOfDictionaryIds(
      final IntVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader dictionaryEncodedValuesReader) {
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
            dictionaryEncodedValuesReader.readDictionaryIdsInternal(vector, idx, numValues);
          } else {
            setNulls(nullabilityHolder, idx, numValues, vector.getValidityBuffer());
          }
          idx += numValues;
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.set(idx, dictionaryEncodedValuesReader.readInteger());
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

  // Used for reading dictionary ids in a vectorized fashion. Unlike other methods, this doesn't
  // check definition level.
  private void readDictionaryIdsInternal(
      final IntVector intVector,
      final int numValsInVector,
      final int numValuesToRead) {
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
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            intVector.set(idx, packedValuesBuffer[packedValuesBufferIdx]);
            packedValuesBufferIdx++;
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
  }

  public void readBatchOfDictionaryEncodedIntegers(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedIntegersInternal(
      FieldVector vector,
      int index,
      int typeWidth,
      int numValuesToRead,
      Dictionary dict) {
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
              setValue(
                  typeWidth,
                  valuesReader,
                  bufferIdx,
                  vector.getValidityBuffer(),
                  vector.getDataBuffer());
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
      VectorizedParquetValuesReader valuesReader,
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
            valuesReader.readBatchOfDictionaryEncodedLongsInternal(vector, typeWidth, idx, numValues, dict);
          } else {
            setNulls(nullabilityHolder, idx, numValues, validityBuffer);
          }
          idx += numValues;
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            if (packedValuesBuffer[packedValuesBufferIdx++] == maxDefLevel) {
              vector.getDataBuffer().setLong(idx, dict.decodeToLong(valuesReader.readInteger()));
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

  private void readBatchOfDictionaryEncodedLongsInternal(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
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
            idx++;
          }
          break;
        case PACKED:
          for (int i = 0; i < numValues; i++) {
            vector.getDataBuffer()
                .setLong(idx, dict.decodeToLong(packedValuesBuffer[packedValuesBufferIdx++]));
            idx++;
          }
          break;
      }
      left -= numValues;
      currentCount -= numValues;
    }
  }

  public void readBatchOfFloats(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, ValuesAsBytesReader valuesReader) {
  }

  private void setValue(
      int typeWidth,
      ValuesAsBytesReader valuesReader,
      int bufferIdx,
      ArrowBuf validityBuffer,
      ArrowBuf dataBuffer) {
    dataBuffer.setBytes(bufferIdx * typeWidth, valuesReader.getBuffer(typeWidth));
    BitVectorHelper.setValidityBitToOne(validityBuffer, bufferIdx);
  }

  public void readBatchOfDictionaryEncodedFloats(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedFloatsInternal(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
  }

  public void readBatchOfDoubles(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
  }

  public void readBatchOfDictionaryEncodedDoubles(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedDoublesInternal(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
  }

  public void readBatchOfFixedWidthBinary(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
  }

  public void readBatchOfDictionaryEncodedFixedWidthBinary(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedFixedWidthBinaryInternal(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
  }

  public void readBatchOfFixedLengthDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
  }

  public void readBatchOfDictionaryEncodedFixedLengthDecimals(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedFixedLengthDecimalsInternal(
      FieldVector vector,
      int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
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
  }

  private void setVarWidthBinaryValue(FieldVector vector, ValuesAsBytesReader valuesReader, int bufferIdx) {
    int len = valuesReader.readInteger();
    ByteBuffer buffer = valuesReader.getBuffer(len);
    // Calling setValueLengthSafe takes care of allocating a larger buffer if
    // running out of space.
    ((BaseVariableWidthVector) vector).setValueLengthSafe(bufferIdx, len);
    // It is possible that the data buffer was reallocated. So it is important to
    // not cache the data buffer reference but instead use vector.getDataBuffer().
    vector.getDataBuffer().writeBytes(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
    // Similarly, we need to get the latest reference to the validity buffer as well
    // since reallocation changes reference of the validity buffers as well.
    BitVectorHelper.setValidityBitToOne(vector.getValidityBuffer(), bufferIdx);
  }

  public void readBatchOfDictionaryEncodedVarWidth(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader dictionaryEncodedValuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedVarWidthBinaryInternal(
      FieldVector vector,
      int index,
      int numValuesToRead,
      Dictionary dict) {
  }

  public void readBatchOfIntLongBackedDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
  }

  public void readBatchOfDictionaryEncodedIntLongBackedDecimals(
      final FieldVector vector,
      final int numValsInVector,
      final int typeWidth,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      VectorizedParquetValuesReader valuesReader,
      Dictionary dict) {
  }

  private void readBatchOfDictionaryEncodedIntLongBackedDecimalsInternal(
      FieldVector vector,
      final int typeWidth,
      int index,
      int numValuesToRead,
      Dictionary dict) {
  }

  public void readBatchOfBooleans(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader) {
  }

  private void setBinaryInVector(
      VarBinaryVector vector,
      int typeWidth,
      ValuesAsBytesReader valuesReader,
      int bufferIdx) {
    byte[] byteArray = new byte[typeWidth];
    valuesReader.getBuffer(typeWidth).get(byteArray);
    vector.setSafe(bufferIdx, byteArray);
  }

  private void setNextNValuesInVector(
      int typeWidth, NullabilityHolder nullabilityHolder,
      ValuesAsBytesReader valuesReader, int bufferIdx, FieldVector vector, int numValues) {
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    int validityBufferIdx = bufferIdx;
    if (currentValue == maxDefLevel) {
      for (int i = 0; i < numValues; i++) {
        BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
        validityBufferIdx++;
      }
      ByteBuffer buffer = valuesReader.getBuffer(numValues * typeWidth);
      vector.getDataBuffer().setBytes(bufferIdx * typeWidth, buffer);
    } else {
      setNulls(nullabilityHolder, bufferIdx, numValues, validityBuffer);
    }
  }

  private void setNull(NullabilityHolder nullabilityHolder, int bufferIdx, ArrowBuf validityBuffer) {
    nullabilityHolder.setNull(bufferIdx);
    BitVectorHelper.setValidityBit(validityBuffer, bufferIdx, 0);
  }

  private void setNulls(NullabilityHolder nullabilityHolder, int idx, int numValues, ArrowBuf validityBuffer) {
    int bufferIdx = idx;
    for (int i = 0; i < numValues; i++) {
      nullabilityHolder.setNull(bufferIdx);
      BitVectorHelper.setValidityBit(validityBuffer, bufferIdx, 0);
      bufferIdx++;
    }
  }
}
