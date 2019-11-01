/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.parquet;

import io.netty.buffer.ArrowBuf;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;

/**
 * A values reader for Parquet's run-length encoded data. This is based off of the version in
 * parquet-mr with these changes:
 * - Supports the vectorized interface.
 * - Works on byte arrays(byte[]) instead of making byte streams.
 * <p>
 * This encoding is used in multiple places:
 * - Definition/Repetition levels
 * - Dictionary ids.
 */
public final class VectorizedValuesReader extends ValuesReader {
  // Current decoding mode. The encoded data contains groups of either run length encoded data
  // (RLE) or bit packed data. Each group contains a header that indicates which group it is and
  // the number of values in the group.
  // More details here: https://github.com/Parquet/parquet-format/blob/master/Encodings.md
  private enum MODE {
    RLE,
    PACKED
  }

  // Encoded data.
  private ByteBufferInputStream in;

  // bit/byte width of decoded data and utility to batch unpack them.
  private int bitWidth;
  private int bytesWidth;
  private BytePacker packer;

  // Current decoding mode and values
  private MODE mode;
  private int currentCount;
  private int currentValue;

  // Buffer of decoded values if the values are PACKED.
  private int[] currentBuffer = new int[16];
  private int currentBufferIdx = 0;

  // If true, the bit width is fixed. This decoder is used in different places and this also
  // controls if we need to read the bitwidth from the beginning of the data stream.
  private final boolean fixedWidth;
  private final boolean readLength;
  private final int maxDefLevel;

  public VectorizedValuesReader(
      int bitWidth,
      int maxDefLevel) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    this.maxDefLevel = maxDefLevel;
    init(bitWidth);
  }

  public VectorizedValuesReader(
      int bitWidth,
      boolean readLength,
      int maxDefLevel) {
    this.fixedWidth = true;
    this.readLength = readLength;
    this.maxDefLevel = maxDefLevel;
    init(bitWidth);
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    this.in = in;
    if (fixedWidth) {
      // initialize for repetition and definition levels
      if (readLength) {
        int length = readIntLittleEndian();
        this.in = in.sliceStream(length);
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
  private void init(int bitWidth) {
    Preconditions.checkArgument(bitWidth >= 0 && bitWidth <= 32, "bitWidth must be >= 0 and <= 32");
    this.bitWidth = bitWidth;
    this.bytesWidth = BytesUtils.paddedByteCountFromBits(bitWidth);
    this.packer = Packer.LITTLE_ENDIAN.newBytePacker(bitWidth);
  }

  /**
   * Reads the next varint encoded int.
   */
  private int readUnsignedVarInt() throws IOException {
    int value = 0;
    int shift = 0;
    int b;
    do {
      b = in.read();
      value |= (b & 0x7F) << shift;
      shift += 7;
    } while ((b & 0x80) != 0);
    return value;
  }

  /**
   * Reads the next 4 byte little endian int.
   */
  private int readIntLittleEndian() throws IOException {
    int ch4 = in.read();
    int ch3 = in.read();
    int ch2 = in.read();
    int ch1 = in.read();
    return ((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0));
  }

  /**
   * Reads the next byteWidth little endian int.
   */
  private int readIntLittleEndianPaddedOnBitWidth() throws IOException {
    switch (bytesWidth) {
      case 0:
        return 0;
      case 1:
        return in.read();
      case 2: {
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 8) + ch2;
      }
      case 3: {
        int ch3 = in.read();
        int ch2 = in.read();
        int ch1 = in.read();
        return (ch1 << 16) + (ch2 << 8) + (ch3 << 0);
      }
      case 4: {
        return readIntLittleEndian();
      }
    }
    throw new RuntimeException("Unreachable");
  }

  private int ceil8(int value) {
    return (value + 7) / 8;
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

          if (this.currentBuffer.length < this.currentCount) {
            this.currentBuffer = new int[this.currentCount];
          }
          currentBufferIdx = 0;
          int valueIndex = 0;
          while (valueIndex < this.currentCount) {
            // values are bit packed 8 at a time, so reading bitWidth will always work
            ByteBuffer buffer = in.slice(bitWidth);
            this.packer.unpack8Values(buffer, buffer.position(), this.currentBuffer, valueIndex);
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
        return this.currentBuffer[currentBufferIdx++];
    }
    throw new RuntimeException("Unreachable");
  }

  // the same method will be used for reading in the data pages that are dictionary encoded
  public void readBatchOfIntegers(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          bufferIdx =
              fillFixWidthValueBuffer(
                  typeWidth,
                  maxDefLevel,
                  nullabilityHolder,
                  valuesReader,
                  bufferIdx,
                  dataBuffer,
                  n);
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              //ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
              //dataBuffer.setBytes(bufferIdx * typeWidth, buffer);
              dataBuffer.setInt(bufferIdx * typeWidth, valuesReader.getBuffer(typeWidth).getInt());
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              bufferIdx++;
            } else {
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  // ok this is the point where we fill the arrow vector. If dictionary is available to us, what we need to do
  // is to take in batch of dictionary ids through the bytes reader, and set it in the arrow vector. So instead
  // of writing values to the dataBuffer, we instead will be writing array indices.

  public void readBatchOfLongs(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          bufferIdx =
              fillFixWidthValueBuffer(
                  typeWidth,
                  maxDefLevel,
                  nullabilityHolder,
                  valuesReader,
                  bufferIdx,
                  dataBuffer,
                  n);
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              //ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
              //dataBuffer.setBytes(bufferIdx * typeWidth, buffer);
              dataBuffer.setLong(bufferIdx * typeWidth, valuesReader.getBuffer(typeWidth).getLong());
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              bufferIdx++;
            } else {
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfFloats(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder, BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          bufferIdx =
              fillFixWidthValueBuffer(
                  typeWidth,
                  maxDefLevel,
                  nullabilityHolder,
                  valuesReader,
                  bufferIdx,
                  dataBuffer,
                  n);
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              //ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
              //dataBuffer.setBytes(bufferIdx * typeWidth, buffer);
              dataBuffer.setFloat(bufferIdx * typeWidth, valuesReader.getBuffer(typeWidth).getFloat());
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              bufferIdx++;
            } else {
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfDoubles(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          bufferIdx =
              fillFixWidthValueBuffer(
                  typeWidth,
                  maxDefLevel,
                  nullabilityHolder,
                  valuesReader,
                  bufferIdx,
                  dataBuffer,
                  n);
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              //ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
              //dataBuffer.setBytes(bufferIdx * typeWidth, buffer);
              dataBuffer.setDouble(bufferIdx * typeWidth, valuesReader.getBuffer(typeWidth).getDouble());
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              bufferIdx++;
            } else {
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfFixedWidthBinary(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            // for (int i = 0; i < n; i++) {
            //   //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            //   validityBufferIdx++;
            // }
            for (int i = 0; i < n; i++) {
              bufferIdx = setBinaryInVector((VarBinaryVector) vector, typeWidth, valuesReader, bufferIdx);
            }
          } else {
            for (int i = 0; i < n; i++) {
              //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              bufferIdx = setBinaryInVector((VarBinaryVector) vector, typeWidth, valuesReader, bufferIdx);
            } else {
              //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfFixedLengthDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    //ArrowBuf validityBuffer = vector.getValidityBuffer();
    //ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            // for (int i = 0; i < n; i++) {
            //   //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            //   validityBufferIdx++;
            // }
            for (int i = 0; i < n; i++) {
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              //bytesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(bufferIdx, byteArray);
              bufferIdx++;
            }
          } else {
            for (int i = 0; i < n; i++) {
              //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(bufferIdx, byteArray);
              bufferIdx++;
            } else {
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
   * vector. It appropriately sets the validity buffer in the Arrow vector.
   */
  public void readBatchVarWidth(
      final FieldVector vector,
      final int numValsInVector,
      final int batchSize,
      NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            // for (int i = 0; i < n; i++) {
            //   //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            //   validityBufferIdx++;
            // }
            for (int i = 0; i < n; i++) {
              int len = valuesReader.readInteger();
              ByteBuffer buffer = valuesReader.getBuffer(len);
              ((BaseVariableWidthVector) vector).setValueLengthSafe(bufferIdx, len);
              dataBuffer.writeBytes(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
              bufferIdx++;
            }
          } else {
            //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            nullabilityHolder.setNulls(bufferIdx, n);
            bufferIdx += n;
          }
          break;
        case PACKED:
          for (int i = 0; i < n; i++) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              int len = valuesReader.readInteger();
              ByteBuffer buffer = valuesReader.getBuffer(len);
              ((BaseVariableWidthVector) vector).setValueLengthSafe(bufferIdx, len);
              dataBuffer.writeBytes(buffer.array(), buffer.position(), buffer.limit() - buffer.position());
              bufferIdx++;
            } else {
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfIntLongBackedDecimals(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    ArrowBuf validityBuffer = vector.getValidityBuffer();
    ArrowBuf dataBuffer = vector.getDataBuffer();
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < n; i++) {
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
              dataBuffer.setBytes(bufferIdx * DecimalVector.TYPE_WIDTH, byteArray);
              bufferIdx++;
            }
          } else {
            nullabilityHolder.setNulls(bufferIdx, n);
            bufferIdx += n;
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              valuesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
              dataBuffer.setBytes(bufferIdx * DecimalVector.TYPE_WIDTH, byteArray);
              bufferIdx++;
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            } else {
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  public void readBatchOfBooleans(
      final FieldVector vector, final int numValsInVector, final int batchSize, NullabilityHolder nullabilityHolder, BytesReader valuesReader) {
    int bufferIdx = numValsInVector;
    int left = batchSize;
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            for (int i = 0; i < n; i++) {
              ((BitVector) vector).setSafe(bufferIdx, ((valuesReader.readBoolean() == false) ? 0 : 1));
              bufferIdx++;
            }
          } else {
            for (int i = 0; i < n; i++) {
              ((BitVector) vector).setNull(bufferIdx);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              ((BitVector) vector).setSafe(bufferIdx, ((valuesReader.readBoolean() == false) ? 0 : 1));
              bufferIdx++;
            } else {
              ((BitVector) vector).setNull(bufferIdx);
              nullabilityHolder.setNull(bufferIdx);
              bufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  private int setBinaryInVector(VarBinaryVector vector, int typeWidth, BytesReader valuesReader, int bufferIdx) {
    byte[] byteArray = new byte[typeWidth];
    valuesReader.getBuffer(typeWidth).get(byteArray);
    vector.setSafe(bufferIdx, byteArray);
    bufferIdx++;
    return bufferIdx;
  }

  private int fillFixWidthValueBuffer(
      int typeWidth, int maxDefLevel, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader, int bufferIdx, ArrowBuf dataBuffer, int n) {
    if (currentValue == maxDefLevel) {
      // for (int i = 0; i < n; i++) {
      //   //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
      //   validityBufferIdx++;
      // }
      ByteBuffer buffer = valuesReader.getBuffer(n * typeWidth);
      dataBuffer.setBytes(bufferIdx * typeWidth, buffer);
      bufferIdx += n;
    } else {
      for (int i = 0; i < n; i++) {
        //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
        nullabilityHolder.setNull(bufferIdx);
        bufferIdx++;
      }
    }
    return bufferIdx;
  }
}
