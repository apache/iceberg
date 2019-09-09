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

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import io.netty.buffer.ArrowBuf;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.LinkedList;
import java.util.List;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.BitVectorHelper;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
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
public final class VectorizedRleValuesReader extends ValuesReader {
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

  public VectorizedRleValuesReader() {
    this.fixedWidth = false;
    this.readLength = false;
  }

  public VectorizedRleValuesReader(int bitWidth) {
    this.fixedWidth = true;
    this.readLength = bitWidth != 0;
    init(bitWidth);
  }

  public VectorizedRleValuesReader(int bitWidth, boolean readLength) {
    this.fixedWidth = true;
    this.readLength = readLength;
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

  /**
   * Method for reading a batch of non-decimal numeric data types (INT32, INT64, FLOAT, DOUBLE, DATE, TIMESTAMP)
   * This method reads batches of bytes from Parquet and writes them into the data buffer underneath the Arrow
   * vector. It appropriately sets the validity buffer in the Arrow vector.
   */
  public void nextBatchNumericNonDecimal(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, final int maxDefLevel, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int validityBufferIdx = numValsInVector;
    int dataBufferIdx = numValsInVector;
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
            // for (int i = 0; i < n; i++) {
            //   //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
            //   validityBufferIdx++;
            // }
            validityBufferIdx += n;
            ByteBuffer buffer = valuesReader.getBuffer(n * typeWidth);
            dataBuffer.setBytes(dataBufferIdx * typeWidth, buffer);
          } else {
            for (int i = 0; i < n; i++) {
              //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
              nullabilityHolder.nullAt(validityBufferIdx);
              validityBufferIdx++;
            }
          }
          dataBufferIdx += n;
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              ByteBuffer buffer = valuesReader.getBuffer(typeWidth);
              dataBuffer.setBytes(dataBufferIdx * typeWidth, buffer);
              //BitVectorHelper.setValidityBitToOne(validityBuffer, validityBufferIdx);
              validityBufferIdx++;
              dataBufferIdx++;
            } else {
              nullabilityHolder.nullAt(validityBufferIdx);
              validityBufferIdx++;
              dataBufferIdx++;
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
  public void nextBatchFixedLengthDecimal(
      final FieldVector vector, final int numValsInVector,
      final int typeWidth, final int batchSize, final int maxDefLevel, NullabilityHolder nullabilityHolder,
      BytesReader valuesReader) {
    int validityBufferIdx = numValsInVector;
    int dataBufferIdx = numValsInVector;
    //ArrowBuf validityBuffer = vector.getValidityBuffer();
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
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              //bytesReader.getBuffer(typeWidth).get(byteArray, 0, typeWidth);
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(dataBufferIdx, byteArray);
              dataBufferIdx++;
            }
          } else {
            for (int i = 0; i < n; i++) {
              //BitVectorHelper.setValidityBit(validityBuffer, validityBufferIdx, 0);
              nullabilityHolder.nullAt(validityBufferIdx);
              validityBufferIdx++;
            }
          }
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              byte[] byteArray = new byte[DecimalVector.TYPE_WIDTH];
              valuesReader.getBuffer(typeWidth).get(byteArray, DecimalVector.TYPE_WIDTH - typeWidth, typeWidth);
              ((DecimalVector) vector).setBigEndian(dataBufferIdx, byteArray);
              dataBufferIdx++;
              validityBufferIdx++;
            } else {
              nullabilityHolder.nullAt(validityBufferIdx);
              validityBufferIdx++;
              dataBufferIdx++;
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
  public void nextBatchVarWidth(
      final FieldVector vector, final int numValsInVector, final int batchSize, final int maxDefLevel,
      NullabilityHolder nullabilityHolder, BytesReader valuesReader) {
    int nullabilityHolderIdx = numValsInVector;
    int dataBufferIdx = numValsInVector;
    int left = batchSize;
    ArrowBuf dataBuffer = vector.getDataBuffer();
    while (left > 0) {
      if (this.currentCount == 0) {
        this.readNextGroup();
      }
      int n = Math.min(left, this.currentCount);
      switch (mode) {
        case RLE:
          if (currentValue == maxDefLevel) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            for (int i = 0; i < n; i++) {
              int len = valuesReader.readInteger();
              ByteBuffer buffer = valuesReader.getBuffer(len);
              byte[] bytes = new byte[len];
              ((BaseVariableWidthVector) vector).setValueLengthSafe(dataBufferIdx, len);
              dataBufferIdx++;
              buffer.get(bytes);
              out.write(bytes, 0, bytes.length);
            }
            dataBuffer.writeBytes(out.toByteArray());
            nullabilityHolderIdx += n;
          } else {
            for (int i = 0; i < n; i++) {
              nullabilityHolder.nullAt(nullabilityHolderIdx);
              dataBufferIdx++;
              nullabilityHolderIdx++;
            }
          }
          dataBufferIdx += n;
          break;
        case PACKED:
          for (int i = 0; i < n; ++i) {
            if (currentBuffer[currentBufferIdx++] == maxDefLevel) {
              int len = valuesReader.readInteger();
              ByteBuffer buffer = valuesReader.getBuffer(len);
              if (buffer.hasArray()) {
                ((BaseVariableWidthVector) vector).setSafe(dataBufferIdx, buffer.array());
              } else {
                byte[] bytes = new byte[len];
                buffer.get(bytes);
                ((BaseVariableWidthVector) vector).setSafe(dataBufferIdx, bytes);
              }
              nullabilityHolderIdx++;
              dataBufferIdx++;
            } else {
              nullabilityHolder.nullAt(nullabilityHolderIdx);
              nullabilityHolderIdx++;
              dataBufferIdx++;
            }
          }
          break;
      }
      left -= n;
      currentCount -= n;
    }
  }

  // private static class Chunk {
  //   private final boolean isNull;
  //   private int num = 1;
  //
  //   Chunk(boolean isNull) {
  //     this.isNull = isNull;
  //   }
  //   void increment() {
  //     num++;
  //   }
  //   boolean isChunkOfNulls() {
  //     return isNull;
  //   }
  //   int chunkSize() {
  //     return num;
  //   }
  // }
  //
  // private List<Chunk> getNullabilityChunks(int actualBatchSize, int maxDefLevel, NullabilityHolder nullabilityHolder) {
  //   int prevDefLevel = definitionLevels.nextInt();
  //   int defLevelsRead = 1;
  //   List<Chunk> chunks = new LinkedList<>();
  //   Chunk currentChunk = new Chunk(prevDefLevel != maxDefLevel);
  //   chunks.add(currentChunk);
  //   while (defLevelsRead < actualBatchSize) {
  //     int defLevel = definitionLevels.nextInt();
  //     if (defLevel == prevDefLevel) {
  //       currentChunk.increment();
  //     } else {
  //       currentChunk = new Chunk(defLevel != maxDefLevel);
  //       chunks.add(currentChunk);
  //       prevDefLevel = defLevel;
  //     }
  //     vectorIdx++;
  //     defLevelsRead++;
  //   }
  //   return chunks;
  // }

}
