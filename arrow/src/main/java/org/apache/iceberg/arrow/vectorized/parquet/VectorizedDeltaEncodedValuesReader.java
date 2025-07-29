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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type DELTA_BINARY_PACKED. This
 * is adapted from Spark's VectorizedDeltaBinaryPackedReader.
 *
 * @see <a
 *     href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-encoding-delta_binary_packed--5">
 *     Parquet format encodings: DELTA_BINARY_PACKED</a>
 */
public class VectorizedDeltaEncodedValuesReader extends ValuesReader
    implements VectorizedValuesReader {

  // header data
  private int blockSizeInValues;
  private int miniBlocksPerBlock;
  private int totalValueCount;
  private long firstValue;

  private int miniBlockSizeInValues;

  // values read by the caller
  private int valuesRead = 0;

  // variables to keep state of the current block and miniblock
  private long lastValueRead; // needed to compute the next value
  private long minDeltaInCurrentBlock; // needed to compute the next value
  // currentMiniBlock keeps track of the mini block within the current block that
  // we read and decoded most recently. Only used as an index into
  // bitWidths array
  private int currentMiniBlock = 0;
  private int[] bitWidths; // bit widths for each miniBlock in the current block
  private int remainingInBlock = 0; // values in current block still to be read
  private int remainingInMiniBlock = 0; // values in current mini block still to be read
  private long[] unpackedValuesBuffer;

  private ByteBufferInputStream inputStream;

  // temporary buffers used by readInteger and readLong
  private int intVal;
  private long longVal;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    Preconditions.checkArgument(
        valueCount >= 1, "Page must have at least one value, but it has " + valueCount);
    this.inputStream = in;
    // Read the header
    this.blockSizeInValues = BytesUtils.readUnsignedVarInt(this.inputStream);
    this.miniBlocksPerBlock = BytesUtils.readUnsignedVarInt(this.inputStream);
    double miniSize = (double) blockSizeInValues / miniBlocksPerBlock;
    Preconditions.checkArgument(
        miniSize % 8 == 0, "miniBlockSize must be multiple of 8, but it's " + miniSize);
    this.miniBlockSizeInValues = (int) miniSize;
    // True value count. May be less than valueCount because of nulls
    this.totalValueCount = BytesUtils.readUnsignedVarInt(this.inputStream);
    this.bitWidths = new int[miniBlocksPerBlock];
    this.unpackedValuesBuffer = new long[miniBlockSizeInValues];
    // read the first value
    firstValue = BytesUtils.readZigZagVarLong(this.inputStream);
  }

  /** DELTA_BINARY_PACKED only supports INT32 and INT64 */
  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  /** DELTA_BINARY_PACKED only supports INT32 and INT64 */
  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  @Override
  public int readInteger() {
    readValues(1, null, 0, INT_SIZE, (f, i, v) -> intVal = (int) v);
    return intVal;
  }

  @Override
  public long readLong() {
    readValues(1, null, 0, LONG_SIZE, (f, i, v) -> longVal = v);
    return longVal;
  }

  /** The Iceberg reader currently does not do skipping */
  @Override
  public void skip() {
    throw new UnsupportedOperationException("skip is not supported");
  }

  /** DELTA_BINARY_PACKED only supports INT32 and INT64 */
  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException("readBinary is not supported");
  }

  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, INT_SIZE, (f, i, v) -> f.getDataBuffer().setInt(i, (int) v));
  }

  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    readValues(total, vec, rowId, LONG_SIZE, (f, i, v) -> f.getDataBuffer().setLong(i, v));
  }

  /** DELTA_BINARY_PACKED only supports INT32 and INT64 */
  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readFloats is not supported");
  }

  /** DELTA_BINARY_PACKED only supports INT32 and INT64 */
  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readDoubles is not supported");
  }

  private void readValues(
      int total, FieldVector vec, int rowId, int typeWidth, IntegerOutputWriter outputWriter) {
    if (valuesRead + total > totalValueCount) {
      throw new ParquetDecodingException(
          "No more values to read. Total values read:  "
              + valuesRead
              + ", total count: "
              + totalValueCount
              + ", trying to read "
              + total
              + " more.");
    }

    int remaining = total;
    int currentRowId = rowId;
    // First value
    if (valuesRead == 0) {
      outputWriter.write(vec, ((long) (currentRowId + valuesRead) * typeWidth), firstValue);
      lastValueRead = firstValue;
      currentRowId++;
      remaining--;
    }

    while (remaining > 0) {
      int loadedRows;
      try {
        loadedRows = loadMiniBlockToOutput(remaining, vec, currentRowId, typeWidth, outputWriter);
      } catch (IOException e) {
        throw new ParquetDecodingException("Error reading mini block.", e);
      }
      currentRowId += loadedRows;
      remaining -= loadedRows;
    }
    valuesRead = total - remaining;
  }

  /**
   * Read from a mini block. Read at most 'remaining' values into output.
   *
   * @return the number of values read into output
   */
  private int loadMiniBlockToOutput(
      int remaining, FieldVector vec, int rowId, int typeWidth, IntegerOutputWriter outputWriter)
      throws IOException {

    // new block; read the block header
    if (remainingInBlock == 0) {
      readBlockHeader();
    }

    // new miniblock, unpack the miniblock
    if (remainingInMiniBlock == 0) {
      unpackMiniBlock();
    }

    // read values from miniblock
    int valuesReadInMiniBlock = 0;
    for (int i = miniBlockSizeInValues - remainingInMiniBlock;
        i < miniBlockSizeInValues && valuesReadInMiniBlock < remaining;
        i++) {
      // calculate values from deltas unpacked for current block
      long outValue = lastValueRead + minDeltaInCurrentBlock + unpackedValuesBuffer[i];
      lastValueRead = outValue;
      outputWriter.write(vec, ((long) (rowId + valuesReadInMiniBlock) * typeWidth), outValue);
      remainingInBlock--;
      remainingInMiniBlock--;
      valuesReadInMiniBlock++;
    }

    return valuesReadInMiniBlock;
  }

  private void readBlockHeader() {
    try {
      minDeltaInCurrentBlock = BytesUtils.readZigZagVarLong(inputStream);
    } catch (IOException e) {
      throw new ParquetDecodingException("Can not read min delta in current block", e);
    }
    readBitWidthsForMiniBlocks();
    remainingInBlock = blockSizeInValues;
    currentMiniBlock = 0;
    remainingInMiniBlock = 0;
  }

  /**
   * mini block has a size of 8*n, unpack 32 value each time
   *
   * <p>see org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader#unpackMiniBlock
   */
  private void unpackMiniBlock() throws IOException {
    Arrays.fill(this.unpackedValuesBuffer, 0);
    BytePackerForLong packer =
        Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidths[currentMiniBlock]);
    for (int j = 0; j < miniBlockSizeInValues; j += 8) {
      ByteBuffer buffer = inputStream.slice(packer.getBitWidth());
      if (buffer.hasArray()) {
        packer.unpack8Values(
            buffer.array(), buffer.arrayOffset() + buffer.position(), unpackedValuesBuffer, j);
      } else {
        packer.unpack8Values(buffer, buffer.position(), unpackedValuesBuffer, j);
      }
    }
    remainingInMiniBlock = miniBlockSizeInValues;
    currentMiniBlock++;
  }

  // From org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesReader
  private void readBitWidthsForMiniBlocks() {
    for (int i = 0; i < miniBlocksPerBlock; i++) {
      try {
        bitWidths[i] = BytesUtils.readIntLittleEndianOnOneByte(inputStream);
      } catch (IOException e) {
        throw new ParquetDecodingException("Can not decode bitwidth in block header", e);
      }
    }
  }

  /** A functional interface to write long values to into a FieldVector */
  @FunctionalInterface
  interface IntegerOutputWriter {

    /**
     * A functional interface that can be used to write a long value to a specified row in a
     * FieldVector
     *
     * @param vec a FieldVector to write the value into
     * @param index The offset to write to
     * @param val value to write
     */
    void write(FieldVector vec, long index, long val);
  }
}
