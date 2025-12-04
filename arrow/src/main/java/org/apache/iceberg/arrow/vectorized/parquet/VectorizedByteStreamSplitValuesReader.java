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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type BYTE_STREAM_SPLIT. This is
 * adapted from Parquet's ByteStreamSplitValuesReader.
 *
 * @see <a
 *     href="https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split-byte_stream_split--9">
 *     Parquet format encodings: BYTE_STREAM_SPLIT</a>
 */
public class VectorizedByteStreamSplitValuesReader implements VectorizedValuesReader {

  private int totalBytesInStream;
  private ByteBufferInputStream in;
  private ByteBuffer decodedDataStream;

  public VectorizedByteStreamSplitValuesReader() {}

  @Override
  public void initFromPage(int ignoredValueCount, ByteBufferInputStream inputStream)
      throws IOException {
    totalBytesInStream = inputStream.available();
    this.in = inputStream;
  }

  @Override
  public float readFloat() {
    ensureDecodedBufferIsInitializedForElementSize(FLOAT_SIZE);
    return decodedDataStream.getFloat();
  }

  @Override
  public double readDouble() {
    ensureDecodedBufferIsInitializedForElementSize(DOUBLE_SIZE);
    return decodedDataStream.getDouble();
  }

  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    readValues(
        FLOAT_SIZE,
        total,
        rowId,
        offset -> vec.getDataBuffer().setFloat(offset, decodedDataStream.getFloat()));
  }

  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    readValues(
        DOUBLE_SIZE,
        total,
        rowId,
        offset -> vec.getDataBuffer().setDouble(offset, decodedDataStream.getDouble()));
  }

  private void ensureDecodedBufferIsInitializedForElementSize(int elementSizeInBytes) {
    if (decodedDataStream == null) {
      decodedDataStream =
          decodeDataFromStream(totalBytesInStream / elementSizeInBytes, elementSizeInBytes);
    }
  }

  private void readValues(int elementSizeInBytes, int total, int rowId, OutputWriter outputWriter) {
    ensureDecodedBufferIsInitializedForElementSize(elementSizeInBytes);
    decodedDataStream.position(rowId * elementSizeInBytes);
    for (int i = 0; i < total; i++) {
      int offset = (rowId + i) * elementSizeInBytes;
      outputWriter.writeToOutput(offset);
    }
  }

  @FunctionalInterface
  interface OutputWriter {
    void writeToOutput(int offset);
  }

  private ByteBuffer decodeDataFromStream(int valuesCount, int elementSizeInBytes) {
    ByteBuffer encoded;
    try {
      encoded = in.slice(totalBytesInStream).slice();
    } catch (EOFException e) {
      throw new RuntimeException("Failed to read bytes from stream", e);
    }
    byte[] decoded = new byte[encoded.limit()];
    int destByteIndex = 0;
    for (int srcValueIndex = 0; srcValueIndex < valuesCount; ++srcValueIndex) {
      for (int stream = 0; stream < elementSizeInBytes; ++stream, ++destByteIndex) {
        decoded[destByteIndex] = encoded.get(srcValueIndex + stream * valuesCount);
      }
    }
    return ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN);
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException("readBoolean is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public int readInteger() {
    throw new UnsupportedOperationException("readInteger is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public long readLong() {
    throw new UnsupportedOperationException("readLong is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException("readBinary is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readIntegers is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readLongs is not supported");
  }

  /** BYTE_STREAM_SPLIT only supports FLOAT and DOUBLE */
  @Override
  public void readBinary(int total, FieldVector vec, int rowId, boolean setArrowValidityVector) {
    throw new UnsupportedOperationException("readBinary is not supported");
  }
}
