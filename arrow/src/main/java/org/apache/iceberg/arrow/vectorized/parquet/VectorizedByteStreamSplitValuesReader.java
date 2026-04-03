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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.arrow.vector.FieldVector;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type BYTE_STREAM_SPLIT. This is
 * adapted from Parquet's ByteStreamSplitValuesReader.
 *
 * @see <a
 *     href="https://parquet.apache.org/docs/file-format/data-pages/encodings/#byte-stream-split-byte_stream_split--9">
 *     Parquet format encodings: BYTE_STREAM_SPLIT</a>
 */
public class VectorizedByteStreamSplitValuesReader extends ValuesReader
    implements VectorizedValuesReader {

  private final int elementSizeInBytes;
  private int totalBytesInStream;
  private ByteBufferInputStream dataStream;
  private ByteBuffer decodedDataStream;

  public VectorizedByteStreamSplitValuesReader(int elementSizeInBytes) {
    this.elementSizeInBytes = elementSizeInBytes;
  }

  @Override
  public void initFromPage(int ignoredValueCount, ByteBufferInputStream in) {
    this.totalBytesInStream = in.available();
    this.dataStream = in;
  }

  @Override
  public int readInteger() {
    ensureDecoded();
    return decodedDataStream.getInt();
  }

  @Override
  public long readLong() {
    ensureDecoded();
    return decodedDataStream.getLong();
  }

  @Override
  public float readFloat() {
    ensureDecoded();
    return decodedDataStream.getFloat();
  }

  @Override
  public double readDouble() {
    ensureDecoded();
    return decodedDataStream.getDouble();
  }

  @Override
  public Binary readBinary(int len) {
    ensureDecoded();
    byte[] bytes = new byte[len];
    decodedDataStream.get(bytes);
    return Binary.fromConstantByteArray(bytes);
  }

  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    readBatch(total, vec, rowId);
  }

  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    readBatch(total, vec, rowId);
  }

  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    readBatch(total, vec, rowId);
  }

  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    readBatch(total, vec, rowId);
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException("skip is not supported");
  }

  private void readBatch(int total, FieldVector vec, int rowId) {
    ensureDecoded();
    int bytesToRead = total * elementSizeInBytes;
    long destOffset = (long) rowId * elementSizeInBytes;
    ByteBuffer slice = decodedDataStream.slice();
    slice.limit(bytesToRead);
    vec.getDataBuffer().setBytes(destOffset, slice);
    decodedDataStream.position(decodedDataStream.position() + bytesToRead);
  }

  private void ensureDecoded() {
    if (decodedDataStream == null) {
      Preconditions.checkState(
          totalBytesInStream % elementSizeInBytes == 0,
          "Stream size %s is not a multiple of element size %s",
          totalBytesInStream,
          elementSizeInBytes);
      this.decodedDataStream = decode(totalBytesInStream / elementSizeInBytes);
    }
  }

  private ByteBuffer decode(int valuesCount) {
    ByteBuffer encoded;
    try {
      encoded = dataStream.slice(totalBytesInStream).slice();
    } catch (EOFException e) {
      throw new UncheckedIOException("Failed to read bytes from stream", e);
    }
    byte[] decoded = new byte[encoded.limit()];
    int destByteIndex = 0;
    for (int srcValueIndex = 0; srcValueIndex < valuesCount; srcValueIndex++) {
      for (int stream = 0; stream < elementSizeInBytes; stream++, destByteIndex++) {
        decoded[destByteIndex] = encoded.get(srcValueIndex + stream * valuesCount);
      }
    }
    return ByteBuffer.wrap(decoded).order(ByteOrder.LITTLE_ENDIAN);
  }
}
