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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for DELTA_LENGTH_BYTE_ARRAY encoding. This
 * encoding stores delta-encoded byte array lengths followed by the concatenated byte array data.
 * This is adapted from Spark's VectorizedDeltaLengthByteArrayReader.
 *
 * @see <a
 *     href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6">
 *     Parquet format encodings: DELTA_LENGTH_BYTE_ARRAY</a>
 */
public class VectorizedDeltaLengthByteArrayValuesReader extends ValuesReader
    implements VectorizedValuesReader {

  private ByteBufferInputStream dataStream;
  private int[] lengths;
  private int currentRow = 0;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    VectorizedDeltaEncodedValuesReader lengthReader = new VectorizedDeltaEncodedValuesReader();
    lengthReader.initFromPage(valueCount, in);
    this.lengths = lengthReader.readIntegers(lengthReader.totalValueCount(), 0);
    this.dataStream = in;
  }

  int lengthForCurrentRow() {
    return lengths[currentRow];
  }

  @Override
  public Binary readBinary(int len) {
    try {
      ByteBuffer buffer = dataStream.slice(lengths[currentRow]);
      this.currentRow++;
      if (buffer.hasArray()) {
        return Binary.fromConstantByteArray(
            buffer.array(), buffer.arrayOffset() + buffer.position(), buffer.remaining());
      } else {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return Binary.fromConstantByteArray(bytes);
      }
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to read binary data", e);
    }
  }

  @Override
  public int readInteger() {
    return lengths[currentRow];
  }

  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException("readBoolean is not supported");
  }

  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  @Override
  public long readLong() {
    throw new UnsupportedOperationException("readLong is not supported");
  }

  @Override
  public float readFloat() {
    throw new UnsupportedOperationException("readFloat is not supported");
  }

  @Override
  public double readDouble() {
    throw new UnsupportedOperationException("readDouble is not supported");
  }

  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readIntegers is not supported");
  }

  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readLongs is not supported");
  }

  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readFloats is not supported");
  }

  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readDoubles is not supported");
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException("skip is not supported");
  }
}
