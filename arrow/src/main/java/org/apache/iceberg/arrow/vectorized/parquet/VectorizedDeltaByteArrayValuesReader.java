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
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for DELTA_BYTE_ARRAY encoding. This encoding
 * stores delta-encoded prefix lengths followed by suffixes encoded as DELTA_LENGTH_BYTE_ARRAY. Each
 * value is reconstructed by taking the prefix of the previous value and appending the suffix. This
 * is adapted from Spark's VectorizedDeltaByteArrayReader.
 *
 * @see <a
 *     href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7">
 *     Parquet format encodings: DELTA_BYTE_ARRAY</a>
 */
public class VectorizedDeltaByteArrayValuesReader extends ValuesReader
    implements VectorizedValuesReader {

  private int[] prefixLengths;
  private VectorizedDeltaLengthByteArrayValuesReader suffixReader;
  private Binary previous = Binary.EMPTY;
  private int currentRow = 0;

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    VectorizedDeltaEncodedValuesReader prefixLengthReader =
        new VectorizedDeltaEncodedValuesReader();
    prefixLengthReader.initFromPage(valueCount, in);
    this.prefixLengths = prefixLengthReader.readIntegers(prefixLengthReader.totalValueCount(), 0);
    this.suffixReader = new VectorizedDeltaLengthByteArrayValuesReader();
    this.suffixReader.initFromPage(valueCount, in);
  }

  @Override
  public int readInteger() {
    return prefixLengths[currentRow] + suffixReader.lengthForCurrentRow();
  }

  @Override
  public Binary readBinary(int len) {
    Binary suffix = suffixReader.readBinary(len);
    int prefixLength = prefixLengths[currentRow];
    this.currentRow++;

    if (prefixLength == 0) {
      this.previous = suffix;
      return suffix;
    }

    byte[] prefixBytes = previous.getBytes();
    byte[] suffixBytes = suffix.getBytes();
    byte[] out = new byte[prefixLength + suffixBytes.length];
    System.arraycopy(prefixBytes, 0, out, 0, prefixLength);
    System.arraycopy(suffixBytes, 0, out, prefixLength, suffixBytes.length);
    this.previous = Binary.fromConstantByteArray(out);
    return this.previous;
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
