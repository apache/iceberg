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
    int prefixLength = prefixLengths[currentRow];
    Binary suffix = suffixReader.readBinary(len - prefixLength);
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
    return previous;
  }

  @Override
  public void skip() {
    throw new UnsupportedOperationException("skip is not supported");
  }
}
