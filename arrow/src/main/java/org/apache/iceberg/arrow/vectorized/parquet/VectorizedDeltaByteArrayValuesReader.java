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
import org.apache.arrow.vector.BaseFixedWidthVector;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.FixedWidthVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type DELTA_BYTE_ARRAY. This is
 * adapted from Spark's VectorizedDeltaByteArrayReader.
 *
 * @see <a
 *     href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-strings-delta_byte_array--7">
 *     Parquet format encodings: DELTA_BYTE_ARRAY</a>
 */
public class VectorizedDeltaByteArrayValuesReader implements VectorizedValuesReader {

  private final VectorizedDeltaEncodedValuesReader prefixLengthReader;
  private final VectorizedDeltaLengthByteArrayValuesReader suffixReader;

  private int[] prefixLengths;
  private Binary previous;

  public VectorizedDeltaByteArrayValuesReader() {
    prefixLengthReader = new VectorizedDeltaEncodedValuesReader();
    suffixReader = new VectorizedDeltaLengthByteArrayValuesReader();
    previous = Binary.EMPTY;
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException {
    prefixLengthReader.initFromPage(valueCount, in);
    prefixLengths = prefixLengthReader.readIntegers(valueCount, 0);
    suffixReader.initFromPage(valueCount, in);
  }

  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void readBinary(int total, FieldVector vec, int rowId, boolean setArrowValidityVector) {
    if (vec instanceof BaseVariableWidthVector) {
      BaseVariableWidthVector vector = (BaseVariableWidthVector) vec;
      readValues(total, rowId, vector::setSafe);
    } else if (vec instanceof FixedWidthVector) {
      BaseFixedWidthVector vector = (BaseFixedWidthVector) vec;
      readValues(total, rowId, (index, value) -> vector.setSafe(index, value, 0, value.length));
    }
  }

  private void readValues(int total, int rowId, BinaryOutputWriter outputWriter) {
    for (int i = 0; i < total; i++) {
      int prefixLength = prefixLengths[rowId + i];
      Binary suffix = suffixReader.readBinaryForRow(rowId + i);
      int length = prefixLength + suffix.length();

      if (prefixLength != 0) {
        byte[] out = new byte[length];
        System.arraycopy(previous.getBytesUnsafe(), 0, out, 0, prefixLength);
        System.arraycopy(suffix.getBytesUnsafe(), 0, out, prefixLength, suffix.length());
        outputWriter.write(rowId + i, out);
        previous = Binary.fromConstantByteArray(out);
      } else {
        outputWriter.write(rowId + i, suffix.getBytesUnsafe());
        previous = suffix;
      }
    }
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException("readBoolean is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public int readInteger() {
    throw new UnsupportedOperationException("readInteger is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public long readLong() {
    throw new UnsupportedOperationException("readLong is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public float readFloat() {
    throw new UnsupportedOperationException("readFloat is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public double readDouble() {
    throw new UnsupportedOperationException("readDouble is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readIntegers is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readLongs is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readFloats is not supported");
  }

  /** DELTA_BYTE_ARRAY only supports BINARY */
  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readDoubles is not supported");
  }

  /** A functional interface to write binary values into a FieldVector */
  @FunctionalInterface
  interface BinaryOutputWriter {

    /**
     * A functional interface that can be used to write a binary value to a specified row
     *
     * @param index The offset to write to
     * @param val value to write
     */
    void write(int index, byte[] val);
  }
}
