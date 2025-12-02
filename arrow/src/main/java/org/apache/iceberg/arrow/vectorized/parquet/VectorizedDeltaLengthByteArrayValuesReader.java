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
import java.util.function.IntUnaryOperator;
import org.apache.arrow.vector.BaseVariableWidthVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type DELTA_LENGTH_BYTE_ARRAY. This
 * is adapted from Spark's VectorizedDeltaLengthByteArrayReader.
 *
 * @see <a
 *     href="https://github.com/apache/parquet-format/blob/master/Encodings.md#delta-length-byte-array-delta_length_byte_array--6">
 *     Parquet format encodings: DELTA_LENGTH_BYTE_ARRAY</a>
 */
public class VectorizedDeltaLengthByteArrayValuesReader implements VectorizedValuesReader {

  private final VectorizedDeltaEncodedValuesReader lengthReader;

  private ByteBufferInputStream in;
  private int[] lengths;
  private ByteBuffer byteBuffer;

  VectorizedDeltaLengthByteArrayValuesReader() {
    lengthReader = new VectorizedDeltaEncodedValuesReader();
  }

  @Override
  public void initFromPage(int valueCount, ByteBufferInputStream inputStream) throws IOException {
    lengthReader.initFromPage(valueCount, inputStream);
    lengths = lengthReader.readIntegers(valueCount, 0);

    this.in = inputStream.remainingStream();
  }

  @Override
  public Binary readBinary(int len) {
    readValues(1, null, 0, x -> len, (f, i, v) -> byteBuffer = v);
    return Binary.fromReusedByteBuffer(byteBuffer);
  }

  @Override
  public void readBinary(int total, FieldVector vec, int rowId, boolean setArrowValidityVector) {
    readValues(
        total,
        vec,
        rowId,
        x -> lengths[x],
        (f, i, v) ->
            ((BaseVariableWidthVector) vec)
                .setSafe(
                    (int) i, v.array(), v.position() + v.arrayOffset(), v.limit() - v.position()));
  }

  @SuppressWarnings("UnusedVariable")
  private void readValues(
      int total,
      FieldVector vec,
      int rowId,
      IntUnaryOperator getLength,
      BinaryOutputWriter outputWriter) {
    ByteBuffer buffer;
    for (int i = 0; i < total; i++) {
      int length = getLength.applyAsInt(rowId + i);
      try {
        if (length <= 0) {
          throw new IllegalStateException("Invalid length: " + length);
        }
        buffer = in.slice(length);
      } catch (EOFException e) {
        throw new ParquetDecodingException("Failed to read " + length + " bytes");
      }
      outputWriter.write(vec, rowId + i, buffer);
    }
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public boolean readBoolean() {
    throw new UnsupportedOperationException("readBoolean is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public int readInteger() {
    throw new UnsupportedOperationException("readInteger is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public long readLong() {
    throw new UnsupportedOperationException("readLong is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public float readFloat() {
    throw new UnsupportedOperationException("readFloat is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public double readDouble() {
    throw new UnsupportedOperationException("readDouble is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readIntegers is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readLongs is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readFloats is not supported");
  }

  /** DELTA_LENGTH_BYTE_ARRAY only supports BINARY */
  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readDoubles is not supported");
  }

  /** A functional interface to write binary values into a FieldVector */
  @FunctionalInterface
  interface BinaryOutputWriter {

    /**
     * A functional interface that can be used to write a binary value to a specified row in a
     * FieldVector
     *
     * @param vec a FieldVector to write the value into
     * @param index The offset to write to
     * @param val value to write
     */
    void write(FieldVector vec, long index, ByteBuffer val);
  }
}
