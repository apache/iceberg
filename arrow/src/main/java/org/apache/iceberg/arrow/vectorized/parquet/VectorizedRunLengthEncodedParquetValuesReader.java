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

import org.apache.arrow.vector.FieldVector;
import org.apache.parquet.io.api.Binary;

/**
 * A {@link VectorizedValuesReader} implementation for the encoding type Run Length Encoding / RLE.
 *
 * @see <a
 *     href="https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3">
 *     Parquet format encodings: RLE</a>
 */
public class VectorizedRunLengthEncodedParquetValuesReader extends BaseVectorizedParquetValuesReader
    implements VectorizedValuesReader {

  // Since we can only read booleans, bit-width is always 1
  private static final int BOOLEAN_BIT_WIDTH = 1;
  // Since this can only be used in the context of a data page, the definition level can be set to
  // anything, and it doesn't really matter
  private static final int IRRELEVANT_MAX_DEFINITION_LEVEL = 1;
  // For boolean values in data page v1 & v2, length is always prepended to the encoded data
  // See
  // https://parquet.apache.org/docs/file-format/data-pages/encodings/#run-length-encoding--bit-packing-hybrid-rle--3
  private static final boolean ALWAYS_READ_LENGTH = true;

  public VectorizedRunLengthEncodedParquetValuesReader(boolean setArrowValidityVector) {
    super(
        BOOLEAN_BIT_WIDTH,
        IRRELEVANT_MAX_DEFINITION_LEVEL,
        ALWAYS_READ_LENGTH,
        setArrowValidityVector);
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public byte readByte() {
    throw new UnsupportedOperationException("readByte is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public short readShort() {
    throw new UnsupportedOperationException("readShort is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public long readLong() {
    throw new UnsupportedOperationException("readLong is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public float readFloat() {
    throw new UnsupportedOperationException("readFloat is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public double readDouble() {
    throw new UnsupportedOperationException("readDouble is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public Binary readBinary(int len) {
    throw new UnsupportedOperationException("readBinary is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readIntegers is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readLongs is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readFloats is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException("readDoubles is not supported");
  }

  /** RLE only supports BOOLEAN as a data page encoding */
  @Override
  public void readBinary(int total, FieldVector vec, int rowId, boolean setArrowValidityVector) {
    throw new UnsupportedOperationException("readBinary is not supported");
  }
}
