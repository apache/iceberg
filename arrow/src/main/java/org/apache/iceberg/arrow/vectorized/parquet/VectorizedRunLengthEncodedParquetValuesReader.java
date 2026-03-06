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
}
