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
 * Interface for value decoding that supports vectorized (aka batched) decoding. Implementations are
 * expected to be {@link ValuesReader} instances, and this interface "extends" that abstract class
 * by overriding the salient methods.
 */
interface VectorizedValuesReader {

  int INT_SIZE = 4;
  int LONG_SIZE = 8;
  int FLOAT_SIZE = 4;
  int DOUBLE_SIZE = 8;

  /** Read a single boolean */
  default boolean readBoolean() {
    throw new UnsupportedOperationException();
  }

  /** Read a single byte */
  default byte readByte() {
    throw new UnsupportedOperationException();
  }

  /** Read a single short */
  default short readShort() {
    throw new UnsupportedOperationException();
  }

  /** Read a single integer */
  default int readInteger() {
    throw new UnsupportedOperationException();
  }

  /** Read a single long */
  default long readLong() {
    throw new UnsupportedOperationException();
  }

  /** Read a single float */
  default float readFloat() {
    throw new UnsupportedOperationException();
  }

  /** Read a single double */
  default double readDouble() {
    throw new UnsupportedOperationException();
  }

  /**
   * Read binary data of some length
   *
   * @param len The number of bytes to read
   */
  default Binary readBinary(int len) {
    throw new UnsupportedOperationException();
  }

  /** Read `total` integers into `vec` starting at `vec[rowId]` */
  default void readIntegers(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException();
  }

  /** Read `total` longs into `vec` starting at `vec[rowId]` */
  default void readLongs(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException();
  }

  /** Read `total` floats into `vec` starting at `vec[rowId]` */
  default void readFloats(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException();
  }

  /** Read `total` doubles into `vec` starting at `vec[rowId]` */
  default void readDoubles(int total, FieldVector vec, int rowId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Initialize the reader from a page. See {@link ValuesReader#initFromPage(int,
   * ByteBufferInputStream)}.
   */
  void initFromPage(int valueCount, ByteBufferInputStream in) throws IOException;
}
