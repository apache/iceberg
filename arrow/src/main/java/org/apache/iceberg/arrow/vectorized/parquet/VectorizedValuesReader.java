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
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.io.api.Binary;

/** Interface for value decoding that supports vectorized (aka batched) decoding. */
interface VectorizedValuesReader {
  boolean readBoolean();

  byte readByte();

  short readShort();

  int readInteger();

  long readLong();

  float readFloat();

  double readDouble();

  Binary readBinary(int len);

  /*
   * Reads `total` values into `vec` start at `vec[rowId]`
   */
  void readIntegers(int total, FieldVector vec, int rowId);

  void readLongs(int total, FieldVector vec, int rowId);

  void readFloats(int total, FieldVector vec, int rowId);

  void readDoubles(int total, FieldVector vec, int rowId);

  void initFromPage(int valueCount, ByteBufferInputStream in);
}
