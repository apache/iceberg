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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

/**
 * Shared helpers for hardening tests of the vectorized Parquet value decoders in this package.
 * Subclasses use these primitives to assemble forged or hand-built encoded pages without each test
 * class re-deriving the same boilerplate.
 */
abstract class VectorizedParquetDecoderTestBase {

  @FunctionalInterface
  protected interface PageWriter {
    void write(ByteArrayOutputStream out) throws IOException;
  }

  /** Builds a page by composing writes into a {@link ByteArrayOutputStream}. */
  protected static byte[] page(PageWriter body) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    try {
      body.write(out);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return out.toByteArray();
  }

  /** Concatenates the given byte arrays in order. */
  protected static byte[] concat(byte[]... parts) {
    int total = 0;
    for (byte[] part : parts) {
      total += part.length;
    }
    byte[] result = new byte[total];
    int pos = 0;
    for (byte[] part : parts) {
      System.arraycopy(part, 0, result, pos, part.length);
      pos += part.length;
    }
    return result;
  }
}
