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
package org.apache.iceberg.util;

import java.io.IOException;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * {@link java.io.Writer} implementation that uses a hashing function to produce a hash value based
 * on the streamed bytes. The output of the writer is not preserved.
 */
public class HashWriter extends Writer {

  private final MessageDigest digest;
  private final CharsetEncoder encoder;
  private boolean isClosed = false;

  public HashWriter(String hashAlgorithm, Charset charset) throws NoSuchAlgorithmException {
    this.digest = MessageDigest.getInstance(hashAlgorithm);
    this.encoder = charset.newEncoder();
  }

  @Override
  public void write(char[] cbuf, int off, int len) throws IOException {
    ensureNotClosed();
    CharBuffer chars = CharBuffer.wrap(cbuf, off, len);
    ByteBuffer byteBuffer = encoder.encode(chars);
    digest.update(byteBuffer);
  }

  @Override
  public void flush() throws IOException {}

  @Override
  public void close() throws IOException {
    isClosed = true;
  }

  /**
   * Calculates the final hash value. The underlying digest will be reset thus subsequent getHash()
   * calls are not permitted.
   *
   * @return bytes of final hash value
   */
  public byte[] getHash() {
    ensureNotClosed();
    isClosed = true;
    return digest.digest();
  }

  private void ensureNotClosed() {
    if (isClosed) {
      throw new IllegalStateException("HashWriter is closed.");
    }
  }
}
