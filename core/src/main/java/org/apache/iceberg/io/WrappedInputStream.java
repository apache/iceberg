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

package org.apache.iceberg.io;

import java.io.IOException;

/**
 * Wrapping InputFiles can have HadoopInputFile as rawInput. Therefore, rawInput's stream can be
 * FSDataInputStream. This triggers problems (closed stream) due to Hadoop handling in e.g. ParquetIO class.
 * Stream wrapping solves this.
 */
public class WrappedInputStream extends SeekableInputStream {

  private final SeekableInputStream wrappedInput;

  public WrappedInputStream(SeekableInputStream wrappedInput) {
    this.wrappedInput = wrappedInput;
  }

  @Override
  public long getPos() throws IOException {
    return wrappedInput.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    wrappedInput.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return wrappedInput.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return wrappedInput.read(b, off, len);
  }

  @Override
  public void close() throws IOException {
    super.close();
    wrappedInput.close();
  }

  @SuppressWarnings("checkstyle:NoFinalizer")
  @Override
  protected void finalize() throws Throwable {
    close();
  }
}
