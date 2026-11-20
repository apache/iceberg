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
package org.apache.iceberg.io.canonicaluri;

import java.io.IOException;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;

/** Retains a delegate stream without exposing it for downstream adapter unwrapping. */
class CanonicalSeekableInputStream extends SeekableInputStream {
  private final SeekableInputStream delegate;

  static SeekableInputStream wrap(SeekableInputStream delegate) {
    return delegate instanceof RangeReadable
        ? new CanonicalRangeReadableInputStream(delegate)
        : new CanonicalSeekableInputStream(delegate);
  }

  private CanonicalSeekableInputStream(SeekableInputStream delegate) {
    this.delegate = delegate;
  }

  @Override
  public long getPos() throws IOException {
    return delegate.getPos();
  }

  @Override
  public void seek(long newPos) throws IOException {
    delegate.seek(newPos);
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read(byte[] buffer, int off, int len) throws IOException {
    return delegate.read(buffer, off, len);
  }

  @Override
  public long skip(long length) throws IOException {
    return delegate.skip(length);
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void mark(int readLimit) {
    delegate.mark(readLimit);
  }

  @Override
  public void reset() throws IOException {
    delegate.reset();
  }

  @Override
  public boolean markSupported() {
    return delegate.markSupported();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }

  private static class CanonicalRangeReadableInputStream extends CanonicalSeekableInputStream
      implements RangeReadable {
    private final RangeReadable rangeDelegate;

    private CanonicalRangeReadableInputStream(SeekableInputStream delegate) {
      super(delegate);
      this.rangeDelegate = (RangeReadable) delegate;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      rangeDelegate.readFully(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      return rangeDelegate.readTail(buffer, offset, length);
    }
  }
}
