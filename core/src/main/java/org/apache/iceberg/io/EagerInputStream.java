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

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** A seekable, range-readable stream backed by a byte array fetched in a single remote call. */
class EagerInputStream extends SeekableInputStream implements RangeReadable {

  private final byte[] contents;
  private final ByteArrayInputStream delegate;

  EagerInputStream(byte[] contents) {
    Preconditions.checkNotNull(contents, "contents is null");
    this.contents = contents;
    this.delegate = new ByteArrayInputStream(contents);
  }

  @Override
  public long getPos() throws IOException {
    return (long) contents.length - delegate.available();
  }

  @Override
  public void seek(long newPos) throws IOException {
    Preconditions.checkArgument(newPos >= 0, "position is negative: %s", newPos);
    delegate.reset();
    if (skip(newPos) != newPos) {
      throw new EOFException(
          "Cannot seek to position " + newPos + ": exceeds stream length " + contents.length);
    }
  }

  @Override
  public int read() throws IOException {
    return delegate.read();
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    return delegate.read(b, off, len);
  }

  @Override
  public long skip(long n) throws IOException {
    return delegate.skip(n);
  }

  @Override
  public int available() throws IOException {
    return delegate.available();
  }

  @Override
  public void readFully(long pos, byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);
    Preconditions.checkArgument(pos >= 0, "position is negative: %s", pos);
    if (pos > contents.length || length > contents.length - pos) {
      throw new EOFException(
          "Cannot read "
              + length
              + " bytes at position "
              + pos
              + ": exceeds stream length "
              + contents.length);
    }
    System.arraycopy(contents, (int) pos, buffer, offset, length);
  }

  @Override
  public int readTail(byte[] buffer, int offset, int length) throws IOException {
    Preconditions.checkPositionIndexes(offset, offset + length, buffer.length);
    int bytesToCopy = Math.min(length, contents.length);
    System.arraycopy(contents, contents.length - bytesToCopy, buffer, offset, bytesToCopy);
    return bytesToCopy;
  }

  @Override
  public void close() throws IOException {}
}
