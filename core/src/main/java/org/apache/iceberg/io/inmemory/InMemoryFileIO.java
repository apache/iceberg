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

package org.apache.iceberg.io.inmemory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.ByteBuffers;

public class InMemoryFileIO implements FileIO {
  private final InMemoryFileStore store;

  public InMemoryFileIO() {
    store = new InMemoryFileStore();
  }

  InMemoryFileStore getStore() {
    return store;
  }

  @Override
  public InputFile newInputFile(String path) {
    return new InMemoryInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return new InMemoryOutputFile(path);
  }

  @Override
  public void deleteFile(String path) {
    Preconditions.checkArgument(store.remove(path), "File at location '%s' does not exist", path);
  }

  private class InMemoryInputFile implements InputFile {
    private final String location;

    InMemoryInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      return getOrThrow().remaining();
    }

    @Override
    public SeekableInputStream newStream() {
      return new InMemorySeekableInputStream(getOrThrow().duplicate());
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return store.exists(location);
    }

    private ByteBuffer getOrThrow() {
      return store
          .get(location)
          .orElseThrow(
              () ->
                  new UncheckedIOException(
                      new FileNotFoundException(
                          String.format("File at location '%s' does not exist", location))));
    }
  }

  private class InMemoryOutputFile implements OutputFile {
    private final String location;

    InMemoryOutputFile(String location) {
      this.location = location;
    }

    @Override
    public PositionOutputStream create() {
      if (store.exists(location)) {
        throw new UncheckedIOException(
            new IOException(String.format("File at location '%s' already exists", location)));
      }
      return new InMemoryOutputStream(location);
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      return new InMemoryOutputStream(location);
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public InputFile toInputFile() {
      return new InMemoryInputFile(location);
    }
  }

  private static class SeekableByteArrayInputStream extends ByteArrayInputStream {

    SeekableByteArrayInputStream(byte[] buf) {
      super(buf);
    }

    void seek(long newPos) {
      reset();
      Preconditions.checkState(
          skip(newPos) == newPos, "Invalid position %s within stream of length %s", newPos, count);
    }

    @Override
    public void mark(int readAheadLimit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean markSupported() {
      return false;
    }

    long position() {
      return pos;
    }
  }

  private static class InMemorySeekableInputStream extends SeekableInputStream {

    private final SeekableByteArrayInputStream delegate;
    private boolean closed = false;

    InMemorySeekableInputStream(ByteBuffer buffer) {
      this.delegate = new SeekableByteArrayInputStream(ByteBuffers.toByteArray(buffer));
    }

    @Override
    public long getPos() throws IOException {
      checkOpen();
      return delegate.position();
    }

    @Override
    public void seek(long newPos) {
      checkOpen();
      delegate.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      checkOpen();
      return delegate.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      checkOpen();
      return delegate.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      checkOpen();
      return delegate.skip(n);
    }

    @Override
    public int available() throws IOException {
      checkOpen();
      return delegate.available();
    }

    @Override
    public synchronized void reset() throws IOException {
      checkOpen();
      delegate.reset();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      closed = true;
    }

    private void checkOpen() {
      // ByteArrayInputStream can be used even after close, so for test purposes disallow such use explicitly
      Preconditions.checkState(!closed, "Stream is closed");
    }
  }

  private class InMemoryOutputStream extends PositionOutputStream {
    private final String location;
    private final ByteArrayOutputStream delegate;
    private boolean closed = false;

    InMemoryOutputStream(String location) {
      this.location = location;
      this.delegate = new ByteArrayOutputStream();
    }

    @Override
    public long getPos() {
      return delegate.size();
    }

    @Override
    public void write(int b) {
      checkOpen();
      delegate.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      checkOpen();
      delegate.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      checkOpen();
      delegate.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      checkOpen();
      delegate.flush();
    }

    @Override
    public void close() throws IOException {
      delegate.close();
      store.put(location, ByteBuffer.wrap(delegate.toByteArray()));
      closed = true;
    }

    private void checkOpen() {
      // ByteArrayInputStream can be used even after close, so for test purposes disallow such use explicitly
      Preconditions.checkState(!closed, "Stream is closed");
    }
  }
}
