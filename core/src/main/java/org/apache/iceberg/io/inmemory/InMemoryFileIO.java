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
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * FileIO implementation backed by in-memory data-structures.
 * This class doesn't touch external resources and
 * can be utilized to write unit tests without side effects.
 * Locations can any string supported by the {@link InMemoryFileStore}.
 */
public final class InMemoryFileIO implements FileIO {

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
    if (!store.remove(path)) {
      throw new IllegalArgumentException("Location: " + path + " does not exist!");
    }
  }

  private class InMemoryInputFile implements InputFile {

    private final String location;

    InMemoryInputFile(String location) {
      this.location = location;
    }

    @Override
    public long getLength() {
      return getDataOrThrow().remaining();
    }

    @Override
    public SeekableInputStream newStream() {
      return new InMemoryInputStream(getDataOrThrow().duplicate());
    }

    private ByteBuffer getDataOrThrow() {
      return store.get(location).orElseThrow(
        () -> new UncheckedIOException(new FileNotFoundException("Cannot find file, does not exist: " + location))
      );
    }

    @Override
    public String location() {
      return location;
    }

    @Override
    public boolean exists() {
      return store.exists(location);
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
        throw new UncheckedIOException(new IOException("Cannot create file, already exists: " + location));
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
      long actuallySkipped = skip(newPos);
      if (actuallySkipped < newPos) {
        throw new UncheckedIOException(new EOFException("Cannot seek to position: " + newPos + ", EOF reached"));
      }
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

  private static class InMemoryInputStream extends SeekableInputStream {

    private final SeekableByteArrayInputStream inputStream;

    InMemoryInputStream(ByteBuffer buffer) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      this.inputStream = new SeekableByteArrayInputStream(bytes);
    }

    @Override
    public long getPos() throws IOException {
      return inputStream.position();
    }

    @Override
    public void seek(long newPos) {
      inputStream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return inputStream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return inputStream.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
      inputStream.close();
    }
  }

  private class InMemoryOutputStream extends PositionOutputStream {

    private final String location;
    private final ByteArrayOutputStream outputStream;

    InMemoryOutputStream(String location) {
      this.location = location;
      this.outputStream = new ByteArrayOutputStream();
    }

    @Override
    public long getPos() throws IOException {
      return outputStream.size();
    }

    @Override
    public void flush() throws IOException {
    }

    @Override
    public void write(int i) throws IOException {
      outputStream.write(i);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      outputStream.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
      outputStream.close();
      store.put(location, ByteBuffer.wrap(outputStream.toByteArray()));
    }
  }
}
