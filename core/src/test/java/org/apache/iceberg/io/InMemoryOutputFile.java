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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.UUID;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class InMemoryOutputFile implements OutputFile {

  private final String location;

  private boolean exists = false;
  private ByteArrayOutputStream contents;

  public InMemoryOutputFile() {
    this("memory:" + UUID.randomUUID());
  }

  public InMemoryOutputFile(String location) {
    Preconditions.checkNotNull(location, "location is null");
    this.location = location;
  }

  @Override
  public PositionOutputStream create() {
    if (exists) {
      throw new AlreadyExistsException("Already exists");
    }
    return createOrOverwrite();
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    exists = true;
    contents = new ByteArrayOutputStream();
    return new InMemoryPositionOutputStream(contents);
  }

  @Override
  public String location() {
    return location;
  }

  @Override
  public InputFile toInputFile() {
    Preconditions.checkState(exists, "Cannot convert a file that has not been written yet");
    return new InMemoryInputFile(location(), toByteArray());
  }

  public byte[] toByteArray() {
    return contents.toByteArray();
  }

  private static class InMemoryPositionOutputStream extends PositionOutputStream {
    private final ByteArrayOutputStream delegate;
    private boolean closed = false;

    InMemoryPositionOutputStream(ByteArrayOutputStream delegate) {
      Preconditions.checkNotNull(delegate, "delegate is null");
      this.delegate = delegate;
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
      closed = true;
    }

    private void checkOpen() {
      // ByteArrayOutputStream can be used even after close, so for test purposes disallow such use explicitly
      Preconditions.checkState(!closed, "Stream is closed");
    }
  }
}
