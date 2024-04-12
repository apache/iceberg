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
package org.apache.iceberg.flink;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkOutputFile implements OutputFile, NativelyEncryptedFile {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkOutputFile.class);

  private final FileSystem fs;
  private final Path path;
  private NativeFileCryptoParameters nativeDecryptionParameters;

  public FlinkOutputFile(Path path) {
    this.path = path;
    try {
      this.fs = path.getFileSystem();
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to get file system for path: %s", path), e);
    }
  }

  @Override
  public PositionOutputStream create() {
    try {
      return new FlinkPositionOutputStream(fs.create(path, FileSystem.WriteMode.NO_OVERWRITE));
    } catch (FileAlreadyExistsException e) {
      throw new AlreadyExistsException(e, "Path already exists: %s", path);
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to create file: %s", path), e);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    try {
      return new FlinkPositionOutputStream(fs.create(path, FileSystem.WriteMode.OVERWRITE));
    } catch (IOException e) {
      throw new UncheckedIOException(String.format("Failed to create file: %s", path), e);
    }
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public InputFile toInputFile() {
    return new FlinkInputFile(path);
  }

  /** PositionOutputStream implementation for FSDataOutputStream. */
  private static class FlinkPositionOutputStream extends PositionOutputStream
      implements DelegatingOutputStream {
    private final FSDataOutputStream stream;
    private final StackTraceElement[] createStack;
    private boolean closed;

    FlinkPositionOutputStream(FSDataOutputStream stream) {
      this.stream = stream;
      this.createStack = Thread.currentThread().getStackTrace();
      this.closed = false;
    }

    @Override
    public OutputStream getDelegate() {
      return stream;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void write(int b) throws IOException {
      stream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      stream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
      stream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      stream.flush();
    }

    @Override
    public void close() throws IOException {
      stream.close();
      this.closed = true;
    }

    @SuppressWarnings("checkstyle:NoFinalizer")
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        close(); // releasing resources is more important than printing the warning
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed output stream created by:\n\t{}", trace);
      }
    }
  }

  @Override
  public NativeFileCryptoParameters nativeCryptoParameters() {
    return nativeDecryptionParameters;
  }

  @Override
  public void setNativeCryptoParameters(NativeFileCryptoParameters nativeCryptoParameters) {
    this.nativeDecryptionParameters = nativeCryptoParameters;
  }
}
