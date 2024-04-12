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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.iceberg.encryption.NativeFileCryptoParameters;
import org.apache.iceberg.encryption.NativelyEncryptedFile;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FlinkInputFile implements InputFile, NativelyEncryptedFile {
  private static final Logger LOG = LoggerFactory.getLogger(FlinkInputFile.class);

  private final Path path;
  private final FileSystem fs;
  private FileStatus stat = null;
  private Long length = null;
  private NativeFileCryptoParameters nativeDecryptionParameters;

  public FlinkInputFile(Path path) {
    this.path = path;
    try {
      this.fs = path.getFileSystem();
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to get file system for path: %s", path), e);
    }
  }

  public FlinkInputFile(Path path, long length) {
    this(path);
    this.length = length;
  }

  @Override
  public long getLength() {
    if (length == null) {
      this.length = lazyStat().getLen();
    }

    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return new FlinkSeekableInputStream(path.getFileSystem().open(path));
    } catch (FileNotFoundException e) {
      throw new NotFoundException(e, "Failed to open input stream for file: %s", path);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to open input stream for file: %s", path), e);
    }
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public boolean exists() {
    try {
      return lazyStat() != null;
    } catch (NotFoundException e) {
      return false;
    }
  }

  private FileStatus lazyStat() {
    if (stat == null) {
      try {
        this.stat = fs.getFileStatus(path);
      } catch (FileNotFoundException e) {
        throw new NotFoundException(e, "File does not exist: %s", path);
      } catch (IOException e) {
        throw new UncheckedIOException(String.format("Failed to get status for file: %s", path), e);
      }
    }

    return stat;
  }

  /** SeekableInputStream implementation for Flink FSDataInputStream. */
  private static class FlinkSeekableInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final FSDataInputStream stream;
    private final StackTraceElement[] createStack;
    private boolean closed;

    FlinkSeekableInputStream(FSDataInputStream stream) {
      this.stream = stream;
      this.createStack = Thread.currentThread().getStackTrace();
      this.closed = false;
    }

    @Override
    public InputStream getDelegate() {
      return stream;
    }

    @Override
    public void close() throws IOException {
      stream.close();
      this.closed = true;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public void seek(long newPos) throws IOException {
      stream.seek(newPos);
    }

    @Override
    public int read() throws IOException {
      return stream.read();
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    @SuppressWarnings("checkstyle:NoFinalizer")
    @Override
    protected void finalize() throws Throwable {
      super.finalize();
      if (!closed) {
        close(); // releasing resources is more important than printing the warning
        String trace =
            Joiner.on("\n\t").join(Arrays.copyOfRange(createStack, 1, createStack.length));
        LOG.warn("Unclosed input stream created by:\n\t{}", trace);
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
