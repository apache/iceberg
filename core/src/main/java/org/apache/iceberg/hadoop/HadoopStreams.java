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
package org.apache.iceberg.hadoop;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience methods to get Parquet abstractions for Hadoop data streams.
 *
 * <p>This class is based on Parquet's HadoopStreams.
 */
public class HadoopStreams {

  private HadoopStreams() {}

  private static final Logger LOG = LoggerFactory.getLogger(HadoopStreams.class);

  /**
   * Wraps a {@link FSDataInputStream} in a {@link SeekableInputStream} implementation for readers.
   *
   * @param stream a Hadoop FSDataInputStream
   * @return a SeekableInputStream
   */
  static SeekableInputStream wrap(FSDataInputStream stream) {
    return new HadoopSeekableInputStream(stream);
  }

  /**
   * Wraps a {@link FSDataOutputStream} in a {@link PositionOutputStream} implementation for
   * writers.
   *
   * @param stream a Hadoop FSDataOutputStream
   * @return a PositionOutputStream
   */
  static PositionOutputStream wrap(FSDataOutputStream stream) {
    return new HadoopPositionOutputStream(stream);
  }

  /**
   * Wraps a {@link SeekableInputStream} in a {@link FSDataOutputStream} implementation for readers.
   *
   * @param stream a SeekableInputStream
   * @return a FSDataOutputStream
   */
  public static FSInputStream wrap(SeekableInputStream stream) {
    return new WrappedSeekableInputStream(stream);
  }

  /**
   * SeekableInputStream implementation for FSDataInputStream that implements ByteBufferReadable in
   * Hadoop 2.
   */
  private static class HadoopSeekableInputStream extends SeekableInputStream
      implements DelegatingInputStream {
    private final FSDataInputStream stream;
    private final StackTraceElement[] createStack;
    private boolean closed;

    HadoopSeekableInputStream(FSDataInputStream stream) {
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

    public int read(ByteBuffer buf) throws IOException {
      return stream.read(buf);
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

  /** PositionOutputStream implementation for FSDataOutputStream. */
  private static class HadoopPositionOutputStream extends PositionOutputStream
      implements DelegatingOutputStream {
    private final FSDataOutputStream stream;
    private final StackTraceElement[] createStack;
    private boolean closed;

    HadoopPositionOutputStream(FSDataOutputStream stream) {
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

  private static class WrappedSeekableInputStream extends FSInputStream
      implements DelegatingInputStream {
    private final SeekableInputStream inputStream;

    private WrappedSeekableInputStream(SeekableInputStream inputStream) {
      this.inputStream = inputStream;
    }

    @Override
    public void seek(long pos) throws IOException {
      inputStream.seek(pos);
    }

    @Override
    public long getPos() throws IOException {
      return inputStream.getPos();
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
      throw new UnsupportedOperationException("seekToNewSource not supported");
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

    @Override
    public InputStream getDelegate() {
      return inputStream;
    }
  }
}
