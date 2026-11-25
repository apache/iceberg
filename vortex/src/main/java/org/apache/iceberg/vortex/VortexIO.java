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
package org.apache.iceberg.vortex;

import dev.vortex.io.NativeReadable;
import dev.vortex.io.NativeWritable;
import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.util.functional.FutureIO;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Bridges Iceberg {@link org.apache.iceberg.io.FileIO} streams into Vortex's caller-provided native
 * I/O interfaces, so Vortex reads and writes go through the table's configured {@code FileIO}
 * instead of Vortex's own storage clients.
 */
final class VortexIO {
  // Largest staging array that a pooled stream keeps for later reads. Larger reads get an array of
  // their own.
  private static final int SCRATCH_RETAIN_LIMIT = 1 << 20;

  // The Hadoop openFile options that give the file length and the read policy. Names from
  // org.apache.hadoop.fs.Options.OpenFileOptions, which Hadoop 3.3.4 does not have.
  private static final String OPEN_FILE_LENGTH = "fs.option.openfile.length";
  private static final String OPEN_FILE_READ_POLICY = "fs.option.openfile.read.policy";
  private static final String RANDOM_READ_POLICY = "random";

  private VortexIO() {}

  /**
   * Opens the output file for writing and returns a {@link NativeWritable} over its stream. The
   * caller owns the result and must close it after the Vortex writer is finalized.
   */
  static NativeWritable writable(OutputFile outputFile) {
    return new OutputFileWritable(outputFile.createOrOverwrite());
  }

  /**
   * Returns a {@link NativeReadable} over the input file, safe for the concurrent positional reads
   * Vortex issues from its worker threads. The caller owns the result and must close it after every
   * data source and scan built on top of it has been closed.
   */
  static NativeReadable readable(InputFile inputFile) {
    return new InputFileReadable(inputFile);
  }

  private static final class OutputFileWritable implements NativeWritable {
    private final PositionOutputStream stream;

    private OutputFileWritable(PositionOutputStream stream) {
      this.stream = stream;
    }

    @Override
    public void write(byte[] buffer, int offset, int length) throws IOException {
      stream.write(buffer, offset, length);
    }

    @Override
    public void flush() throws IOException {
      stream.flush();
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /**
   * Serves each native read from a pooled stream of the file, opening a new stream when all pooled
   * streams are checked out by concurrent reads.
   *
   * <p>Vortex coalesces neighbouring reads before it calls {@link #readFully}, so each call reads
   * its whole range in one request to the stream. On object stores, each request to a new position
   * is a separate GET.
   */
  private static final class InputFileReadable implements NativeReadable {
    private final InputFile inputFile;
    private final long length;
    private final Queue<PooledStream> pool = new ConcurrentLinkedQueue<>();
    private volatile boolean closed = false;

    private InputFileReadable(InputFile inputFile) {
      this.inputFile = inputFile;
      this.length = inputFile.getLength();
    }

    @Override
    public String name() {
      return inputFile.location();
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public void readFully(long position, ByteBuffer buffer) throws IOException {
      Preconditions.checkState(!closed, "Cannot read: already closed");
      if (position + buffer.remaining() > length) {
        throw new EOFException(
            String.format(
                Locale.ROOT,
                "Cannot read %d bytes at position %d: %s is %d bytes long",
                buffer.remaining(),
                position,
                inputFile.location(),
                length));
      }

      PooledStream pooled = pool.poll();
      if (pooled == null) {
        pooled = new PooledStream(openStream());
      }

      try {
        pooled.copy(position, buffer);
      } catch (IOException | RuntimeException e) {
        try {
          pooled.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
        throw e;
      }

      pool.add(pooled);
      if (closed) {
        // Racing with close(): make sure nothing this read returned to the pool leaks.
        closeAllPooled();
      }
    }

    /**
     * Opens one more stream. A plain Hadoop {@code open} asks the store for the file status first,
     * which is one HEAD request on S3 for each stream. The length is known, so give it to the file
     * system and skip that request. Vortex reads ranges at random positions, so ask for the random
     * read policy too.
     */
    private PositionalStream openStream() throws IOException {
      if (!(inputFile instanceof HadoopInputFile hadoopFile)) {
        return new SeekableStream(inputFile.newStream());
      }

      FSDataInputStream stream =
          FutureIO.awaitFuture(
              hadoopFile
                  .getFileSystem()
                  .openFile(hadoopFile.getPath())
                  .opt(OPEN_FILE_LENGTH, Long.toString(length))
                  .opt(OPEN_FILE_READ_POLICY, RANDOM_READ_POLICY)
                  .build());
      return new HadoopStream(stream);
    }

    @Override
    public void close() throws IOException {
      closed = true;
      closeAllPooled();
    }

    private void closeAllPooled() {
      PooledStream pooled;
      while ((pooled = pool.poll()) != null) {
        try {
          pooled.close();
        } catch (IOException e) {
          throw new org.apache.iceberg.exceptions.RuntimeIOException(
              e, "Failed to close stream for %s", inputFile.location());
        }
      }
    }
  }

  /** One stream of a {@link InputFileReadable}, used by at most one read at a time. */
  private static final class PooledStream implements Closeable {
    private final PositionalStream stream;
    private byte[] scratch = new byte[0];

    private PooledStream(PositionalStream stream) {
      this.stream = stream;
    }

    /** Fills {@code buffer} from {@code position} with one request to the stream. */
    private void copy(long position, ByteBuffer buffer) throws IOException {
      int remaining = buffer.remaining();
      byte[] bytes = scratch(remaining);
      stream.readFully(position, bytes, remaining);
      buffer.put(bytes, 0, remaining);
    }

    /** An array of at least {@code size} bytes. Arrays larger than the limit are not kept. */
    private byte[] scratch(int size) {
      if (scratch.length >= size) {
        return scratch;
      }

      if (size > SCRATCH_RETAIN_LIMIT) {
        return new byte[size];
      }

      scratch = new byte[size];
      return scratch;
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /** A stream that reads a range of the file in one request. */
  private interface PositionalStream extends Closeable {
    /** Fills the first {@code length} bytes of {@code bytes} from {@code position}. */
    void readFully(long position, byte[] bytes, int length) throws IOException;
  }

  /** A Hadoop stream, which reads a range with one positional read. */
  private static final class HadoopStream implements PositionalStream {
    private final FSDataInputStream stream;

    private HadoopStream(FSDataInputStream stream) {
      this.stream = stream;
    }

    @Override
    public void readFully(long position, byte[] bytes, int length) throws IOException {
      stream.readFully(position, bytes, 0, length);
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  /** A stream of another {@link org.apache.iceberg.io.FileIO}. */
  private static final class SeekableStream implements PositionalStream {
    private final SeekableInputStream stream;

    private SeekableStream(SeekableInputStream stream) {
      this.stream = stream;
    }

    @Override
    public void readFully(long position, byte[] bytes, int length) throws IOException {
      if (stream instanceof RangeReadable range) {
        range.readFully(position, bytes, 0, length);
        return;
      }

      stream.seek(position);
      IOUtil.readFully(stream, bytes, 0, length);
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }
}
