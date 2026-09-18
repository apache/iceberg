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
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Locale;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
  private static final int COPY_CHUNK_SIZE = 1 << 20;

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
   * Serves each native read from a pooled {@link SeekableInputStream}, opening a new stream when
   * all pooled streams are checked out by concurrent reads.
   */
  private static final class InputFileReadable implements NativeReadable {
    private final InputFile inputFile;
    private final long length;
    private final Queue<SeekableInputStream> pool = new ConcurrentLinkedQueue<>();
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

      SeekableInputStream stream = pool.poll();
      if (stream == null) {
        stream = inputFile.newStream();
      }

      try {
        copy(stream, position, buffer);
      } catch (IOException | RuntimeException e) {
        try {
          stream.close();
        } catch (IOException suppressed) {
          e.addSuppressed(suppressed);
        }
        throw e;
      }

      pool.add(stream);
      if (closed) {
        // Racing with close(): make sure nothing this read returned to the pool leaks.
        closeAllPooled();
      }
    }

    private void copy(SeekableInputStream stream, long position, ByteBuffer buffer)
        throws IOException {
      byte[] chunk = new byte[Math.min(buffer.remaining(), COPY_CHUNK_SIZE)];
      if (stream instanceof RangeReadable range) {
        long pos = position;
        while (buffer.hasRemaining()) {
          int len = Math.min(chunk.length, buffer.remaining());
          range.readFully(pos, chunk, 0, len);
          buffer.put(chunk, 0, len);
          pos += len;
        }
      } else {
        stream.seek(position);
        while (buffer.hasRemaining()) {
          int read = stream.read(chunk, 0, Math.min(chunk.length, buffer.remaining()));
          if (read < 0) {
            throw new EOFException(
                "Unexpected end of stream reading " + inputFile.location() + " at " + position);
          }
          buffer.put(chunk, 0, read);
        }
      }
    }

    @Override
    public void close() throws IOException {
      closed = true;
      closeAllPooled();
    }

    private void closeAllPooled() {
      SeekableInputStream stream;
      while ((stream = pool.poll()) != null) {
        try {
          stream.close();
        } catch (IOException e) {
          throw new org.apache.iceberg.exceptions.RuntimeIOException(
              e, "Failed to close stream for %s", inputFile.location());
        }
      }
    }
  }
}
