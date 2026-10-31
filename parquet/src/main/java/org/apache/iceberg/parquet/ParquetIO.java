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
package org.apache.iceberg.parquet;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.iceberg.io.FileRange;
import org.apache.iceberg.io.RangeReadable;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.io.ByteStreams;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.ConfigurationUtil;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.ParquetFileRange;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

/** Methods in this class translate from the IO API to Parquet's IO API. */
class ParquetIO {
  // number of bytes read from the end of a file to get the footer in a single request; this matches
  // the default footer read size used by the Arrow C++ Parquet reader
  @VisibleForTesting static final int FOOTER_READ_SIZE = 64 * 1024;
  // footer length (4 bytes) followed by the magic (4 bytes)
  private static final int FOOTER_LENGTH_AND_MAGIC_SIZE = 8;

  private ParquetIO() {}

  /**
   * Opens a {@link ParquetFileReader} for the given file.
   *
   * <p>Parquet reads the footer length at the end of the file and then seeks backward to read the
   * footer, which costs two sequential requests when the file is stored in an object store. When
   * the stream supports {@link RangeReadable}, the end of the file is read once and the footer is
   * parsed from it instead. A second read is needed only when the footer is larger than {@link
   * #FOOTER_READ_SIZE}.
   */
  static ParquetFileReader open(InputFile file, ParquetReadOptions options) throws IOException {
    SeekableInputStream stream = file.newStream();
    if (!(stream instanceof RangeReadable)) {
      return new ParquetFileReader(file, options, stream);
    }

    ParquetMetadata footer;
    try {
      footer = readFooter(file, options, (RangeReadable) stream);
    } catch (IOException | RuntimeException e) {
      try {
        stream.close();
      } catch (IOException closeException) {
        e.addSuppressed(closeException);
      }

      throw e;
    }

    return new ParquetFileReader(file, footer, options, stream);
  }

  private static ParquetMetadata readFooter(
      InputFile file, ParquetReadOptions options, RangeReadable stream) throws IOException {
    long fileLength = file.getLength();
    byte[] tail = readTail(stream, (int) Math.min(fileLength, FOOTER_READ_SIZE));

    if (tail.length >= FOOTER_LENGTH_AND_MAGIC_SIZE) {
      long footerLength =
          ByteBuffer.wrap(tail)
                  .order(ByteOrder.LITTLE_ENDIAN)
                  .getInt(tail.length - FOOTER_LENGTH_AND_MAGIC_SIZE)
              & 0xFFFFFFFFL;
      long requiredLength = footerLength + FOOTER_LENGTH_AND_MAGIC_SIZE;
      if (requiredLength > tail.length
          && requiredLength <= Math.min(fileLength, Integer.MAX_VALUE)) {
        tail = readTail(stream, (int) requiredLength);
      }
    }

    // Parquet validates the file length, magic, and footer location, and handles encrypted footers
    return ParquetFileReader.readFooter(file, options, new TailInputStream(tail, fileLength));
  }

  private static byte[] readTail(RangeReadable stream, int length) throws IOException {
    byte[] tail = new byte[length];
    int bytesRead = stream.readTail(tail, 0, length);
    if (bytesRead != length) {
      throw new EOFException(
          String.format(
              Locale.ROOT,
              "Reached the end of stream with %d bytes left to read",
              length - bytesRead));
    }

    return tail;
  }

  static InputFile file(org.apache.iceberg.io.InputFile file) {
    // TODO: use reflection to avoid depending on classes from iceberg-hadoop
    // TODO: use reflection to avoid depending on classes from hadoop
    if (file instanceof HadoopInputFile) {
      HadoopInputFile hfile = (HadoopInputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(
            hfile.getStat(), hfile.getConf());
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create Parquet input file for %s", file.location());
      }
    }
    return new ParquetInputFile(file);
  }

  static OutputFile file(org.apache.iceberg.io.OutputFile file) {
    if (file instanceof HadoopOutputFile) {
      HadoopOutputFile hfile = (HadoopOutputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
            hfile.getPath(), hfile.getConf());
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create Parquet output file for %s", file.location());
      }
    }
    return new ParquetOutputFile(file);
  }

  static OutputFile file(org.apache.iceberg.io.OutputFile file, ParquetConfiguration conf) {
    if (file instanceof HadoopOutputFile) {
      HadoopOutputFile hfile = (HadoopOutputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(
            hfile.getPath(), ConfigurationUtil.createHadoopConfiguration(conf));
      } catch (IOException e) {
        throw new RuntimeIOException(
            e, "Failed to create Parquet output file for %s", file.location());
      }
    }
    return new ParquetOutputFile(file);
  }

  static SeekableInputStream stream(org.apache.iceberg.io.SeekableInputStream stream) {
    if (stream instanceof DelegatingInputStream) {
      InputStream wrapped = ((DelegatingInputStream) stream).getDelegate();
      if (wrapped instanceof FSDataInputStream) {
        return HadoopStreams.wrap((FSDataInputStream) wrapped);
      }
    }

    if (stream instanceof RangeReadable) {
      return new ParquetRangeReadableInputStreamAdapter(stream);
    }

    return new ParquetInputStreamAdapter(stream);
  }

  static PositionOutputStream stream(org.apache.iceberg.io.PositionOutputStream stream) {
    if (stream instanceof DelegatingOutputStream) {
      OutputStream wrapped = ((DelegatingOutputStream) stream).getDelegate();
      if (wrapped instanceof FSDataOutputStream) {
        return HadoopStreams.wrap((FSDataOutputStream) wrapped);
      }
    }
    return new ParquetOutputStreamAdapter(stream);
  }

  private static class ParquetInputStreamAdapter extends DelegatingSeekableInputStream {
    private final org.apache.iceberg.io.SeekableInputStream delegate;

    private ParquetInputStreamAdapter(org.apache.iceberg.io.SeekableInputStream delegate) {
      super(delegate);
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
  }

  @VisibleForTesting
  static class ParquetRangeReadableInputStreamAdapter<
          T extends org.apache.iceberg.io.SeekableInputStream & RangeReadable>
      extends DelegatingSeekableInputStream implements RangeReadable {
    private final T delegate;

    ParquetRangeReadableInputStreamAdapter(T delegate) {
      super(delegate);
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
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
      delegate.readFully(position, buffer, offset, length);
    }

    @Override
    public int readTail(byte[] buffer, int offset, int length) throws IOException {
      return delegate.readTail(buffer, offset, length);
    }

    @Override
    public boolean readVectoredAvailable(ByteBufferAllocator allocate) {
      return true;
    }

    @Override
    public void readVectored(List<ParquetFileRange> ranges, ByteBufferAllocator allocate)
        throws IOException {
      List<FileRange> delegateRange = convertRanges(ranges);
      delegate.readVectored(delegateRange, allocate::allocate);
    }

    private static List<FileRange> convertRanges(List<ParquetFileRange> ranges) {
      return ranges.stream()
          .map(
              parquetFileRange -> {
                CompletableFuture<ByteBuffer> future = new CompletableFuture<>();
                parquetFileRange.setDataReadFuture(future);
                return new FileRange(
                    parquetFileRange.getDataReadFuture(),
                    parquetFileRange.getOffset(),
                    parquetFileRange.getLength());
              })
          .collect(Collectors.toList());
    }
  }

  /** A stream over the last bytes of a file that uses positions relative to the whole file. */
  private static class TailInputStream extends DelegatingSeekableInputStream {
    private final ByteArrayInputStream stream;
    private final long fileLength;
    private final long tailStart;

    private TailInputStream(byte[] tail, long fileLength) {
      this(new ByteArrayInputStream(tail), fileLength, fileLength - tail.length);
    }

    private TailInputStream(ByteArrayInputStream stream, long fileLength, long tailStart) {
      super(stream);
      this.stream = stream;
      this.fileLength = fileLength;
      this.tailStart = tailStart;
    }

    @Override
    public long getPos() {
      return fileLength - stream.available();
    }

    @Override
    public void seek(long newPos) throws IOException {
      if (newPos < tailStart || newPos > fileLength) {
        throw new EOFException(
            String.format(
                Locale.ROOT,
                "Cannot seek to %d, outside of the buffered range [%d, %d]",
                newPos,
                tailStart,
                fileLength));
      }

      stream.reset();
      ByteStreams.skipFully(stream, newPos - tailStart);
    }
  }

  private static class ParquetOutputStreamAdapter extends DelegatingPositionOutputStream {
    private final org.apache.iceberg.io.PositionOutputStream delegate;

    private ParquetOutputStreamAdapter(org.apache.iceberg.io.PositionOutputStream delegate) {
      super(delegate);
      this.delegate = delegate;
    }

    @Override
    public long getPos() throws IOException {
      return delegate.getPos();
    }
  }

  private static class ParquetOutputFile implements OutputFile {
    private final org.apache.iceberg.io.OutputFile file;

    private ParquetOutputFile(org.apache.iceberg.io.OutputFile file) {
      this.file = file;
    }

    @Override
    public PositionOutputStream create(long ignored) throws IOException {
      return stream(file.create());
    }

    @Override
    public PositionOutputStream createOrOverwrite(long ignored) throws IOException {
      return stream(file.createOrOverwrite());
    }

    @Override
    public boolean supportsBlockSize() {
      return false;
    }

    @Override
    public long defaultBlockSize() {
      return 0;
    }
  }

  private static class ParquetInputFile implements InputFile {
    private final org.apache.iceberg.io.InputFile file;

    private ParquetInputFile(org.apache.iceberg.io.InputFile file) {
      this.file = file;
    }

    @Override
    public long getLength() throws IOException {
      return file.getLength();
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
      return stream(file.newStream());
    }
  }
}
