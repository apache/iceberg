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
package org.apache.iceberg.avro;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.avro.InvalidAvroMagicException;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.FileHandlingException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.SeekableInputStream;

class AvroIO {
  private static final byte[] AVRO_MAGIC = new byte[] {'O', 'b', 'j', 1};
  private static final ValueReader<byte[]> MAGIC_READER = ValueReaders.fixed(AVRO_MAGIC.length);
  private static final ValueReader<Map<String, String>> META_READER =
      ValueReaders.map(ValueReaders.strings(), ValueReaders.strings());
  private static final ValueReader<byte[]> SYNC_READER = ValueReaders.fixed(16);

  private AvroIO() {}

  private static final Class<?> fsDataInputStreamClass =
      DynClasses.builder().impl("org.apache.hadoop.fs.FSDataInputStream").orNull().build();

  private static final boolean relocated =
      "org.apache.avro.file.SeekableInput".equals(SeekableInput.class.getName());

  private static final DynConstructors.Ctor<SeekableInput> avroFsInputCtor =
      !relocated && fsDataInputStreamClass != null
          ? DynConstructors.builder(SeekableInput.class)
              .impl("org.apache.hadoop.fs.AvroFSInput", fsDataInputStreamClass, Long.TYPE)
              .build()
          : null;

  static SeekableInput stream(SeekableInputStream stream, long length) {
    if (stream instanceof DelegatingInputStream) {
      InputStream wrapped = ((DelegatingInputStream) stream).getDelegate();
      if (avroFsInputCtor != null
          && fsDataInputStreamClass != null
          && fsDataInputStreamClass.isInstance(wrapped)) {
        return avroFsInputCtor.newInstance(wrapped, length);
      }
    }
    return new AvroInputStreamAdapter(stream, length);
  }

  private static class AvroInputStreamAdapter extends SeekableInputStream implements SeekableInput {
    private final SeekableInputStream stream;
    private final long length;

    AvroInputStreamAdapter(SeekableInputStream stream, long length) {
      this.stream = stream;
      this.length = length;
    }

    @Override
    public void close() throws IOException {
      stream.close();
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
    public long tell() throws IOException {
      return getPos();
    }

    @Override
    public long length() throws IOException {
      return length;
    }

    @Override
    public int read() throws IOException {
      return stream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
      return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      return stream.skip(n);
    }

    @Override
    public int available() throws IOException {
      return stream.available();
    }

    @Override
    public synchronized void mark(int readlimit) {
      stream.mark(readlimit);
    }

    @Override
    public synchronized void reset() throws IOException {
      stream.reset();
    }

    @Override
    public boolean markSupported() {
      return stream.markSupported();
    }
  }

  static long findStartingRowPos(Supplier<SeekableInputStream> open, long start) {
    long totalRows = 0;
    try (SeekableInputStream in = open.get()) {
      // use a direct decoder that will not buffer so the position of the input stream is accurate
      BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(in, null);

      // an Avro file's layout looks like this:
      //   header|block|block|...
      // the header contains:
      //   magic|string-map|sync
      // each block consists of:
      //   row-count|compressed-size-in-bytes|block-bytes|sync

      // it is necessary to read the header here because this is the only way to get the expected
      // file sync bytes
      byte[] magic = MAGIC_READER.read(decoder, null);
      if (!Arrays.equals(AVRO_MAGIC, magic)) {
        throw new InvalidAvroMagicException("Not an Avro file");
      }

      META_READER.read(decoder, null); // ignore the file metadata, it isn't needed
      byte[] fileSync = SYNC_READER.read(decoder, null);

      // the while loop reads row counts and seeks past the block bytes until the next sync pos is
      // >= start, which
      // indicates that the next sync is the start of the split.
      byte[] blockSync = new byte[16];
      long nextSyncPos = in.getPos();

      while (nextSyncPos < start) {
        if (nextSyncPos != in.getPos()) {
          in.seek(nextSyncPos);
          SYNC_READER.read(decoder, blockSync);

          if (!Arrays.equals(fileSync, blockSync)) {
            throw new FileHandlingException("Invalid sync at %s", nextSyncPos);
          }
        }

        long rowCount = decoder.readLong();
        long compressedBlockSize = decoder.readLong();

        totalRows += rowCount;
        nextSyncPos = in.getPos() + compressedBlockSize;
      }

      return totalRows;

    } catch (EOFException e) {
      return totalRows;

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to read stream while finding starting row position");
    }
  }
}
