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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DelegatingInputStream;
import org.apache.iceberg.io.DelegatingOutputStream;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.DelegatingPositionOutputStream;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

/** Methods in this class translate from the IO API to Parquet's IO API. */
class ParquetIO {
  private ParquetIO() {}

  static InputFile file(org.apache.iceberg.io.InputFile file) {
    // TODO: use reflection to avoid depending on classes from iceberg-hadoop
    // TODO: use reflection to avoid depending on classes from hadoop
    if (file instanceof HadoopInputFile) {
      HadoopInputFile hfile = (HadoopInputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopInputFile.fromStatus(
            hfile.getStat(), hfile.getConf());
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create Parquet input file for %s", file);
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
        throw new RuntimeIOException(e, "Failed to create Parquet output file for %s", file);
      }
    }
    return new ParquetOutputFile(file);
  }

  static OutputFile file(org.apache.iceberg.io.OutputFile file, Configuration conf) {
    if (file instanceof HadoopOutputFile) {
      HadoopOutputFile hfile = (HadoopOutputFile) file;
      try {
        return org.apache.parquet.hadoop.util.HadoopOutputFile.fromPath(hfile.getPath(), conf);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to create Parquet output file for %s", file);
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
