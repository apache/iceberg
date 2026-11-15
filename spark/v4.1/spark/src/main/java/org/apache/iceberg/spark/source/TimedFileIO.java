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
package org.apache.iceberg.spark.source;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;

/** A {@link FileIO} that records the time Hadoop output files spend in storage calls. */
class TimedFileIO implements FileIO {
  private final FileIO io;
  private final WriteTimer timer;

  TimedFileIO(FileIO io, WriteTimer timer) {
    this.io = io;
    this.timer = timer;
  }

  @Override
  public InputFile newInputFile(String path) {
    return io.newInputFile(path);
  }

  @Override
  public OutputFile newOutputFile(String path) {
    OutputFile file = io.newOutputFile(path);
    if (file instanceof HadoopOutputFile hfile) {
      // Parquet and ORC write through the FileSystem of a HadoopOutputFile, not its create()
      return HadoopOutputFile.fromPath(
          hfile.getPath(), new TimedFileSystem(hfile.getFileSystem(), timer), hfile.getConf());
    }

    // other FileIOs are not timed
    return file;
  }

  @Override
  public void deleteFile(String path) {
    io.deleteFile(path);
  }

  @Override
  public Map<String, String> properties() {
    return io.properties();
  }

  private static class TimedFileSystem extends FilterFileSystem {
    private final WriteTimer timer;

    private TimedFileSystem(FileSystem fs, WriteTimer timer) {
      super(fs);
      this.timer = timer;
    }

    // Iceberg's Parquet, ORC and Avro writers all reach this overload through FileSystem defaults
    @Override
    public FSDataOutputStream create(
        Path path,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      long start = System.nanoTime();
      FSDataOutputStream stream =
          super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
      timer.recordStorageWrite(System.nanoTime() - start, 0L);
      // DataOutputStream's final writeInt and similar bypass FSDataOutputStream's write methods,
      // so time the stream it wraps
      return new FSDataOutputStream(
          new TimedHadoopStream(stream, timer), null /* stats */, stream.getPos());
    }
  }

  private static class TimedHadoopStream extends OutputStream
      implements Syncable, StreamCapabilities {
    private final FSDataOutputStream stream;
    private final WriteTimer timer;

    private TimedHadoopStream(FSDataOutputStream stream, WriteTimer timer) {
      this.stream = stream;
      this.timer = timer;
    }

    @Override
    public void write(int b) throws IOException {
      long start = System.nanoTime();
      stream.write(b);
      timer.recordStorageWrite(System.nanoTime() - start, 1L);
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
      long start = System.nanoTime();
      stream.write(bytes, off, len);
      timer.recordStorageWrite(System.nanoTime() - start, len);
    }

    @Override
    public void flush() throws IOException {
      long start = System.nanoTime();
      stream.flush();
      timer.recordStorageWrite(System.nanoTime() - start, 0L);
    }

    @Override
    public void hflush() throws IOException {
      long start = System.nanoTime();
      stream.hflush();
      timer.recordStorageWrite(System.nanoTime() - start, 0L);
    }

    @Override
    public void hsync() throws IOException {
      long start = System.nanoTime();
      stream.hsync();
      timer.recordStorageWrite(System.nanoTime() - start, 0L);
    }

    @Override
    public void close() throws IOException {
      long start = System.nanoTime();
      stream.close();
      timer.recordStorageWrite(System.nanoTime() - start, 0L);
      // mirrors HadoopStreams: S3ABlockOutputStream.close() swallows an interrupted upload
      if (Thread.interrupted()
          && "org.apache.hadoop.fs.s3a.S3ABlockOutputStream"
              .equals(stream.getWrappedStream().getClass().getName())) {
        throw new IOException(
            "S3ABlockOutputStream failed to upload object after stream was closed");
      }
    }

    @Override
    public boolean hasCapability(String capability) {
      return stream.hasCapability(capability);
    }
  }
}
