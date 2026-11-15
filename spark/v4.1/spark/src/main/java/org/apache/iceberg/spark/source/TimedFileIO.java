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
import org.apache.iceberg.io.PositionOutputStream;

/** A {@link FileIO} that records the time output files spend in storage calls. */
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

    return new TimedOutputFile(file, timer);
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
      try {
        FSDataOutputStream stream =
            super.create(path, permission, overwrite, bufferSize, replication, blockSize, progress);
        // DataOutputStream's final writeInt and similar bypass FSDataOutputStream's write methods,
        // so time the stream it wraps
        return new FSDataOutputStream(
            new TimedHadoopStream(stream, timer), null /* stats */, stream.getPos());
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
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
      try {
        stream.write(b);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 1L);
      }
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
      long start = System.nanoTime();
      try {
        stream.write(bytes, off, len);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, len);
      }
    }

    @Override
    public void flush() throws IOException {
      long start = System.nanoTime();
      try {
        stream.flush();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public void hflush() throws IOException {
      long start = System.nanoTime();
      try {
        stream.hflush();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public void hsync() throws IOException {
      long start = System.nanoTime();
      try {
        stream.hsync();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public void close() throws IOException {
      long start = System.nanoTime();
      try {
        stream.close();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public boolean hasCapability(String capability) {
      return stream.hasCapability(capability);
    }
  }

  private static class TimedOutputFile implements OutputFile {
    private final OutputFile file;
    private final WriteTimer timer;

    private TimedOutputFile(OutputFile file, WriteTimer timer) {
      this.file = file;
      this.timer = timer;
    }

    @Override
    public PositionOutputStream create() {
      long start = System.nanoTime();
      try {
        return new TimedPositionOutputStream(file.create(), timer);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      long start = System.nanoTime();
      try {
        return new TimedPositionOutputStream(file.createOrOverwrite(), timer);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public String location() {
      return file.location();
    }

    @Override
    public InputFile toInputFile() {
      return file.toInputFile();
    }

    @Override
    public String toString() {
      return file.toString();
    }
  }

  // must not implement DelegatingOutputStream, or Parquet would unwrap it and bypass the timer
  private static class TimedPositionOutputStream extends PositionOutputStream {
    private final PositionOutputStream stream;
    private final WriteTimer timer;

    private TimedPositionOutputStream(PositionOutputStream stream, WriteTimer timer) {
      this.stream = stream;
      this.timer = timer;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getPos();
    }

    @Override
    public long storedLength() throws IOException {
      return stream.storedLength();
    }

    @Override
    public void write(int b) throws IOException {
      long start = System.nanoTime();
      try {
        stream.write(b);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 1L);
      }
    }

    @Override
    public void write(byte[] bytes, int off, int len) throws IOException {
      long start = System.nanoTime();
      try {
        stream.write(bytes, off, len);
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, len);
      }
    }

    @Override
    public void flush() throws IOException {
      long start = System.nanoTime();
      try {
        stream.flush();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }

    @Override
    public void close() throws IOException {
      long start = System.nanoTime();
      try {
        stream.close();
      } finally {
        timer.recordStorageWrite(System.nanoTime() - start, 0L);
      }
    }
  }
}
