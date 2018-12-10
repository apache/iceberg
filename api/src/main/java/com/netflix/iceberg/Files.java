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

package com.netflix.iceberg;

import com.netflix.iceberg.exceptions.AlreadyExistsException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.io.PositionOutputStream;
import com.netflix.iceberg.io.SeekableInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Files {

  public static OutputFile localOutput(File file) {
    return new LocalOutputFile(file);
  }

  public static OutputFile localOutput(String file) {
    return localOutput(new File(file));
  }

  private static class LocalOutputFile implements OutputFile {
    private final File file;

    private LocalOutputFile(File file) {
      this.file = file;
    }

    @Override
    public PositionOutputStream create() {
      if (file.exists()) {
        throw new AlreadyExistsException("File already exists: %s", file);
      }

      try {
        return new PositionFileOutputStream(new RandomAccessFile(file, "rw"));
      } catch (FileNotFoundException e) {
        throw new RuntimeIOException(e, "Failed to create file: %s", file);
      }
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      if (file.exists()) {
        if (!file.delete()) {
          throw new RuntimeIOException("Failed to delete: " + file);
        }
      }
      return create();
    }

    @Override
    public String location() {
      return file.toString();
    }

    @Override
    public InputFile toInputFile() {
      return localInput(file);
    }

    @Override
    public String toString() {
      return location();
    }
  }

  public static InputFile localInput(File file) {
    return new LocalInputFile(file);
  }

  public static InputFile localInput(String file) {
    return localInput(new File(file));
  }

  private static class LocalInputFile implements InputFile {
    private final File file;

    private LocalInputFile(File file) {
      this.file = file;
    }

    @Override
    public long getLength() {
      return file.length();
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        return new SeekableFileInputStream(new RandomAccessFile(file, "r"));
      } catch (FileNotFoundException e) {
        throw new RuntimeIOException(e, "Failed to read file: %s", file);
      }
    }

    @Override
    public String location() {
      return file.toString();
    }

    @Override
    public String toString() {
      return location();
    }
  }

  private static class SeekableFileInputStream extends SeekableInputStream {
    private final RandomAccessFile stream;

    private SeekableFileInputStream(RandomAccessFile stream) {
      this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getFilePointer();
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
    public int read(byte[] b) throws IOException {
      return stream.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      return stream.read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
      if (n > Integer.MAX_VALUE) {
        return stream.skipBytes(Integer.MAX_VALUE);
      } else {
        return stream.skipBytes((int) n);
      }
    }

    @Override
    public void close() throws IOException {
      stream.close();
    }
  }

  private static class PositionFileOutputStream extends PositionOutputStream {
    private final RandomAccessFile stream;

    private PositionFileOutputStream(RandomAccessFile stream) {
      this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
      return stream.getFilePointer();
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
    public void close() throws IOException {
      stream.close();
    }

    @Override
    public void write(int b) throws IOException {
      stream.write(b);
    }
  }
}
