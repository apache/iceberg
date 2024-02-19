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
package org.apache.iceberg;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;

public class Files {

  private Files() {}

  public static OutputFile localOutput(File file) {
    return new LocalOutputFile(file);
  }

  public static OutputFile localOutput(String file) {
    return localOutput(Paths.get(file).toAbsolutePath().toFile());
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

      if (!file.getParentFile().isDirectory() && !file.getParentFile().mkdirs()) {
        throw new RuntimeIOException(
            "Failed to create the file's directory at %s.", file.getParentFile().getAbsolutePath());
      }

      try {
        return new PositionFileOutputStream(file, new RandomAccessFile(file, "rw"));
      } catch (FileNotFoundException e) {
        throw new NotFoundException(e, "Failed to create file: %s", file);
      }
    }

    @Override
    public PositionOutputStream createOrOverwrite() {
      if (file.exists()) {
        if (!file.delete()) {
          throw new RuntimeIOException("Failed to delete: %s", file);
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
    return new LocalInputFile(file, -1L);
  }

  public static InputFile localInput(File file, long length) {
    return new LocalInputFile(file, length);
  }

  public static InputFile localInput(String file) {
    return localInput(file, -1L);
  }

  public static InputFile localInput(String file, long length) {
    if (file.startsWith("file:")) {
      return localInput(new File(file.replaceFirst("file:", "")), length);
    }
    return localInput(new File(file), length);
  }

  private static class LocalInputFile implements InputFile {
    private final File file;
    private final long fileLength;

    private LocalInputFile(File file, long length) {
      this.file = file;
      this.fileLength = length;
    }

    @Override
    public long getLength() {
      if (fileLength >= 0) {
        return fileLength;
      } else {
        return file.length();
      }
    }

    @Override
    public SeekableInputStream newStream() {
      try {
        return new SeekableFileInputStream(new RandomAccessFile(file, "r"));
      } catch (FileNotFoundException e) {
        throw new NotFoundException(e, "Failed to read file: %s", file);
      }
    }

    @Override
    public String location() {
      return file.toString();
    }

    @Override
    public boolean exists() {
      return file.exists();
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
    private final File file;
    private final RandomAccessFile stream;
    private boolean isClosed = false;

    private PositionFileOutputStream(File file, RandomAccessFile stream) {
      this.file = file;
      this.stream = stream;
    }

    @Override
    public long getPos() throws IOException {
      if (isClosed) {
        return file.length();
      }
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
    public void write(int b) throws IOException {
      stream.write(b);
    }

    @Override
    public void close() throws IOException {
      stream.close();
      this.isClosed = true;
    }
  }
}
