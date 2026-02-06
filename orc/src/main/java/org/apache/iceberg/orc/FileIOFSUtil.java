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
package org.apache.iceberg.orc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.hadoop.HadoopStreams;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

class FileIOFSUtil {
  private FileIOFSUtil() {}

  private static class NullFileSystem extends FileSystem {

    @Override
    public URI getUri() {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
        throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean rename(Path src, Path dst) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete(Path f, boolean recursive) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus[] listStatus(Path f) throws FileNotFoundException, IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void setWorkingDirectory(Path new_dir) {
      throw new UnsupportedOperationException();
    }

    @Override
    public Path getWorkingDirectory() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public FileStatus getFileStatus(Path f) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  static class InputFileSystem extends NullFileSystem {
    private final InputFile inputFile;
    private final Path inputPath;

    InputFileSystem(InputFile inputFile) {
      this.inputFile = inputFile;
      this.inputPath = new Path(inputFile.location());
    }

    @Override
    public FSDataInputStream open(Path f) throws IOException {
      Preconditions.checkArgument(
          f.equals(inputPath), String.format("Input %s does not equal expected %s", f, inputPath));
      return new FSDataInputStream(HadoopStreams.wrap(inputFile.newStream()));
    }

    @Override
    public FSDataInputStream open(Path f, int bufferSize) throws IOException {
      return open(f);
    }
  }

  static class OutputFileSystem extends NullFileSystem {
    private final OutputFile outputFile;
    private final Path outPath;

    OutputFileSystem(OutputFile outputFile) {
      this.outputFile = outputFile;
      this.outPath = new Path(outputFile.location());
    }

    @Override
    public FSDataOutputStream create(Path f, boolean overwrite) throws IOException {
      Preconditions.checkArgument(
          f.equals(outPath), String.format("Input %s does not equal expected %s", f, outPath));
      OutputStream outputStream = overwrite ? outputFile.createOrOverwrite() : outputFile.create();
      return new FSDataOutputStream(outputStream, null);
    }

    @Override
    public FSDataOutputStream create(
        Path f,
        FsPermission permission,
        boolean overwrite,
        int bufferSize,
        short replication,
        long blockSize,
        Progressable progress)
        throws IOException {
      return create(f, overwrite);
    }
  }
}
