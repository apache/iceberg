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

package org.apache.iceberg.actions;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.EnumSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A mock FileSystem to simulate the file path will be resolved with format:
 * schema://hostname:port/path when call MockFileSystem.listStatus.
 *
 * File path: /path will be resolved as mock://localhost:9000/path
 * File path: mock:/path will be resolved as mock://localhost:9000/path
 * File path: mock://localhost:9000/path will be resolved as mock://localhost:9000/path
 *
 */
public class MockFileSystem extends RawLocalFileSystem {
  private static final URI NAME = URI.create("mock://localhost:9000");

  private URI convertToLocal(URI uri) {
    if (uri != null && uri.getScheme() != null && uri.getScheme().equals("mock")) {
      try {
        return new URI("file://" + uri.getPath());
      } catch (URISyntaxException e) {
        return uri;
      }
    } else {
      return uri;
    }
  }

  private Path convertToLocal(Path path) {
    return new Path(convertToLocal(path.toUri()));
  }

  public URI convertToMock(URI uri) {
    if (uri == null) {
      return uri;
    }

    if (uri.getScheme() == null || uri.getScheme().equals("file")) {
      try {
        return new URI(NAME + uri.getPath());
      } catch (URISyntaxException e) {
        return uri;
      }
    } else {
      return uri;
    }
  }

  public Path convertToMock(Path path) {
    return new Path(convertToMock(path.toUri()));
  }

  public FileStatus convertToMock(FileStatus status) {
    try {
      FileStatus newStatus = new FileStatus(status);
      newStatus.setPath(convertToMock(newStatus.getPath()));
      if (newStatus.isSymlink()) {
        newStatus.setSymlink(convertToMock(newStatus.getSymlink()));
      }

      return newStatus;
    } catch (IOException e) {
      return status;
    }
  }

  @Override
  public File pathToFile(Path path) {
    return super.pathToFile(convertToLocal(path));
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(convertToLocal(name), conf);
  }

  @Override
  public String getScheme() {
    return "mock";
  }

  @Override
  public Path makeQualified(Path path) {
    Path qualified = super.makeQualified(convertToLocal(path));
    return convertToMock(qualified);
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    return super.open(convertToLocal(f), bufferSize);
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return super.append(convertToLocal(f), bufferSize, progress);
  }

  @Override
  public FSDataOutputStream create(
      Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize,
      Progressable progress) throws IOException {
    return super.create(convertToLocal(f), permission, overwrite, bufferSize, replication, blockSize, progress);
  }

  @Override
  protected OutputStream createOutputStreamWithMode(
      Path f, boolean append, FsPermission permission) throws IOException {
    return super.createOutputStreamWithMode(convertToLocal(f), append, permission);
  }

  @Override
  public FSDataOutputStream createNonRecursive(
      Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication,
      long blockSize, Progressable progress) throws IOException {
    return super.createNonRecursive(convertToLocal(f), permission, flags, bufferSize, replication, blockSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return super.rename(convertToLocal(src), convertToLocal(dst));
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    return super.truncate(convertToLocal(f), newLength);
  }

  @Override
  public boolean delete(Path p, boolean recursive) throws IOException {
    return super.delete(convertToLocal(p), recursive);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    FileStatus[] status = super.listStatus(convertToLocal(f));
    return Arrays.stream(status).map(this::convertToMock).toArray(FileStatus[]::new);
  }

  @Override
  public boolean mkdirs(Path f) throws IOException {
    return super.mkdirs(convertToLocal(f));
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return super.mkdirs(convertToLocal(f), permission);
  }

  @Override
  public FsStatus getStatus(Path p) throws IOException {
    return super.getStatus(convertToLocal(p));
  }

  @Override
  public void moveFromLocalFile(Path src, Path dst) throws IOException {
    super.moveFromLocalFile(convertToLocal(src), convertToLocal(dst));
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    FileStatus status = super.getFileStatus(convertToLocal(f));
    return convertToMock(status);
  }

  @Override
  public void createSymlink(Path target, Path link, boolean createParent) throws IOException {
    super.createSymlink(convertToLocal(target), link, createParent);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    FileStatus status = super.getFileStatus(convertToLocal(f));
    convertToMock(status);
    return status;
  }
}
