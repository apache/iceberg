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

package org.apache.iceberg.hadoop;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;

/**
 * {@link InputFile} implementation using the Hadoop {@link FileSystem} API.
 * <p>
 * This class is based on Parquet's HadoopInputFile.
 */
public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final Path path;
  private final Configuration conf;
  private FileStatus stat = null;
  private Long length = null;

  public static HadoopInputFile fromLocation(CharSequence location, Configuration conf) {
    Path path = new Path(location.toString());
    return fromPath(path, conf);
  }

  public static HadoopInputFile fromLocation(CharSequence location, long length,
                                             Configuration conf) {
    Path path = new Path(location.toString());
    return fromPath(path, length, conf);
  }

  public static HadoopInputFile fromPath(Path path, Configuration conf) {
    try {
      FileSystem fs = path.getFileSystem(conf);
      return new HadoopInputFile(fs, path, conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  public static HadoopInputFile fromPath(Path path, long length, Configuration conf) {
    try {
      FileSystem fs = path.getFileSystem(conf);
      return new HadoopInputFile(fs, path, length, conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf) {
    try {
      FileSystem fs = stat.getPath().getFileSystem(conf);
      return new HadoopInputFile(fs, stat, conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", stat.getPath());
    }
  }

  private HadoopInputFile(FileSystem fs, Path path, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
  }

  private HadoopInputFile(FileSystem fs, Path path, long length, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
    this.length = length;
  }

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.path = stat.getPath();
    this.stat = stat;
    this.conf = conf;
    this.length = stat.getLen();
  }

  private FileStatus lazyStat() {
    if (stat == null) {
      try {
        this.stat = fs.getFileStatus(path);
      } catch (IOException e) {
        throw new RuntimeIOException(e, "Failed to get status for file: %s", path);
      }
    }
    return stat;
  }

  @Override
  public long getLength() {
    if (length == null) {
      this.length = lazyStat().getLen();
    }
    return length;
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return HadoopStreams.wrap(fs.open(path));
    } catch (FileNotFoundException e) {
      throw new NotFoundException(e, "Failed to open input stream for file: %s", path);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to open input stream for file: %s", path);
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public FileStatus getStat() {
    return lazyStat();
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public boolean exists() {
    try {
      return fs.exists(path);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to check existence for file: %s", path);
    }
  }

  @Override
  public String toString() {
    return path.toString();
  }
}
