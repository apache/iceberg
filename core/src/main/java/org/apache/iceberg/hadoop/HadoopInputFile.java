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
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * {@link InputFile} implementation using the Hadoop {@link FileSystem} API.
 * <p>
 * This class is based on Parquet's HadoopInputFile.
 */
public class HadoopInputFile implements InputFile {
  public static final String[] NO_LOCATION_PREFERENCE = new String[0];

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

  public static HadoopInputFile fromLocation(CharSequence location, FileSystem fs) {
    Path path = new Path(location.toString());
    return fromPath(path, fs);
  }

  public static HadoopInputFile fromLocation(CharSequence location, long length,
                                             FileSystem fs) {
    Path path = new Path(location.toString());
    return fromPath(path, length, fs);
  }

  public static HadoopInputFile fromPath(Path path, Configuration conf) {
    FileSystem fs = Util.getFs(path, conf);
    return fromPath(path, fs, conf);
  }

  public static HadoopInputFile fromPath(Path path, long length, Configuration conf) {
    FileSystem fs = Util.getFs(path, conf);
    return fromPath(path, length, fs, conf);
  }

  public static HadoopInputFile fromPath(Path path, FileSystem fs) {
    return fromPath(path, fs, fs.getConf());
  }

  public static HadoopInputFile fromPath(Path path, long length, FileSystem fs) {
    return fromPath(path, length, fs, fs.getConf());
  }

  public static HadoopInputFile fromPath(Path path, FileSystem fs, Configuration conf) {
    return new HadoopInputFile(fs, path, conf);
  }

  public static HadoopInputFile fromPath(Path path, long length, FileSystem fs, Configuration conf) {
    return new HadoopInputFile(fs, path, length, conf);
  }

  public static HadoopInputFile fromStatus(FileStatus stat, Configuration conf) {
    FileSystem fs = Util.getFs(stat.getPath(), conf);
    return fromStatus(stat, fs, conf);
  }

  public static HadoopInputFile fromStatus(FileStatus stat, FileSystem fs) {
    return fromStatus(stat, fs, fs.getConf());
  }

  public static HadoopInputFile fromStatus(FileStatus stat, FileSystem fs, Configuration conf) {
    return new HadoopInputFile(fs, stat, conf);
  }

  private HadoopInputFile(FileSystem fs, Path path, Configuration conf) {
    this.fs = fs;
    this.path = path;
    this.conf = conf;
  }

  private HadoopInputFile(FileSystem fs, Path path, long length, Configuration conf) {
    Preconditions.checkArgument(length >= 0, "Invalid file length: %s", length);
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

  public FileSystem getFileSystem() {
    return fs;
  }

  public FileStatus getStat() {
    return lazyStat();
  }

  public Path getPath() {
    return path;
  }

  public String[] getBlockLocations(long start, long end) {
    List<String> hosts = Lists.newArrayList();
    try {
      for (BlockLocation location : fs.getFileBlockLocations(path, start, end)) {
        Collections.addAll(hosts, location.getHosts());
      }

      return hosts.toArray(NO_LOCATION_PREFERENCE);

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get block locations for path: %s", path);
    }
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
