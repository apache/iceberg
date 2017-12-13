/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg.hadoop;

import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.SeekableInputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

/**
 * {@link InputFile} implementation using the Hadoop {@link FileSystem} API.
 * <p>
 * This class is based on Parquet's HadoopInputFile.
 */
public class HadoopInputFile implements InputFile {

  private final FileSystem fs;
  private final FileStatus stat;
  private final Configuration conf;

  public static HadoopInputFile fromLocation(CharSequence location, Configuration conf) {
    Path path = new Path(location.toString());
    return fromPath(path, conf);
  }

  public static HadoopInputFile fromPath(Path path, Configuration conf) {
    try {
      FileSystem fs = path.getFileSystem(conf);
      return new HadoopInputFile(fs, fs.getFileStatus(path), conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file status for path: %s", path);
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

  private HadoopInputFile(FileSystem fs, FileStatus stat, Configuration conf) {
    this.fs = fs;
    this.stat = stat;
    this.conf = conf;
  }

  @Override
  public long getLength() {
    return stat.getLen();
  }

  @Override
  public SeekableInputStream newStream() {
    try {
      return HadoopStreams.wrap(fs.open(stat.getPath()));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to open input stream for file: %s", stat.getPath());
    }
  }

  public Configuration getConf() {
    return conf;
  }

  public FileStatus getStat() {
    return stat;
  }

  @Override
  public String location() {
    return stat.getPath().toString();
  }

  @Override
  public String toString() {
    return location();
  }
}
