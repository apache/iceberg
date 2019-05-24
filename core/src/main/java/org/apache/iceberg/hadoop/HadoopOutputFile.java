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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;

/**
 * {@link OutputFile} implementation using the Hadoop {@link FileSystem} API.
 */
public class HadoopOutputFile implements OutputFile {
  public static OutputFile fromPath(Path path, Configuration conf) {
    return new HadoopOutputFile(path, conf);
  }

  private final Path path;
  private final Configuration conf;

  private HadoopOutputFile(Path path, Configuration conf) {
    this.path = path;
    this.conf = conf;
  }

  @Override
  public PositionOutputStream create() {
    FileSystem fs = Util.getFs(path, conf);
    try {
      return HadoopStreams.wrap(fs.create(path, false /* createOrOverwrite */));
    } catch (FileAlreadyExistsException e) {
      throw new AlreadyExistsException(e, "Path already exists: %s", path);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create file: %s", path);
    }
  }

  @Override
  public PositionOutputStream createOrOverwrite() {
    FileSystem fs = Util.getFs(path, conf);
    try {
      return HadoopStreams.wrap(fs.create(path, true /* createOrOverwrite */));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to create file: %s", path);
    }
  }

  public Path getPath() {
    return path;
  }

  public Configuration getConf() {
    return conf;
  }

  @Override
  public String location() {
    return path.toString();
  }

  @Override
  public InputFile toInputFile() {
    return HadoopInputFile.fromPath(path, conf);
  }

  @Override
  public String toString() {
    return location();
  }
}
