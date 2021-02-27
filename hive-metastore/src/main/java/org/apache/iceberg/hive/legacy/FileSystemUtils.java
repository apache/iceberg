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

package org.apache.iceberg.hive.legacy;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

class FileSystemUtils {

  private FileSystemUtils() {
  }

  /**
   * Lists all non-hidden files for the given directory
   */
  static List<FileStatus> listFiles(String directory, Configuration conf) {

    final Path directoryPath = new Path(directory);
    final FileStatus[] files;
    try {
      FileSystem fs = directoryPath.getFileSystem(conf);
      files = fs.listStatus(directoryPath, HiddenPathFilter.INSTANCE);
    } catch (IOException e) {
      throw new UncheckedIOException("Error listing files for directory: " + directory, e);
    }
    return Arrays.asList(files);
  }

  private enum HiddenPathFilter implements PathFilter {
    INSTANCE;

    @Override
    public boolean accept(Path path) {
      return !path.getName().startsWith("_") && !path.getName().startsWith(".");
    }
  }
}
