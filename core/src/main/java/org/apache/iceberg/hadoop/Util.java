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
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTask;
import org.apache.iceberg.ScanTaskGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.ResolvingFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Util {

  public static final String VERSION_HINT_FILENAME = "version-hint.text";

  private static final Set<String> LOCALITY_WHITELIST_FS = ImmutableSet.of("hdfs");

  private static final Logger LOG = LoggerFactory.getLogger(Util.class);

  private Util() {}

  public static FileSystem getFs(Path path, Configuration conf) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException e) {
      throw new UncheckedIOException(
          String.format("Failed to get file system for path: %s", path), e);
    }
  }

  public static String[] blockLocations(CombinedScanTask task, Configuration conf) {
    Set<String> locationSets = Sets.newHashSet();
    for (FileScanTask f : task.files()) {
      Path path = new Path(f.file().path().toString());
      try {
        FileSystem fs = path.getFileSystem(conf);
        for (BlockLocation b : fs.getFileBlockLocations(path, f.start(), f.length())) {
          locationSets.addAll(Arrays.asList(b.getHosts()));
        }
      } catch (IOException ioe) {
        LOG.warn("Failed to get block locations for path {}", path, ioe);
      }
    }

    return locationSets.toArray(new String[0]);
  }

  public static String[] blockLocations(FileIO io, ScanTaskGroup<?> taskGroup) {
    Set<String> locations = Sets.newHashSet();

    for (ScanTask task : taskGroup.tasks()) {
      if (task instanceof ContentScanTask) {
        Collections.addAll(locations, blockLocations(io, (ContentScanTask<?>) task));
      }
    }

    return locations.toArray(HadoopInputFile.NO_LOCATION_PREFERENCE);
  }

  public static boolean mayHaveBlockLocations(FileIO io, String location) {
    if (usesHadoopFileIO(io, location)) {
      InputFile inputFile = io.newInputFile(location);
      if (inputFile instanceof HadoopInputFile) {
        String scheme = ((HadoopInputFile) inputFile).getFileSystem().getScheme();
        return LOCALITY_WHITELIST_FS.contains(scheme);

      } else {
        return false;
      }
    }

    return false;
  }

  private static String[] blockLocations(FileIO io, ContentScanTask<?> task) {
    String location = task.file().path().toString();
    if (usesHadoopFileIO(io, location)) {
      InputFile inputFile = io.newInputFile(location);
      if (inputFile instanceof HadoopInputFile) {
        return ((HadoopInputFile) inputFile).getBlockLocations(task.start(), task.length());

      } else {
        return HadoopInputFile.NO_LOCATION_PREFERENCE;
      }
    } else {
      return HadoopInputFile.NO_LOCATION_PREFERENCE;
    }
  }

  private static boolean usesHadoopFileIO(FileIO io, String location) {
    if (io instanceof HadoopFileIO) {
      return true;

    } else if (io instanceof ResolvingFileIO) {
      ResolvingFileIO resolvingFileIO = (ResolvingFileIO) io;
      return HadoopFileIO.class.isAssignableFrom(resolvingFileIO.ioClass(location));

    } else {
      return false;
    }
  }

  /**
   * From Apache Spark
   *
   * <p>Convert URI to String. Since URI.toString does not decode the uri, e.g. change '%25' to '%'.
   * Here we create a hadoop Path with the given URI, and rely on Path.toString to decode the uri
   *
   * @param uri the URI of the path
   * @return the String of the path
   */
  public static String uriToString(URI uri) {
    return new Path(uri).toString();
  }
}
