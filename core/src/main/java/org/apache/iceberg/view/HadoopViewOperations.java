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
package org.apache.iceberg.view;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/** The Views implementation Based on FileIO. */
public class HadoopViewOperations implements ViewOperations {
  private final Configuration conf;
  private final Path location;
  private ViewMetadata currentMetadata = null;
  private Integer version = null;
  private boolean shouldRefresh = true;
  private HadoopFileIO defaultFileIo = null;

  protected HadoopViewOperations(Path location, Configuration conf) {
    this.conf = conf;
    this.location = location;
  }

  @Override
  public ViewMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  private Path metadataFile(int otherVersion) {
    return metadataPath("v" + otherVersion + ".json");
  }

  @Override
  public ViewMetadata refresh() {
    int ver = version != null ? version : readVersionHint();
    Path metadataFile = metadataFile(ver);
    FileSystem fs = getFS(metadataFile, conf);
    try {
      // don't check if the file exists if version is non-null because it was already checked
      if (version == null && !fs.exists(metadataFile)) {
        if (ver == 0) {
          // no v0 metadata means the table doesn't exist yet
          return null;
        }
        throw new ValidationException("Metadata file is missing: %s", metadataFile);
      }

      while (fs.exists(metadataFile(ver + 1))) {
        ver += 1;
        metadataFile = metadataFile(ver);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", metadataFile);
    }
    this.version = ver;
    this.currentMetadata = ViewMetadataParser.read(io().newInputFile(metadataFile.toString()));
    this.shouldRefresh = false;
    return currentMetadata;
  }

  @Override
  public FileIO io() {
    if (defaultFileIo == null) {
      defaultFileIo = new HadoopFileIO(conf);
    }
    return defaultFileIo;
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata, Map<String, String> properties) {
    if (!base.equals(current())) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (!base.equals(metadata)) {
      return;
    }

    Preconditions.checkArgument(
        base == null || base.location().equals(metadata.location()),
        "Hadoop path-based tables cannot be relocated");
    Preconditions.checkArgument(
        !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
        "Hadoop path-based tables cannot relocate metadata");

    Path tempMetadataFile = metadataPath(UUID.randomUUID() + ".json");
    ViewMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (version != null ? version + 1 : 1);
    Path finalMetadataFile = metadataFile(nextVersion);
    FileSystem fs = getFS(tempMetadataFile, conf);

    try {
      if (fs.exists(finalMetadataFile)) {
        throw new CommitFailedException(
            "Version %d already exists: %s", nextVersion, finalMetadataFile);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(
          e, "Failed to check if next version exists: %s", finalMetadataFile);
    }

    try {
      // this rename operation is the atomic commit operation
      if (!fs.rename(tempMetadataFile, finalMetadataFile)) {
        throw new CommitFailedException(
            "Failed to commit changes using rename: %s", finalMetadataFile);
      }
    } catch (IOException e) {
      throw new CommitFailedException(
          e, "Failed to commit changes using rename: %s", finalMetadataFile);
    }

    // update the best-effort version pointer
    writeVersionHint(nextVersion);

    this.shouldRefresh = true;
  }

  private Path metadataPath(String filename) {
    return new Path(new Path(location, "metadata"), filename);
  }

  private Path versionHintFile() {
    return metadataPath("version-hint.text");
  }

  public static FileSystem getFS(Path path, Configuration conf) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", path);
    }
  }

  private int readVersionHint() {
    Path versionHintFile = versionHintFile();
    try {
      FileSystem fs = getFS(versionHintFile, conf);
      if (!fs.exists(versionHintFile)) {
        return 0;
      }

      try (BufferedReader in =
          new BufferedReader(new InputStreamReader(fs.open(versionHintFile), "UTF-8"))) {
        String versionStr = in.readLine();
        if (versionStr != null) {
          return Integer.parseInt(versionStr.replace("\n", ""));
        }
        return 0;
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", versionHintFile);
    }
  }

  private void writeVersionHint(int otherVersion) {
    Path versionHintFile = versionHintFile();
    FileSystem fs = getFS(versionHintFile, conf);

    try (FSDataOutputStream out = fs.create(versionHintFile, true /* overwrite */)) {
      out.write(String.valueOf(otherVersion).getBytes("UTF-8"));
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write version hint");
    }
  }
}
