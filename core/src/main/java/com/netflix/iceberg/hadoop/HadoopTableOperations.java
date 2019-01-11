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

package com.netflix.iceberg.hadoop;

import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableMetadataParser;
import com.netflix.iceberg.TableOperations;
import com.netflix.iceberg.exceptions.CommitFailedException;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.exceptions.ValidationException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.UUID;

import static com.netflix.iceberg.TableMetadataParser.getFileExtension;

/**
 * TableOperations implementation for file systems that support atomic rename.
 * <p>
 * This maintains metadata in a "metadata" folder under the table location.
 */
public class HadoopTableOperations implements TableOperations {
  private static final String VERSION_HINT_FILE_NAME = "version-hint.text";
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);

  private final Configuration conf;
  private final Path location;
  private TableMetadata currentMetadata = null;
  private Integer version = null;
  private boolean shouldRefresh = true;
  private HadoopFileIO defaultFileIo = null;

  protected HadoopTableOperations(Path location, Configuration conf) {
    this.conf = conf;
    this.location = location;
  }

  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  @Override
  public TableMetadata refresh() {
    int ver = version != null ? version : readVersionHint();
    InputFile metadataFile = io().readMetadataFile(metadataFileName(ver));
    Path metadataFilePath = new Path(metadataFile.location());
    FileSystem fs = getFS(metadataFilePath, conf);
    try {
      // don't check if the file exists if version is non-null because it was already checked
      if (version == null && !fs.exists(metadataFilePath)) {
        if (ver == 0) {
          // no v0 metadata means the table doesn't exist yet
          return null;
        }
        throw new ValidationException("Metadata file is missing: %s", metadataFile);
      }

      while (fs.exists(new Path(io().readMetadataFile(metadataFileName(ver + 1)).location()))) {
        ver += 1;
        metadataFile = io().readMetadataFile(metadataFileName(ver));
      }

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", metadataFile);
    }
    this.version = ver;
    this.currentMetadata = TableMetadataParser.read(this, metadataFile);
    this.shouldRefresh = false;
    if (defaultFileIo != null) {
      defaultFileIo.updateProperties(currentMetadata.properties());
    }
    return currentMetadata;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    if (base != current()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    OutputFile newTempMetadataFile = io().newMetadataFile(
        UUID.randomUUID().toString() + getFileExtension(conf));
    TableMetadataParser.write(metadata, newTempMetadataFile);

    int nextVersion = (version != null ? version : 0) + 1;
    Path tempMetadataFile = new Path(newTempMetadataFile.location());
    Path finalMetadataFile = new Path(io().newMetadataFile(
        metadataFileName(nextVersion)).location());
    FileSystem fs = getFS(tempMetadataFile, conf);

    try {
      if (fs.exists(finalMetadataFile)) {
        throw new CommitFailedException(
            "Version %d already exists: %s", nextVersion, finalMetadataFile);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e,
          "Failed to check if next version exists: " + finalMetadataFile);
    }

    try {
      // this rename operation is the atomic commit operation
      if (!fs.rename(tempMetadataFile, finalMetadataFile)) {
        throw new CommitFailedException(
            "Failed to commit changes using rename: %s", finalMetadataFile);
      }
    } catch (IOException e) {
      throw new CommitFailedException(e,
          "Failed to commit changes using rename: %s", finalMetadataFile);
    }

    // update the best-effort version pointer
    writeVersionHint(nextVersion);

    this.shouldRefresh = true;
  }

  private String metadataFileName(int metadataVersion) {
    return "v" + metadataVersion + getFileExtension(conf);
  }

  @Override
  public FileIO io() {
    if (defaultFileIo == null) {
      defaultFileIo = new HadoopFileIO(
          conf,
          location.toString(),
          currentMetadata == null ? ImmutableMap.of() : currentMetadata.properties());
    }
    return defaultFileIo;
  }

  private void writeVersionHint(int version) {
    try (OutputStream out = io().newMetadataFile(VERSION_HINT_FILE_NAME).createOrOverwrite()) {
      out.write(String.valueOf(version).getBytes("UTF-8"));

    } catch (IOException e) {
      LOG.warn("Failed to update version hint", e);
    }
  }

  private int readVersionHint() {
    InputFile versionHintFile = io().readMetadataFile(VERSION_HINT_FILE_NAME);
    try {
      Path versionHintFilePath = new Path(versionHintFile.location());
      FileSystem fs = Util.getFS(versionHintFilePath, conf);
      if (!fs.exists(versionHintFilePath)) {
        return 0;
      }

      try (BufferedReader in = new BufferedReader(new InputStreamReader(versionHintFile.newStream()))) {
        return Integer.parseInt(in.readLine().replace("\n", ""));
      }

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", versionHintFile.location());
    }
  }

  protected FileSystem getFS(Path path, Configuration conf) {
    return Util.getFS(path, conf);
  }
}
