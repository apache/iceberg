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

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableOperations implementation for file systems that support atomic rename.
 * <p>
 * This maintains metadata in a "metadata" folder under the table location.
 */
public class HadoopTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);

  private final Configuration conf;
  private final Path location;
  private HadoopFileIO defaultFileIo = null;

  private volatile TableMetadata currentMetadata = null;
  private volatile Integer version = null;
  private volatile boolean shouldRefresh = true;

  protected HadoopTableOperations(Path location, Configuration conf) {
    this.conf = conf;
    this.location = location;
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  private synchronized Pair<Integer, TableMetadata> versionAndMetadata() {
    return Pair.of(version, currentMetadata);
  }

  private synchronized void updateVersionAndMetadata(int newVersion, String metadataFile) {
    // update if the current version is out of date
    if (version == null || version != newVersion) {
      this.version = newVersion;
      this.currentMetadata = checkUUID(currentMetadata, TableMetadataParser.read(io(), metadataFile));
    }
  }

  @Override
  public TableMetadata refresh() {
    int ver = version != null ? version : readVersionHint();
    try {
      Path metadataFile = getMetadataFile(ver);
      if (version == null && metadataFile == null && ver == 0) {
        // no v0 metadata means the table doesn't exist yet
        return null;
      } else if (metadataFile == null) {
        throw new ValidationException("Metadata file for version %d is missing", ver);
      }

      Path nextMetadataFile = getMetadataFile(ver + 1);
      while (nextMetadataFile != null) {
        ver += 1;
        metadataFile = nextMetadataFile;
        nextMetadataFile = getMetadataFile(ver + 1);
      }

      updateVersionAndMetadata(ver, metadataFile.toString());

      this.shouldRefresh = false;
      return currentMetadata;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to refresh the table");
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Pair<Integer, TableMetadata> current = versionAndMetadata();
    if (base != current.second()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    Preconditions.checkArgument(base == null || base.location().equals(metadata.location()),
        "Hadoop path-based tables cannot be relocated");
    Preconditions.checkArgument(
        !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
        "Hadoop path-based tables cannot relocate metadata");

    String codecName = metadata.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
    String fileExtension = TableMetadataParser.getFileExtension(codec);
    Path tempMetadataFile = metadataPath(UUID.randomUUID().toString() + fileExtension);
    TableMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (current.first() != null ? current.first() : 0) + 1;
    Path finalMetadataFile = metadataFilePath(nextVersion, codec);
    FileSystem fs = getFileSystem(tempMetadataFile, conf);

    try {
      if (fs.exists(finalMetadataFile)) {
        throw new CommitFailedException(
            "Version %d already exists: %s", nextVersion, finalMetadataFile);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e,
          "Failed to check if next version exists: " + finalMetadataFile);
    }

    // this rename operation is the atomic commit operation
    renameToFinal(fs, tempMetadataFile, finalMetadataFile);

    // update the best-effort version pointer
    writeVersionHint(nextVersion);

    deleteRemovedMetadataFiles(base, metadata);

    this.shouldRefresh = true;
  }

  @Override
  public FileIO io() {
    if (defaultFileIo == null) {
      defaultFileIo = new HadoopFileIO(conf);
    }
    return defaultFileIo;
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public String metadataFileLocation(String fileName) {
    return metadataPath(fileName).toString();
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException("Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return HadoopTableOperations.this.metadataFileLocation(fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return HadoopTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return HadoopTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return HadoopTableOperations.this.newSnapshotId();
      }
    };
  }

  private Path getMetadataFile(int metadataVersion) throws IOException {
    for (TableMetadataParser.Codec codec : TableMetadataParser.Codec.values()) {
      Path metadataFile = metadataFilePath(metadataVersion, codec);
      FileSystem fs = getFileSystem(metadataFile, conf);
      if (fs.exists(metadataFile)) {
        return metadataFile;
      }

      if (codec.equals(TableMetadataParser.Codec.GZIP)) {
        // we have to be backward-compatible with .metadata.json.gz files
        metadataFile = oldMetadataFilePath(metadataVersion, codec);
        fs = getFileSystem(metadataFile, conf);
        if (fs.exists(metadataFile)) {
          return metadataFile;
        }
      }
    }

    return null;
  }

  private Path metadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getFileExtension(codec));
  }

  private Path oldMetadataFilePath(int metadataVersion, TableMetadataParser.Codec codec) {
    return metadataPath("v" + metadataVersion + TableMetadataParser.getOldFileExtension(codec));
  }

  private Path metadataPath(String filename) {
    return new Path(new Path(location, "metadata"), filename);
  }

  private Path versionHintFile() {
    return metadataPath("version-hint.text");
  }

  private void writeVersionHint(int versionToWrite) {
    Path versionHintFile = versionHintFile();
    FileSystem fs = getFileSystem(versionHintFile, conf);

    try (FSDataOutputStream out = fs.create(versionHintFile, true /* overwrite */)) {
      out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      LOG.warn("Failed to update version hint", e);
    }
  }

  private int readVersionHint() {
    Path versionHintFile = versionHintFile();
    try {
      FileSystem fs = Util.getFs(versionHintFile, conf);
      if (!fs.exists(versionHintFile)) {
        return 0;
      }

      try (InputStreamReader fsr = new InputStreamReader(fs.open(versionHintFile), StandardCharsets.UTF_8);
           BufferedReader in = new BufferedReader(fsr)) {
        return Integer.parseInt(in.readLine().replace("\n", ""));
      }

    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to get file system for path: %s", versionHintFile);
    }
  }

  /**
   * Renames the source file to destination, using the provided file system. If the rename failed,
   * an attempt will be made to delete the source file.
   *
   * @param fs the filesystem used for the rename
   * @param src the source file
   * @param dst the destination file
   */
  private void renameToFinal(FileSystem fs, Path src, Path dst) {
    try {
      if (!fs.rename(src, dst)) {
        CommitFailedException cfe = new CommitFailedException(
            "Failed to commit changes using rename: %s", dst);
        RuntimeException re = tryDelete(src);
        if (re != null) {
          cfe.addSuppressed(re);
        }
        throw cfe;
      }
    } catch (IOException e) {
      CommitFailedException cfe = new CommitFailedException(e,
          "Failed to commit changes using rename: %s", dst);
      RuntimeException re = tryDelete(src);
      if (re != null) {
        cfe.addSuppressed(re);
      }
      throw cfe;
    }
  }

  /**
   * Deletes the file from the file system. Any RuntimeException will be caught and returned.
   *
   * @param path the file to be deleted.
   * @return RuntimeException caught, if any. null otherwise.
   */
  private RuntimeException tryDelete(Path path) {
    try {
      io().deleteFile(path.toString());
      return null;
    } catch (RuntimeException re) {
      return re;
    }
  }

  protected FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    return Util.getFs(path, hadoopConf);
  }

  /**
   * Deletes the oldest metadata files if {@link TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base     table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit = metadata.propertyAsBoolean(
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
        TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles = Sets.newHashSet(base.previousFiles());
    removedPreviousMetadataFiles.removeAll(metadata.previousFiles());

    if (deleteAfterCommit) {
      Tasks.foreach(removedPreviousMetadataFiles)
          .noRetry().suppressFailureWhenFinished()
          .onFailure((previousMetadataFile, exc) ->
              LOG.warn("Delete failed for previous metadata file: {}", previousMetadataFile, exc))
          .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }

  private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata) {
    String newUUID = newMetadata.uuid();
    if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
      Preconditions.checkState(newUUID.equals(currentMetadata.uuid()),
          "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
    }
    return newMetadata;
  }
}
