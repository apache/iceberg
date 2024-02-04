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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TableOperations implementation for file systems that support atomic rename.
 *
 * <p>This maintains metadata in a "metadata" folder under the table location.
 */
public class HadoopTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(HadoopTableOperations.class);
  private static final Pattern VERSION_PATTERN = Pattern.compile("v([^.]*)\\..*");

  private final Configuration conf;
  private final Path location;
  private final FileIO fileIO;
  private final LockManager lockManager;

  private volatile TableMetadata currentMetadata = null;
  private volatile Integer version = null;
  private volatile boolean shouldRefresh = true;
  private volatile boolean firstRun;

  protected HadoopTableOperations(
      Path location, FileIO fileIO, Configuration conf, LockManager lockManager) {
    this.conf = conf;
    this.location = location;
    this.fileIO = fileIO;
    this.lockManager = lockManager;
    this.firstRun = true;
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
      this.currentMetadata =
          checkUUID(currentMetadata, TableMetadataParser.read(io(), metadataFile));
    }
  }

  @Override
  public TableMetadata refresh() {
    int ver = version != null ? version : findVersion();
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
    // findOldVersionHint-------------------->isFirstRun------->JobFail
    //       |                 NOT EXISTS         |        NO
    //       | yes                                |
    //       |                                    |
    //       ↓                   NO               |
    // checkNextVersionIsLatest------>JobFail     |
    //       |                                    |
    //       | yes                                |
    //       ↓              NO                    |YES
    // dropOldVersionHint----------->JobFail      |
    //       |                                    |
    //       |<-----------------------------------|
    //       | yes
    //       ↓
    // writeNewVersionMeta-------------------->JobFail
    //       |                 NO
    //       |
    //       | yes
    //       ↓
    // writeNewVersionHint--------------->|
    //       |                  NO        |
    //       |                            |
    //       | yes                        |
    //       ↓                            |
    // cleanOldMeta---------------------->|
    //       |                NO          |
    //       |                            |
    //       | yes                        |
    //       ↓                            |
    //    SUCCESS<------------------------|
    Pair<Integer, TableMetadata> current = versionAndMetadata();
    if (base != current.second()) {
      throw new CommitFailedException("Cannot commit changes based on stale table metadata");
    }

    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    Preconditions.checkArgument(
        base == null || base.location().equals(metadata.location()),
        "Hadoop path-based tables cannot be relocated");
    Preconditions.checkArgument(
        !metadata.properties().containsKey(TableProperties.WRITE_METADATA_LOCATION),
        "Hadoop path-based tables cannot relocate metadata");

    String codecName =
        metadata.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    // TODO:This is not compatible with the scenario where the user modifies the metadata file
    // compression codec arbitrarily.
    // We can inform the user about this bug first, and fix it later.(Do not modify the compressed
    // format after the table is created.)
    TableMetadataParser.Codec codec = TableMetadataParser.Codec.fromName(codecName);
    String fileExtension = TableMetadataParser.getFileExtension(codec);
    Path tempMetadataFile = metadataPath(UUID.randomUUID() + fileExtension);
    TableMetadataParser.write(metadata, io().newOutputFile(tempMetadataFile.toString()));

    int nextVersion = (current.first() != null ? current.first() : 0) + 1;
    Path finalMetadataFile = metadataFilePath(nextVersion, codec);
    FileSystem fs = getFileSystem(tempMetadataFile, conf);
    boolean versionCommitSuccess = false;
    try {
      deleteOldVersionHint(fs, versionHintFile(), nextVersion);
      // This renames operation is the atomic commit operation.
      // Since fs.rename() cannot overwrite existing files, in case of concurrent operations, only
      // one client will execute renameToFinal() successfully.
      versionCommitSuccess = commitNewVersion(fs, tempMetadataFile, finalMetadataFile, nextVersion);
      if (!versionCommitSuccess) {
        // Users should clean up orphaned files after job fail.This may be too heavy.
        // But it can stay that way for now.
        String msg =
            String.format(
                "Can not write versionHint. commitVersion = %s.Is there a problem with the file system?",
                nextVersion);
        throw new RuntimeException(msg);
      } else {
        this.shouldRefresh = versionCommitSuccess;
        // In fact, we don't really care if the metadata cleanup/update succeeds or not,
        // if it fails this time, we can execute it in the next commit method call.
        // So we should fix the shouldRefresh flag first.
        if (this.firstRun) {
          this.firstRun = false;
        }
        LOG.info("Committed a new metadata file {}", finalMetadataFile);
        // update the best-effort version pointer
        boolean writeVersionHintSuccess = writeVersionHint(nextVersion);
        if (!writeVersionHintSuccess) {
          LOG.warn(
              "Failed to write a new versionHintFile,commit version is [{}], is there a problem with the file system?",
              nextVersion);
        }
        deleteRemovedMetadataFiles(base, metadata);
      }
    } catch (CommitStateUnknownException | CommitFailedException e) {
      // These exceptions are thrown under our manual control.
      // When these two exceptions are thrown, the metadata should not be submitted successfully.
      this.shouldRefresh = versionCommitSuccess;
      throw e;
    } catch (Throwable e) {
      // If new version metadata wrote, we consider the commit to have actually succeeded.
      // We'll swallow all the exception.
      // Otherwise, we will throw a CommitFailedException.
      this.shouldRefresh = versionCommitSuccess;
      if (versionCommitSuccess) {
        if (this.firstRun) {
          this.firstRun = false;
        }
        LOG.warn(
            "The commit actually successfully, "
                + "but something unexpected happened after the commit was completed.",
            e);
      } else {
        cleanUncommittedMeta(tempMetadataFile);
        throw new CommitFailedException(e);
      }
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
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
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
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
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
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

  @VisibleForTesting
  boolean isFirstRun() {
    return firstRun;
  }

  @VisibleForTesting
  Path getMetadataFile(Integer metadataVersion) throws IOException {
    // TODO:This is not compatible with the scenario where the user modifies the metadata file
    // compression codec arbitrarily.
    // We can inform the user about this bug first, and fix it later.(Do not modify the compressed
    // format after the table is created.)
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
    return new Path(metadataRoot(), filename);
  }

  private Path metadataRoot() {
    return new Path(location, "metadata");
  }

  private int version(String fileName) {
    Matcher matcher = VERSION_PATTERN.matcher(fileName);
    if (!matcher.matches()) {
      return -1;
    }
    String versionNumber = matcher.group(1);
    try {
      return Integer.parseInt(versionNumber);
    } catch (NumberFormatException ne) {
      return -1;
    }
  }

  @VisibleForTesting
  Path versionHintFile() {
    return metadataPath(Util.VERSION_HINT_FILENAME);
  }

  @VisibleForTesting
  boolean writeVersionHint(Integer versionToWrite) throws Exception {
    Path versionHintFile = versionHintFile();
    FileSystem fs = getFileSystem(versionHintFile, conf);
    Path tempVersionHintFile = metadataPath(UUID.randomUUID() + "-version-hint.temp");
    try {
      writeVersionToPath(fs, tempVersionHintFile, versionToWrite);
      return renameVersionHint(fs, tempVersionHintFile, versionHintFile);
    } catch (Exception e) {
      // Cleaning up temporary files.
      io().deleteFile(tempVersionHintFile.toString());
      throw e;
    }
  }

  @VisibleForTesting
  void deleteOldVersionHint(FileSystem fs, Path versionHintFile, Integer nextVersion)
      throws IOException {
    // In order to be compatible with scenarios where the iceberg table has just been created or
    // the last time there was no versionHint due to an unplanned interruption,
    // we need to ignore the lack of versionHint when the task is executed for the first time.
    // Otherwise, if we can't find the versionHint, there must be another task running in parallel
    // with us.
    // If multiple clients delete the same version Hint at the same time, only one client will
    // succeed.
    // By doing this, we solve the concurrency problem.
    // It can't handle scenarios where multiple clients are running at the same time for the first
    // time and versionHint not exists. But we will block them in the next step.
    boolean versionHintExists = versionHintExists(fs, versionHintFile);
    boolean canNotFindVersionHintAndNotFirstRun = !versionHintExists && !isFirstRun();
    if (canNotFindVersionHintAndNotFirstRun) {
      String msg =
          String.format(
              "Can not find old versionHint. commitVersion = %s.Do you have multiple tasks working on this table at the same time, or is your file system failing?",
              nextVersion);
      throw new RuntimeException(msg);
    }
    // Because the user always configures an expiration policy for the metadata.
    // If we don't do any checking, then the user can resubmit a version that has already expired.
    // So we need to check that the next commit is up-to-date.
    if (versionHintExists && (!nextVersionIsLatest(nextVersion) || !deleteVersionHint(fs))) {
      String msg =
          String.format(
              "Can not drop old versionHint. commitVersion = %s.Do you have multiple tasks working on this table at the same time, or is your file system failing?",
              nextVersion);
      throw new RuntimeException(msg);
    }
  }

  @VisibleForTesting
  boolean nextVersionIsLatest(Integer nextVersion) {
    return nextVersion == (findVersion() + 1);
  }

  @VisibleForTesting
  boolean deleteVersionHint(FileSystem fs) throws IOException {
    return fs.delete(versionHintFile(), false /* recursive delete*/);
  }

  @VisibleForTesting
  boolean versionHintExists(FileSystem fs, Path versionHintFile) throws IOException {
    return fs.exists(versionHintFile);
  }

  @VisibleForTesting
  boolean renameVersionHint(FileSystem fs, Path src, Path target) throws IOException {
    return fs.rename(src, target);
  }

  private void writeVersionToPath(FileSystem fs, Path path, int versionToWrite) {
    try (FSDataOutputStream out = fs.create(path, false /* overwrite */)) {
      out.write(String.valueOf(versionToWrite).getBytes(StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @VisibleForTesting
  int findVersion() {
    Path versionHintFile = versionHintFile();
    FileSystem fs = getFileSystem(versionHintFile, conf);

    try (InputStreamReader fsr =
            new InputStreamReader(fs.open(versionHintFile), StandardCharsets.UTF_8);
        BufferedReader in = new BufferedReader(fsr)) {
      return Integer.parseInt(in.readLine().replace("\n", ""));

    } catch (Exception e) {
      try {
        if (fs.exists(metadataRoot())) {
          LOG.warn("Error reading version hint file {}", versionHintFile, e);
        } else {
          // Either the table has just been created, or there has been a corruption which we will
          // not consider.
          LOG.warn("Metadata for table not found in directory [{}]", metadataRoot(), e);
          return 0;
        }

        // List the metadata directory to find the version files, and try to recover the max
        // available version
        FileStatus[] files =
            fs.listStatus(
                metadataRoot(), name -> VERSION_PATTERN.matcher(name.getName()).matches());
        int maxVersion = 0;

        for (FileStatus file : files) {
          int currentVersion = version(file.getPath().getName());
          if (currentVersion > maxVersion && getMetadataFile(currentVersion) != null) {
            maxVersion = currentVersion;
          }
        }

        return maxVersion;
      } catch (IOException io) {
        LOG.warn("Error trying to recover version-hint.txt data for {}", versionHintFile, e);
        // We failed to retrieve the version files, we should throw the exception
        throw new RuntimeIOException(io);
      }
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
  @VisibleForTesting
  boolean commitNewVersion(FileSystem fs, Path src, Path dst, Integer nextVersion)
      throws IOException {
    try {
      if (!lockManager.acquire(dst.toString(), dst.toString())) {
        throw new CommitFailedException(
            "Failed to acquire lock on file: %s with owner: %s", dst, src);
      }

      if (fs.exists(dst)) {
        throw new CommitFailedException("Version %d already exists: %s", nextVersion, dst);
      }
      return fs.rename(src, dst);
    } finally {
      if (!lockManager.release(dst.toString(), dst.toString())) {
        LOG.warn("Failed to release lock on file: {} with owner: {}", dst, src);
      }
    }
  }

  private void cleanUncommittedMeta(Path src) {
    io().deleteFile(src.toString());
  }

  protected FileSystem getFileSystem(Path path, Configuration hadoopConf) {
    return Util.getFs(path, hadoopConf);
  }

  /**
   * Deletes the oldest metadata files if {@link
   * TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  @VisibleForTesting
  void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit =
        metadata.propertyAsBoolean(
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
            TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    if (deleteAfterCommit) {
      Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles =
          Sets.newHashSet(base.previousFiles());
      metadata.previousFiles().forEach(removedPreviousMetadataFiles::remove);
      Tasks.foreach(removedPreviousMetadataFiles)
          .executeWith(ThreadPools.getWorkerPool())
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure(
              (previousMetadataFile, exc) ->
                  LOG.warn(
                      "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
          .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }

  private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata) {
    String newUUID = newMetadata.uuid();
    if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
      Preconditions.checkState(
          newUUID.equals(currentMetadata.uuid()),
          "Table UUID does not match: current=%s != refreshed=%s",
          currentMetadata.uuid(),
          newUUID);
    }
    return newMetadata;
  }
}
