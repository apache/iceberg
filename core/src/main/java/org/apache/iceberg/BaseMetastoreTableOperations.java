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
package org.apache.iceberg;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.NativeEncryptionKeyMetadata;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreTableOperations extends BaseMetastoreOperations
    implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreTableOperations.class);

  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
  public static final String METADATA_LOCATION_PROP = "metadata_location";
  public static final String METADATA_WRAPPED_KEY_PROP = "metadata_wrapped_key";
  public static final String METADATA_SIZE_PROP = "metadata_size";
  public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";
  public static final String PREVIOUS_METADATA_WRAPPED_KEY_PROP = "previous_metadata_wrapped_key";
  public static final String PREVIOUS_METADATA_SIZE_PROP = "previous_metadata_size";

  private static final String METADATA_FOLDER_NAME = "metadata";

  private TableMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private MetadataFile currentMetadataFile = null;
  private boolean shouldRefresh = true;
  private int version = -1;
  private String encryptionKeyId;
  private int encryptionDekLength = -1;

  protected BaseMetastoreTableOperations() {}

  /**
   * The full name of the table used for logging purposes only. For example for HiveTableOperations
   * it is catalogName + "." + database + "." + table.
   *
   * @return The full name
   */
  protected abstract String tableName();

  protected String encryptionKeyIdFromProps() {
    return encryptionKeyId;
  }

  protected int dekLength() {
    return encryptionDekLength;
  }

  @Override
  public TableMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  public String currentMetadataLocation() {
    return currentMetadataLocation;
  }

  public MetadataFile currentMetadataFile() {
    return currentMetadataFile;
  }

  public int currentVersion() {
    return version;
  }

  @Override
  public TableMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      doRefresh();
    } catch (NoSuchTableException e) {
      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the table during refresh, setting current metadata to null", e);
        shouldRefresh = true;
      }

      currentMetadata = null;
      currentMetadataLocation = null;
      version = -1;
      throw e;
    }
    return current();
  }

  protected void doRefresh() {
    throw new UnsupportedOperationException("Not implemented: doRefresh");
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      if (base != null) {
        throw new CommitFailedException("Cannot commit: stale table metadata");
      } else {
        // when current is non-null, the table exists. but when base is null, the commit is trying
        // to create the table
        throw new AlreadyExistsException("Table already exists: %s", tableName());
      }
    }
    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();
    doCommit(base, metadata);
    deleteRemovedMetadataFiles(base, metadata);
    requestRefresh();

    LOG.info(
        "Successfully committed to table {} in {} ms",
        tableName(),
        System.currentTimeMillis() - start);
  }

  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Not implemented: doCommit");
  }

  protected void requestRefresh() {
    this.shouldRefresh = true;
  }

  protected void disableRefresh() {
    this.shouldRefresh = false;
  }

  protected String writeNewMetadataIfRequired(boolean newTable, TableMetadata metadata) {
    return writeNewMetadataFileIfRequired(newTable, metadata).location();
  }

  protected MetadataFile writeNewMetadataFileIfRequired(boolean newTable, TableMetadata metadata) {
    return newTable && metadata.metadataFileLocation() != null
        ? new MetadataFile(metadata.metadataFileLocation(), null, -1L)
        : writeNewMetadataFile(metadata, currentVersion() + 1);
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    return writeNewMetadataFile(metadata, newVersion).location();
  }

  protected MetadataFile writeNewMetadataFile(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);

    if (encryptionKeyId == null) {
      encryptionKeyId = metadata.property(TableProperties.ENCRYPTION_TABLE_KEY, null);
    }

    OutputFile newMetadataFile;
    String wrappedMetadataKey;

    if (encryptionKeyId != null) {

      if (encryptionDekLength < 0) {
        String encryptionDekLenProp =
            metadata.property(TableProperties.ENCRYPTION_DEK_LENGTH, null);
        encryptionDekLength =
            (encryptionDekLenProp == null)
                ? TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT
                : Integer.valueOf(encryptionDekLenProp);
      }

      FileIO io = io();
      Preconditions.checkArgument(
          io instanceof EncryptingFileIO,
          "Cannot encrypt table metadata because the fileIO (%s) does not "
              + "implement EncryptingFileIO",
          io.getClass());
      EncryptingFileIO encryptingIO = (EncryptingFileIO) io();
      EncryptedOutputFile newEncryptedMetadataFile =
          encryptingIO.newEncryptingOutputFile(newTableMetadataFilePath);

      if (newEncryptedMetadataFile.keyMetadata() == null
          || newEncryptedMetadataFile.keyMetadata().buffer() == null) {
        throw new IllegalStateException("Null key metadata in encrypted table");
      }

      newMetadataFile = newEncryptedMetadataFile.encryptingOutputFile();
      EncryptionManager encryptionManager = encryptingIO.encryptionManager();

      Preconditions.checkArgument(
          encryptionManager instanceof StandardEncryptionManager,
          "Cannot encrypt table metadata because the encryption manager (%s) does not "
              + "implement StandardEncryptionManager",
          encryptionManager.getClass());
      NativeEncryptionKeyMetadata keyMetadata =
          (NativeEncryptionKeyMetadata) newEncryptedMetadataFile.keyMetadata();
      ByteBuffer metadataEncryptionKey = keyMetadata.encryptionKey();
      // Wrap (encrypt) metadata file key
      ByteBuffer wrappedEncryptionKey =
          ((StandardEncryptionManager) encryptionManager).wrapKey(metadataEncryptionKey);

      ByteBuffer metadataAADPrefix = keyMetadata.aadPrefix();
      wrappedMetadataKey =
          Base64.getEncoder()
              .encodeToString(
                  EncryptionUtil.createKeyMetadata(wrappedEncryptionKey, metadataAADPrefix)
                      .buffer()
                      .array());
    } else {
      newMetadataFile = io().newOutputFile(newTableMetadataFilePath);
      wrappedMetadataKey = null;
    }

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    long size = TableMetadataParser.overwrite(metadata, newMetadataFile);

    if (encryptionKeyId != null && size <= 0) {
      throw new RuntimeException("Metadata file size is not recorded in an encrypted table");
    }

    return new MetadataFile(newMetadataFile.location(), wrappedMetadataKey, size);
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    refreshFromMetadataLocation(newLocation, null, 20);
  }

  protected void refreshFromMetadataLocation(String newLocation, int numRetries) {
    refreshFromMetadataLocation(newLocation, null, numRetries);
  }

  protected void refreshFromMetadataLocation(MetadataFile newLocation, int numRetries) {
    refreshFromMetadataLocation(newLocation, null, numRetries);
  }

  protected void refreshFromMetadataLocation(
      String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    refreshFromMetadataLocation(
        newLocation,
        shouldRetry,
        numRetries,
        metadataLocation -> TableMetadataParser.read(io(), metadataLocation));
  }

  protected void refreshFromMetadataLocation(
      MetadataFile newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    refreshFromMetadataLocation(
        newLocation,
        shouldRetry,
        numRetries,
        metadataFile -> TableMetadataParser.read(io(), metadataFile));
  }

  protected void refreshFromMetadataLocation(
      String newLocation,
      Predicate<Exception> shouldRetry,
      int numRetries,
      Function<String, TableMetadata> metadataLoader) {
    MetadataFile metadataFile = new MetadataFile(newLocation, null, 0);

    refreshFromMetadataLocation(
        metadataFile,
        shouldRetry,
        numRetries,
        metadataLocation -> metadataLoader.apply(metadataLocation.location()));
  }

  protected void refreshFromMetadataLocation(
      MetadataFile newMetadataFile,
      Predicate<Exception> shouldRetry,
      int numRetries,
      Function<MetadataFile, TableMetadata> metadataLoader) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newMetadataFile.location())) {
      LOG.info("Refreshing table metadata from new version: {}", newMetadataFile.location());

      AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newMetadataFile)
          .retry(numRetries)
          .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .throwFailureWhenFinished()
          .stopRetryOn(NotFoundException.class) // overridden if shouldRetry is non-null
          .shouldRetryTest(shouldRetry)
          .run(metadataLocation -> newMetadata.set(metadataLoader.apply(metadataLocation)));

      String newUUID = newMetadata.get().uuid();
      if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
        Preconditions.checkState(
            newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s",
            currentMetadata.uuid(),
            newUUID);
      }

      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newMetadataFile.location();
      this.currentMetadataFile = newMetadataFile;
      this.version = parseVersion(newMetadataFile.location());
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
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
        return BaseMetastoreTableOperations.this.metadataFileLocation(
            uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return BaseMetastoreTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return BaseMetastoreTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return BaseMetastoreTableOperations.this.newSnapshotId();
      }
    };
  }

  /**
   * @deprecated since 1.6.0, will be removed in 1.7.0; Use {@link
   *     BaseMetastoreOperations.CommitStatus} instead
   */
  @Deprecated
  protected enum CommitStatus {
    FAILURE,
    SUCCESS,
    UNKNOWN
  }

  /**
   * Attempt to load the table and see if any current or past metadata location matches the one we
   * were attempting to set. This is used as a last resort when we are dealing with exceptions that
   * may indicate the commit has failed but are not proof that this is the case. Past locations must
   * also be searched on the chance that a second committer was able to successfully commit on top
   * of our commit.
   *
   * @param newMetadataLocation the path of the new commit file
   * @param config metadata to use for configuration
   * @return Commit Status of Success, Failure or Unknown
   */
  protected CommitStatus checkCommitStatus(String newMetadataLocation, TableMetadata config) {
    return CommitStatus.valueOf(
        checkCommitStatus(
                tableName(),
                newMetadataLocation,
                config.properties(),
                () -> checkCurrentMetadataLocation(newMetadataLocation))
            .name());
  }

  /**
   * Validate if the new metadata location is the current metadata location or present within
   * previous metadata files.
   *
   * @param newMetadataLocation newly written metadata location
   * @return true if the new metadata location is the current metadata location or present within
   *     previous metadata files.
   */
  private boolean checkCurrentMetadataLocation(String newMetadataLocation) {
    TableMetadata metadata = refresh();
    String currentMetadataFileLocation = metadata.metadataFileLocation();
    return currentMetadataFileLocation.equals(newMetadataLocation)
        || metadata.previousFiles().stream()
            .anyMatch(log -> log.file().equals(newMetadataLocation));
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName =
        meta.property(
            TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(
        meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  /**
   * Parse the version from table metadata file name.
   *
   * @param metadataLocation table metadata file location
   * @return version of the table metadata file in success case and -1 if the version is not
   *     parsable (as a sign that the metadata is not part of this catalog)
   */
  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem table's metadata
      return -1;
    }

    try {
      return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  /**
   * Deletes the oldest metadata files if {@link
   * TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
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
      // TableMetadata#addPreviousFile builds up the metadata log and uses
      // TableProperties.METADATA_PREVIOUS_VERSIONS_MAX to determine how many files should stay in
      // the log, thus we don't include metadata.previousFiles() for deletion - everything else can
      // be removed
      removedPreviousMetadataFiles.removeAll(metadata.previousFiles());
      Tasks.foreach(removedPreviousMetadataFiles)
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure(
              (previousMetadataFile, exc) ->
                  LOG.warn(
                      "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
          .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }
}
