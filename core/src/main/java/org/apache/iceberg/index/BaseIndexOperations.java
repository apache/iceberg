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
package org.apache.iceberg.index;

import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.iceberg.BaseMetastoreOperations;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchIndexException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Objects;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseIndexOperations extends BaseMetastoreOperations
    implements IndexOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseIndexOperations.class);

  private static final String METADATA_FOLDER_NAME = "metadata";

  private IndexMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  protected BaseIndexOperations() {}

  protected void requestRefresh() {
    this.shouldRefresh = true;
  }

  protected void disableRefresh() {
    this.shouldRefresh = false;
  }

  protected abstract void doRefresh();

  protected abstract void doCommit(IndexMetadata base, IndexMetadata metadata);

  protected abstract String indexName();

  protected abstract FileIO io();

  protected String currentMetadataLocation() {
    return currentMetadataLocation;
  }

  protected int currentVersion() {
    return version;
  }

  @Override
  public IndexMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }

    return currentMetadata;
  }

  @Override
  public IndexMetadata refresh() {
    boolean currentMetadataWasAvailable = currentMetadata != null;
    try {
      doRefresh();
    } catch (NoSuchIndexException e) {
      if (currentMetadataWasAvailable) {
        LOG.warn("Could not find the index during refresh, setting current metadata to null", e);
        shouldRefresh = true;
      }

      currentMetadata = null;
      currentMetadataLocation = null;
      version = -1;
      throw e;
    }

    return current();
  }

  @Override
  @SuppressWarnings("ImmutablesReferenceEquality")
  public void commit(IndexMetadata base, IndexMetadata metadata) {
    // if the metadata is already out of date, reject it
    if (base != current()) {
      if (base != null) {
        throw new CommitFailedException("Cannot commit: stale index metadata");
      } else {
        // when current is non-null, the index exists. but when base is null, the commit is trying
        // to create the index
        throw new AlreadyExistsException("Index already exists: %s", indexName());
      }
    }

    // if the metadata is not changed, return early
    if (base == metadata) {
      LOG.info("Nothing to commit.");
      return;
    }

    long start = System.currentTimeMillis();
    doCommit(base, metadata);
    requestRefresh();

    LOG.info(
        "Successfully committed to index {} in {} ms",
        indexName(),
        System.currentTimeMillis() - start);
  }

  private String writeNewMetadata(IndexMetadata metadata, int newVersion) {
    String newMetadataFilePath = newMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newMetadataFilePath);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    IndexMetadataParser.overwrite(metadata, newMetadataLocation);

    return newMetadataLocation.location();
  }

  protected String writeNewMetadataIfRequired(IndexMetadata metadata) {
    return null != metadata.metadataFileLocation()
        ? metadata.metadataFileLocation()
        : writeNewMetadata(metadata, version + 1);
  }

  private String newMetadataFilePath(IndexMetadata metadata, int newVersion) {
    String codecName =
        metadata.currentVersion().properties() != null
            ? metadata
                .currentVersion()
                .properties()
                .getOrDefault(
                    IndexProperties.METADATA_COMPRESSION,
                    IndexProperties.METADATA_COMPRESSION_DEFAULT)
            : IndexProperties.METADATA_COMPRESSION_DEFAULT;
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(
        metadata,
        String.format(Locale.ROOT, "%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  private String metadataFileLocation(IndexMetadata metadata, String filename) {
    String metadataLocation =
        metadata.currentVersion().properties() != null
            ? metadata.currentVersion().properties().get(IndexProperties.WRITE_METADATA_LOCATION)
            : null;

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format(
          "%s/%s/%s",
          LocationUtil.stripTrailingSlash(metadata.location()), METADATA_FOLDER_NAME, filename);
    }
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    refreshFromMetadataLocation(newLocation, null, 20);
  }

  protected void refreshFromMetadataLocation(
      String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    refreshFromMetadataLocation(
        newLocation,
        shouldRetry,
        numRetries,
        metadataLocation -> IndexMetadataParser.read(io(), metadataLocation));
  }

  protected void refreshFromMetadataLocation(
      String newLocation,
      Predicate<Exception> shouldRetry,
      int numRetries,
      Function<String, IndexMetadata> metadataLoader) {
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Refreshing index metadata from new version: {}", newLocation);

      AtomicReference<IndexMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newLocation)
          .retry(numRetries)
          .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .throwFailureWhenFinished()
          .stopRetryOn(NotFoundException.class) // overridden if shouldRetry is non-null
          .shouldRetryTest(shouldRetry)
          .run(metadataLocation -> newMetadata.set(metadataLoader.apply(metadataLocation)));

      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newLocation;
      this.version = parseVersion(newLocation);
    }

    this.shouldRefresh = false;
  }

  /**
   * Parse the version from index metadata file name.
   *
   * @param metadataLocation index metadata file location
   * @return version of the index metadata file in success case and -1 if the version is not
   *     parsable (as a sign that the metadata is not part of this catalog)
   */
  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    if (versionEnd < 0) {
      // found filesystem index's metadata
      return -1;
    }

    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }
}
