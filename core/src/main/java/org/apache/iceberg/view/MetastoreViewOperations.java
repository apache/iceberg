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

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MetastoreViewOperations implements ViewOperations {
  private static final Logger LOG = LoggerFactory.getLogger(MetastoreViewOperations.class);

  public static final String METADATA_LOCATION_PROP = "metadata_location";
  public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";
  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String METADATA_FOLDER_NAME = "metadata";

  private ViewMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  @Override
  public ViewMetadata current() {
    if (shouldRefresh) {
      return refresh();
    }
    return currentMetadata;
  }

  public String currentMetadataLocation() {
    return currentMetadataLocation;
  }

  public int currentVersion() {
    return version;
  }

  protected void requestRefresh() {
    this.shouldRefresh = true;
  }

  protected abstract String viewName();

  protected String writeNewMetadata(ViewMetadata metadata, int newVersion) {
    String newViewMetadataFilePath = newViewMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = io().newOutputFile(newViewMetadataFilePath);

    // write the new metadata
    // use overwrite to avoid negative caching in S3. this is safe because the metadata location is
    // always unique because it includes a UUID.
    ViewMetadataParser.overwrite(metadata, newMetadataLocation);
    return newViewMetadataFilePath;
  }

  protected void refreshFromMetadataLocation(
      String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equals(currentMetadataLocation, newLocation)) {
      LOG.info("Refreshing table metadata from new version: {}", newLocation);

      AtomicReference<ViewMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newLocation)
          .retry(numRetries)
          .exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .throwFailureWhenFinished()
          .shouldRetryTest(shouldRetry)
          .run(
              metadataLocation ->
                  newMetadata.set(ViewMetadataParser.read(io().newInputFile(metadataLocation))));
      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newLocation;
      this.version = parseVersion(newLocation);
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(ViewMetadata metadata, String filename) {
    return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
  }

  private String newViewMetadataFilePath(ViewMetadata meta, int newVersion) {
    return metadataFileLocation(
        meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), ".metadata.json"));
  }

  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    try {
      return Integer.parseInt(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: {}", metadataLocation, e);
      return -1;
    }
  }

  public enum CommitStatus {
    SUCCESS,
    FAILURE,
    UNKNOWN
  }
}
