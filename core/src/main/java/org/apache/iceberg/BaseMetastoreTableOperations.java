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

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseMetastoreTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreTableOperations.class);

  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
  public static final String METADATA_LOCATION_PROP = "metadata_location";
  public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";

  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String DATA_FOLDER_NAME = "data";

  private final Configuration conf;
  private final FileIO fileIo;

  private TableMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  protected BaseMetastoreTableOperations(Configuration conf) {
    this.conf = conf;
    this.fileIo = new HadoopFileIO(conf);
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

  public int currentVersion() {
    return version;
  }

  protected void requestRefresh() {
    this.shouldRefresh = true;
  }

  protected String writeNewMetadata(TableMetadata metadata, int newVersion) {
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, newVersion);
    OutputFile newMetadataLocation = fileIo.newOutputFile(newTableMetadataFilePath);

    // write the new metadata
    TableMetadataParser.write(metadata, newMetadataLocation);

    return newTableMetadataFilePath;
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    refreshFromMetadataLocation(newLocation, 20);
  }

  protected void refreshFromMetadataLocation(String newLocation, int numRetries) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      int metadataVersion = parseVersion(newLocation);
      LOG.info("Refreshing table metadata from new version: {} ({})", metadataVersion, newLocation);

      AtomicReference<TableMetadata> newMetadata = new AtomicReference<>();
      Tasks.foreach(newLocation)
          .retry(numRetries).exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */)
          .suppressFailureWhenFinished()
          .run(metadataLocation -> newMetadata.set(
              TableMetadataParser.read(this,
                new BaseTableMetadataFile(io().newInputFile(metadataLocation), metadataVersion))));

      String newUUID = newMetadata.get().uuid();
      if (currentMetadata != null) {
        Preconditions.checkState(newUUID == null || newUUID.equals(currentMetadata.uuid()),
            "Table UUID does not match: current=%s != refreshed=%s", currentMetadata.uuid(), newUUID);
      }

      this.currentMetadata = newMetadata.get();
      this.currentMetadataLocation = newLocation;
      this.version = metadataVersion;
    }
    this.shouldRefresh = false;
  }

  private Path metadataLocation(TableMetadata metadata) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);
    return metadataLocation == null ?
        new Path(metadata.location(), METADATA_FOLDER_NAME) :
        new Path(metadataLocation);
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    return new Path(metadataLocation(metadata), filename).toString();
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public FileIO io() {
    return fileIo;
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public Iterable<TableMetadataFile> tableMetadataFiles() {
    Path metadataLocation = metadataLocation(current());
    try {
      FileSystem fs = metadataLocation.getFileSystem(conf);
      FileStatus[] fileStatuses = fs.listStatus(metadataLocation,
        path -> path.getName().endsWith(TableMetadataParser.fileSuffix()));

      return Stream.of(fileStatuses)
        .map(FileStatus::getPath)
        .map(path -> new BaseTableMetadataFile(io().newInputFile(path.toString()), parseVersion(path.toString())))
        .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeIOException("Unable to list table metadata files", e);
    }
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    String codecName = meta.property(
        TableProperties.METADATA_COMPRESSION, TableProperties.METADATA_COMPRESSION_DEFAULT);
    String fileExtension = TableMetadataParser.getFileExtension(codecName);
    return metadataFileLocation(meta, String.format("%05d-%s%s", newVersion, UUID.randomUUID(), fileExtension));
  }

  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    try {
      return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      throw new RuntimeIOException("Unable to parse metadata version for: {}", metadataLocation, e);
    }
  }
}
