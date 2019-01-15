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

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.netflix.iceberg.hadoop.HadoopFileIO;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.Tasks;
import java.util.UUID;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.netflix.iceberg.TableMetadataParser.getFileExtension;
import static com.netflix.iceberg.TableMetadataParser.read;
import static com.netflix.iceberg.hadoop.HadoopInputFile.fromLocation;

public abstract class BaseMetastoreTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreTableOperations.class);

  public static final String TABLE_TYPE_PROP = "table_type";
  public static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
  public static final String METADATA_LOCATION_PROP = "metadata_location";
  public static final String PREVIOUS_METADATA_LOCATION_PROP = "previous_metadata_location";

  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String DATA_FOLDER_NAME = "data";

  private final Configuration conf;

  private HadoopFileIO defaultIo;
  private TableMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private boolean shouldRefresh = true;
  private int version = -1;

  protected BaseMetastoreTableOperations(Configuration conf) {
    this.conf = conf;
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

  protected String writeNewMetadata(TableMetadata metadata, int version) {
<<<<<<< HEAD
    if (baseLocation == null) {
      baseLocation = metadata.location();
    }

    String newTableMetadataFilePath = newTableMetadataFilePath(baseLocation, version);
    OutputFile newMetadataLocation = io().newOutputFile(newTableMetadataFilePath);
||||||| merged common ancestors
    if (baseLocation == null) {
      baseLocation = metadata.location();
    }

    String newTableMetadataFilePath = newTableMetadataFilePath(baseLocation, version);
    OutputFile newMetadataLocation = fileIo.newOutputFile(newTableMetadataFilePath);
=======
    String newTableMetadataFilePath = newTableMetadataFilePath(metadata, version);
    OutputFile newMetadataLocation = fileIo.newOutputFile(newTableMetadataFilePath);
>>>>>>> upstream-incubator/master

    // write the new metadata
    TableMetadataParser.write(metadata, newMetadataLocation);
    if (defaultIo != null) {
      defaultIo.updateTableMetadata(metadata);
    }
    return newTableMetadataFilePath;
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    refreshFromMetadataLocation(newLocation, 20);
  }

  protected void refreshFromMetadataLocation(String newLocation, int numRetries) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Refreshing table metadata from new version: " + newLocation);

      Tasks.foreach(newLocation)
          .retry(numRetries).exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */ )
          .suppressFailureWhenFinished()
<<<<<<< HEAD
          .run(location -> {
            this.currentMetadata = read(this, fromLocation(location, conf));
            this.currentMetadataLocation = location;
            this.baseLocation = currentMetadata.location();
            this.version = parseVersion(location);
            if (defaultIo != null) {
              defaultIo.updateTableMetadata(currentMetadata);
            }
||||||| merged common ancestors
          .run(location -> {
            this.currentMetadata = read(this, fromLocation(location, conf));
            this.currentMetadataLocation = location;
            this.baseLocation = currentMetadata.location();
            this.version = parseVersion(location);
=======
          .run(metadataLocation -> {
            this.currentMetadata = read(this, fromLocation(metadataLocation, conf));
            this.currentMetadataLocation = metadataLocation;
            this.version = parseVersion(metadataLocation);
>>>>>>> upstream-incubator/master
          });
    }
    this.shouldRefresh = false;
  }

  private String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties()
        .get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", metadataLocation, filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  @Override
<<<<<<< HEAD
||||||| merged common ancestors
  public String metadataFileLocation(String fileName) {
    return String.format("%s/%s/%s", baseLocation, METADATA_FOLDER_NAME, fileName);
  }

  @Override
=======
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
>>>>>>> upstream-incubator/master
  public FileIO io() {
    if (defaultIo == null) {
      defaultIo = new HadoopFileIO(
          conf,
          baseLocation,
          currentMetadata == null ? ImmutableMap.of() : currentMetadata.properties());
    }
    return defaultIo;
  }

  private String newTableMetadataFilePath(TableMetadata meta, int newVersion) {
    return metadataFileLocation(meta,
        String.format("%05d-%s%s", newVersion, UUID.randomUUID(), getFileExtension(this.conf)));
  }

  private static int parseVersion(String metadataLocation) {
    int versionStart = metadataLocation.lastIndexOf('/') + 1; // if '/' isn't found, this will be 0
    int versionEnd = metadataLocation.indexOf('-', versionStart);
    try {
      return Integer.valueOf(metadataLocation.substring(versionStart, versionEnd));
    } catch (NumberFormatException e) {
      LOG.warn("Unable to parse version from metadata location: " + metadataLocation);
      return -1;
    }
  }
}
