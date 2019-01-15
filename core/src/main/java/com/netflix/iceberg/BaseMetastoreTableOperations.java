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

  private final Configuration conf;

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
    // This would be false if the subclass overrides io() and returns a different kind.
    OutputFile newMetadataOutputFile = io(metadata).newMetadataOutputFile(
        newTableMetadataFileName(version));

    // write the new metadata
    TableMetadataParser.write(metadata, newMetadataOutputFile);
    return newMetadataOutputFile.location();
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
          .run(metadataLocation -> {
            this.currentMetadata = read(this, fromLocation(metadataLocation, conf));
            this.currentMetadataLocation = metadataLocation;
            this.version = parseVersion(metadataLocation);
          });
    }
    this.shouldRefresh = false;
  }

  @Override
  public FileIO io() {
    return new HadoopFileIO(conf, currentMetadata);
  }

  @Override
  public FileIO io(TableMetadata tableMetadata) {
    return new HadoopFileIO(conf, tableMetadata);
  }

  private String newTableMetadataFileName(int newVersion) {
    return String.format(
        "%05d-%s%s", newVersion, UUID.randomUUID(), getFileExtension(this.conf));
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
