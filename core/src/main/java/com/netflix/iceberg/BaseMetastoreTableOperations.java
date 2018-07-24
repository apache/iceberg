/*
 * Copyright 2017 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.iceberg;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.hadoop.HadoopOutputFile;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.util.Tasks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.util.UUID;

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
  private static final String HIVE_LOCATION_FOLDER_NAME = "empty";

  private final Configuration conf;

  private TableMetadata currentMetadata = null;
  private String currentMetadataLocation = null;
  private String baseLocation = null;
  private int version = -1;

  protected BaseMetastoreTableOperations(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public TableMetadata current() {
    return currentMetadata;
  }

  public String currentMetadataLocation() {
    return currentMetadataLocation;
  }

  public int currentVersion() {
    return version;
  }

  public String hiveTableLocation() {
    return String.format("%s/%s", baseLocation, HIVE_LOCATION_FOLDER_NAME);
  }

  public String dataLocation() {
    return String.format("%s/%s", baseLocation, DATA_FOLDER_NAME);
  }

  protected String writeNewMetadata(TableMetadata metadata, int version) {
    if (baseLocation == null) {
      baseLocation = metadata.location();
    }

    String newFilename = newTableMetadataFilename(baseLocation, version);
    OutputFile newMetadataLocation = HadoopOutputFile.fromPath(new Path(newFilename), conf);

    // write the new metadata
    TableMetadataParser.write(metadata, newMetadataLocation);

    return newFilename;
  }

  protected void refreshFromMetadataLocation(String newLocation) {
    // use null-safe equality check because new tables have a null metadata location
    if (!Objects.equal(currentMetadataLocation, newLocation)) {
      LOG.info("Refreshing table metadata from new version: " + newLocation);

      Tasks.foreach(newLocation)
          .retry(20).exponentialBackoff(100, 5000, 600000, 4.0 /* 100, 400, 1600, ... */ )
          .suppressFailureWhenFinished()
          .run(location -> {
            this.currentMetadata = read(this, fromLocation(location, conf));
            this.currentMetadataLocation = location;
            this.baseLocation = currentMetadata.location();
            this.version = parseVersion(location);
          });
    }
  }

  @Override
  public InputFile newInputFile(String path) {
    return fromLocation(path, conf);
  }

  @Override
  public OutputFile newMetadataFile(String filename) {
    return HadoopOutputFile.fromPath(
        new Path(newMetadataLocation(baseLocation, filename)), conf);
  }

  @Override
  public void deleteFile(String file) {
    Path path = new Path(file);
    try {
      getFS(path, conf).delete(path, false /* should be a file, not recursive */ );
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  @Override
  public long newSnapshotId() {
    return System.currentTimeMillis();
  }

  private static String newTableMetadataFilename(String baseLocation, int newVersion) {
    return String.format("%s/%s/%05d-%s.metadata.json",
        baseLocation, METADATA_FOLDER_NAME, newVersion, UUID.randomUUID());
  }

  private static String newMetadataLocation(String baseLocation, String filename) {
    return String.format("%s/%s/%s", baseLocation, METADATA_FOLDER_NAME, filename);
  }

  private static String parseBaseLocation(String metadataLocation) {
    int lastSlash = metadataLocation.lastIndexOf('/');
    int secondToLastSlash = metadataLocation.lastIndexOf('/', lastSlash);

    // verify that the metadata file was contained in a "metadata" folder
    String parentFolderName = metadataLocation.substring(secondToLastSlash + 1, lastSlash);
    Preconditions.checkArgument(METADATA_FOLDER_NAME.equals(parentFolderName),
        "Invalid metadata location, not in metadata/ folder: %s", metadataLocation);

    return metadataLocation.substring(0, secondToLastSlash);
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

  private static FileSystem getFS(Path path, Configuration conf) {
    try {
      return path.getFileSystem(conf);
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
}
