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

import com.google.common.base.Preconditions;
import com.netflix.iceberg.PartitionSpec;
import com.netflix.iceberg.StructLike;
import com.netflix.iceberg.TableMetadata;
import com.netflix.iceberg.TableProperties;
import com.netflix.iceberg.io.FileIO;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.io.InputFile;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.transforms.Transform;
import com.netflix.iceberg.transforms.Transforms;
import com.netflix.iceberg.types.Types;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileIO implements FileIO {

  private static final Transform<String, Integer> HASH_FUNC = Transforms
      .bucket(Types.StringType.get(), Integer.MAX_VALUE);

  private static final String METADATA_DIR_NAME = "metadata";
  private static final String DATA_DIR_NAME = "data";

  private final SerializableConfiguration hadoopConf;
  private final String tableLocation;
  private final String newDataFileDir;
  private final boolean useObjectStorage;
  private final String objectStorePath;
  private final String newMetadataFileDir;

  public HadoopFileIO(
      Configuration hadoopConf, TableMetadata tableMetadata) {
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.tableLocation = tableMetadata.location();
    Map<String, String> tableProperties = tableMetadata.properties();
    this.newDataFileDir = stripTrailingSlash(
        tableProperties
            .getOrDefault(
                TableProperties.WRITE_NEW_DATA_LOCATION,
                defaultDataDir()));
    this.useObjectStorage = Boolean.parseBoolean(
        tableProperties.getOrDefault(
            TableProperties.OBJECT_STORE_ENABLED,
            String.valueOf(TableProperties.OBJECT_STORE_ENABLED_DEFAULT)));
    if (useObjectStorage) {
      this.objectStorePath = stripTrailingSlash(
          tableProperties.get(TableProperties.OBJECT_STORE_PATH));
      Preconditions.checkNotNull(
          objectStorePath,
          "Cannot use object storage, missing location: %s",
          TableProperties.OBJECT_STORE_PATH);
    } else {
      this.objectStorePath = null;
    }
    String metadataLocation = tableProperties
        .get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      this.newMetadataFileDir = stripTrailingSlash(metadataLocation);
    } else {
      this.newMetadataFileDir = defaultMetadataDir();
    }
  }

  public HadoopFileIO(Configuration hadoopConf, String location) {
    this.tableLocation = location;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.newMetadataFileDir = defaultMetadataDir();
    this.useObjectStorage = TableProperties.OBJECT_STORE_ENABLED_DEFAULT;
    this.objectStorePath = null;
    this.newDataFileDir = defaultDataDir();
  }

  public HadoopFileIO(Configuration hadoopConf) {
    this.tableLocation = null;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.newMetadataFileDir = null;
    this.useObjectStorage = TableProperties.OBJECT_STORE_ENABLED_DEFAULT;
    this.objectStorePath = null;
    this.newDataFileDir = null;
  }

  private String defaultDataDir() {
    return String.format("%s/%s", tableLocation, DATA_DIR_NAME);
  }

  private String defaultMetadataDir() {
    return String.format("%s/%s", tableLocation, METADATA_DIR_NAME);
  }

  @Override
  public InputFile newInputFile(String path) {
    return HadoopInputFile.fromLocation(path, hadoopConf.get());
  }

  @Override
  public OutputFile newOutputFile(String path) {
    return HadoopOutputFile.fromPath(new Path(path), hadoopConf.get());
  }

  @Override
  public void deleteFile(String path) {
    Path toDelete = new Path(path);
    FileSystem fs = Util.getFS(toDelete, hadoopConf.get());
    try {
      fs.delete(toDelete, false /* not recursive */);
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", path);
    }
  }

  @Override
  public OutputFile newMetadataOutputFile(String fileName) {
    Preconditions.checkNotNull(
        newMetadataFileDir,
        "This file IO is not configured to write metadata to a specified location.");
    return newOutputFile(String.format("%s/%s", newMetadataFileDir, fileName));
  }

  @Override
  public OutputFile newDataOutputFile(
      PartitionSpec partitionSpec, StructLike filePartition, String fileName) {
    Preconditions.checkNotNull(
        newDataFileDir,
        "This file IO is not configured to write data to a specified location.");
    String location;
    if (useObjectStorage) {
      // try to get db and table portions of the path for context in the object store
      String context = pathContext(new Path(newDataFileDir));
      String partitionAndFilename = String.format(
          "%s/%s", partitionSpec.partitionToPath(filePartition), fileName);
      int hash = HASH_FUNC.apply(partitionAndFilename);
      location = String.format(
          "%s/%08x/%s/%s/%s",
          objectStorePath,
          hash,
          context,
          partitionSpec.partitionToPath(filePartition),
          fileName);
    } else {
      location = String.format(
          "%s/%s/%s",
          newDataFileDir,
          partitionSpec.partitionToPath(filePartition),
          fileName);
    }
    return newOutputFile(location);
  }

  @Override
  public OutputFile newDataOutputFile(String fileName) {
    Preconditions.checkNotNull(
        newDataFileDir,
        "This file IO is not configured to write data to a specified location.");
    return newOutputFile(String.format("%s/%s", newDataFileDir, fileName));
  }

  private static String stripTrailingSlash(String path) {
    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, path.length() - 1);
    }
    return result;
  }

  private static String pathContext(Path dataPath) {
    Path parent = dataPath.getParent();
    String resolvedContext;
    if (parent != null) {
      // remove the data folder
      if (dataPath.getName().equals("data")) {
        resolvedContext = pathContext(parent);
      } else {
        resolvedContext = String.format("%s/%s", parent.getName(), dataPath.getName());
      }
    } else {
      resolvedContext = dataPath.getName();
    }

    Preconditions.checkState(
        !resolvedContext.endsWith("/"),
        "Path context must not end with a slash.");
    return resolvedContext;
  }
}
