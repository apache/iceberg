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

  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final String DATA_FOLDER_NAME = "data";

  private final SerializableConfiguration hadoopConf;
  private String tableLocation;
  private String newDataFileLocation;
  private boolean useObjectStorage = TableProperties.OBJECT_STORE_ENABLED_DEFAULT;
  private String objectStorePath;
  private String newMetadataFileDir;

  public HadoopFileIO(
      Configuration hadoopConf, String initialTableLocation, Map<String, String> initialProperties) {
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    update(initialTableLocation, initialProperties);
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
    return newOutputFile(String.format("%s/%s", newMetadataFileDir, fileName));
  }

  @Override
  public OutputFile newDataOutputFile(
      PartitionSpec partitionSpec, StructLike filePartition, String fileName) {
    String location;
    if (useObjectStorage) {
      // try to get db and table portions of the path for context in the object store
      String context = pathContext(new Path(newDataFileLocation));
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
          newDataFileLocation,
          partitionSpec.partitionToPath(filePartition),
          fileName);
    }
    return newOutputFile(location);
  }

  @Override
  public OutputFile newDataOutputFile(String fileName) {
    return newOutputFile(String.format("%s/%s", newDataFileLocation, fileName));
  }

  public void updateTableMetadata(TableMetadata newMetadata) {
    update(newMetadata.location(), newMetadata.properties());
  }

  private void update(String newTableLocation, Map<String, String> newTableProperties) {
    this.newDataFileLocation = stripTrailingSlash(
        newTableProperties
            .getOrDefault(
                TableProperties.WRITE_NEW_DATA_LOCATION,
                String.format("%s/data", tableLocation)));
    this.useObjectStorage = Boolean.parseBoolean(
        newTableProperties.getOrDefault(
            TableProperties.OBJECT_STORE_ENABLED,
            String.valueOf(TableProperties.OBJECT_STORE_ENABLED_DEFAULT)));
    if (useObjectStorage) {
      this.objectStorePath = stripTrailingSlash(
          newTableProperties.get(TableProperties.OBJECT_STORE_PATH));
      Preconditions.checkNotNull(
          objectStorePath,
          "Cannot use object storage, missing location: %s",
          TableProperties.OBJECT_STORE_PATH);
    }
    this.tableLocation = newTableLocation;
    String metadataLocation = newTableProperties
        .get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      this.newMetadataFileDir = stripTrailingSlash(metadataLocation);
    } else {
      this.newMetadataFileDir = String.format("%s/%s", newTableLocation, METADATA_FOLDER_NAME);
    }
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
