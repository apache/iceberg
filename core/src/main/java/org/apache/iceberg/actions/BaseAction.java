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

package org.apache.iceberg.actions;

import java.util.List;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

abstract class BaseAction<ThisT, R> implements Action<ThisT, R> {

  protected abstract Table table();

  protected String metadataTableName(MetadataTableType type) {
    return metadataTableName(table().name(), type);
  }

  protected String metadataTableName(String tableName, MetadataTableType type) {
    if (tableName.contains("/")) {
      return tableName + "#" + type;
    } else if (tableName.startsWith("hadoop.")) {
      // for HadoopCatalog tables, use the table location to load the metadata table
      // because IcebergCatalog uses HiveCatalog when the table is identified by name
      return table().location() + "#" + type;
    } else if (tableName.startsWith("hive.")) {
      // HiveCatalog prepend a logical name which we need to drop for Spark 2.4
      return tableName.replaceFirst("hive\\.", "") + "." + type;
    } else {
      return tableName + "." + type;
    }
  }

  /**
   * Returns all the path locations of all Manifest Lists for a given list of snapshots
   *
   * @param snapshots snapshots
   * @return the paths of the Manifest Lists
   */
  protected List<String> getManifestListPaths(Iterable<Snapshot> snapshots) {
    List<String> manifestLists = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestLists.add(manifestListLocation);
      }
    }
    return manifestLists;
  }

  /**
   * Returns all Metadata file paths which may not be in the current metadata. Specifically this includes "version-hint"
   * files as well as entries in metadata.previousFiles.
   *
   * @param ops TableOperations for the table we will be getting paths from
   * @return a list of paths to metadata files
   */
  protected List<String> getOtherMetadataFilePaths(TableOperations ops) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.add(ops.metadataFileLocation("version-hint.text"));

    TableMetadata metadata = ops.current();
    otherMetadataFiles.add(metadata.metadataFileLocation());
    for (TableMetadata.MetadataLogEntry previousMetadataFile : metadata.previousFiles()) {
      otherMetadataFiles.add(previousMetadataFile.file());
    }
    return otherMetadataFiles;
  }

}
