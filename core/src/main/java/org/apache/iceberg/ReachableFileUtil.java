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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReachableFileUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ReachableFileUtil.class);

  private ReachableFileUtil() {
  }

  /**
   * Returns the location of the version.text file
   *
   * @param ops tableOperation for which version.text path needs to be retrieved
   * @return the location of the version hint file
   */
  public static String versionHintLocation(TableOperations ops) {
    return ops.metadataFileLocation("version-hint.text");
  }

  /**
   * Returns locations of JSON metadata files in a table.
   *
   * @param ops       TableOperations to get JSON metadata files from
   * @param recursive When true, recursively retrieves all the reachable JSON metadata files.
   *                 When false, gets the all the JSON metadata files only from the current metadata.
   * @return locations of JSON metadata files
   */
  public static Set<String> metadataFileLocations(TableOperations ops, boolean recursive) {
    Set<String> metadataFileLocations = Sets.newHashSet();
    TableMetadata tableMetadata = ops.current();
    metadataFileLocations.add(tableMetadata.metadataFileLocation());
    metadataFileLocations(tableMetadata, metadataFileLocations, ops.io(), recursive);
    return metadataFileLocations;
  }

  private static void metadataFileLocations(TableMetadata metadata, Set<String> metaFiles,
                                            FileIO io, boolean isRecursive) {
    List<TableMetadata.MetadataLogEntry> metadataLogEntries = metadata.previousFiles();
    if (metadataLogEntries.size() > 0) {
      for (TableMetadata.MetadataLogEntry metadataLogEntry : metadataLogEntries) {
        metaFiles.add(metadataLogEntry.file());
      }
      if (isRecursive) {
        String metadataFileLocation = metadataLogEntries.get(0).file();
        try {
          TableMetadata newMetadata = TableMetadataParser.read(io, metadataFileLocation);
          metadataFileLocations(newMetadata, metaFiles, io, isRecursive);
        } catch (Exception e) {
          LOG.error("Failed to load {}", metadataFileLocation, e);
        }
      }
    }
  }

  /**
   * Returns locations of manifest lists in a table.
   *
   * @param table table for which manifestList needs to be fetched
   * @return the paths of the Manifest Lists
   */
  public static List<String> manifestListLocations(Table table) {
    Iterable<Snapshot> snapshots = table.snapshots();
    List<String> manifestListLocations = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestListLocations.add(manifestListLocation);
      }
    }
    return manifestListLocations;
  }
}
