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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReachableFileUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ReachableFileUtils.class);

  private ReachableFileUtils() {
  }

  /**
   * Returns the location of version.text file
   * @param table table whose version.text path needs to be retrieved
   * @return the path to version.text
   */
  public static String versionHintLocation(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    return ops.metadataFileLocation("version-hint.text");
  }

  /**
   * Returns the metadata.json files associated with {@code table}
   * @param table table to get the metadata json files from
   * @param recursive
   * <p>When true, recursively retrieves all the reachable metadata.json files.
   * <p>when false, gets the all the metadata.json files only from the current metadata.
   * @return a list of paths to metadata files
   */
  public static Set<String> metadataFileLocations(Table table, boolean recursive) {
    Set<String> metadataFileLocations = new HashSet<>();
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata tableMetadata = ops.current();
    metadataFileLocations.add(tableMetadata.metadataFileLocation());
    metadataFileLocations(tableMetadata, metadataFileLocations, ops.io(), recursive);
    return metadataFileLocations;
  }

  private static void metadataFileLocations(TableMetadata metadata, Set<String> metaFiles,
                                            FileIO io, boolean isRecursive) {
    List<TableMetadata.MetadataLogEntry> metadataLogEntries = metadata.previousFiles();
    List<String> previousMetadataFiles =
        metadataLogEntries.stream().map(TableMetadata.MetadataLogEntry::file)
            .collect(Collectors.toList());
    if (previousMetadataFiles.size() > 0) {
      metaFiles.addAll(previousMetadataFiles);
      // Find the first existent metadata json file and recurse
      if (isRecursive) {
        for (String metadataFileLocation : previousMetadataFiles) {
          try {
            TableMetadata newMetadata = TableMetadataParser.read(io, metadataFileLocation);
            metadataFileLocations(newMetadata, metaFiles, io, isRecursive);
            break;
          } catch (Exception e) {
            LOG.error("Failed to load {}", metadataFileLocation, e);
          }
        }
      }
    }
  }

  /**
   * Returns all the path locations of all Manifest Lists for a given table
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
