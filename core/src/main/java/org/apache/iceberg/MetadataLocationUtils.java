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
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataLocationUtils {

  private MetadataLocationUtils() {
  }

  private static final Logger LOG = LoggerFactory.getLogger(MetadataLocationUtils.class);

  /**
   * Returns all Metadata file paths which may not be in the current metadata. Specifically
   * this includes "version-hint" files as well as entries in metadata.previousFiles.
   * @param ops TableOperations of the table to get paths from
   * @param isRecursive when true, recursively retrieves all the metadata.json files using metadata.previousFiles
   *                    when false, gets the all the metadata.json files pointed to by the only current metadata.json
   * @return a list of paths to metadata files
   */
  public static List<String> miscMetadataFiles(TableOperations ops, boolean isRecursive) {
    Set<String> miscMetadataLocation = new HashSet<>();
    miscMetadataLocation.add(ops.metadataFileLocation("version-hint.text"));
    String location = ops.current().metadataFileLocation();
    miscMetadataLocation.add(location);
    miscMetadataFiles(location, miscMetadataLocation, ops.io(), isRecursive);
    return new ArrayList<>(miscMetadataLocation);
  }

  private static void miscMetadataFiles(String metadataFileLocation, Set<String> metaFiles,
                                        FileIO io, boolean isRecursive) {
    if (metadataFileLocation == null) {
      return;
    }
    try {
      TableMetadata metadata = TableMetadataParser.read(io, metadataFileLocation);
      List<TableMetadata.MetadataLogEntry> metadataLogEntries = metadata.previousFiles();
      List<String> previousMetadataFiles =
          metadataLogEntries.stream().map(TableMetadata.MetadataLogEntry::file).collect(Collectors.toList());
      metaFiles.addAll(previousMetadataFiles);
      if (isRecursive && previousMetadataFiles.size() > 0) {
        miscMetadataFiles(previousMetadataFiles.get(0), metaFiles, io, isRecursive);
      }
    } catch (NotFoundException e) {
      LOG.info("File not found", e);
    }
  }

  /**
   * Returns all the path locations of all Manifest Lists for a given list of snapshots
   * @param table table for which manifestList needs to be fetched
   * @return the paths of the Manifest Lists
   */
  public static List<String> manifestListPaths(Table table) {
    Iterable<Snapshot> snapshots = table.snapshots();
    List<String> manifestLists = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestLists.add(manifestListLocation);
      }
    }
    return manifestLists;
  }
}
