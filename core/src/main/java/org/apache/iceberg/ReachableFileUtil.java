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

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.TableMetadata.MetadataLogEntry;
import org.apache.iceberg.hadoop.Util;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReachableFileUtil {

  private static final Logger LOG = LoggerFactory.getLogger(ReachableFileUtil.class);
  private static final String METADATA_FOLDER_NAME = "metadata";

  private ReachableFileUtil() {
  }

  /**
   * Returns the location of the version hint file
   *
   * @param table table for which version hint file's path needs to be retrieved
   * @return the location of the version hint file
   */
  public static String versionHintLocation(Table table) {
    // only Hadoop tables have a hint file and such tables have a fixed metadata layout
    Path metadataPath = new Path(table.location(), METADATA_FOLDER_NAME);
    Path versionHintPath = new Path(metadataPath, Util.VERSION_HINT_FILENAME);
    return versionHintPath.toString();
  }

  /**
   * Returns locations of JSON metadata files in a table.
   *
   * @param table     Table to get JSON metadata files from
   * @param recursive When true, recursively retrieves all the reachable JSON metadata files.
   *                  When false, gets the all the JSON metadata files only from the current metadata.
   * @return locations of JSON metadata files
   */
  public static Set<String> metadataFileLocations(Table table, boolean recursive) {
    Set<String> metadataFileLocations = Sets.newHashSet();
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata tableMetadata = ops.current();
    metadataFileLocations.add(tableMetadata.metadataFileLocation());
    metadataFileLocations(tableMetadata, metadataFileLocations, ops.io(), recursive);
    return metadataFileLocations;
  }

  private static void metadataFileLocations(TableMetadata metadata, Set<String> metadataFileLocations,
                                            FileIO io, boolean recursive) {
    List<MetadataLogEntry> metadataLogEntries = metadata.previousFiles();
    if (metadataLogEntries.size() > 0) {
      for (MetadataLogEntry metadataLogEntry : metadataLogEntries) {
        metadataFileLocations.add(metadataLogEntry.file());
      }
      if (recursive) {
        TableMetadata previousMetadata = findFirstExistentPreviousMetadata(metadataLogEntries, io);
        if (previousMetadata != null) {
          metadataFileLocations(previousMetadata, metadataFileLocations, io, recursive);
        }
      }
    }
  }

  private static TableMetadata findFirstExistentPreviousMetadata(List<MetadataLogEntry> metadataLogEntries, FileIO io) {
    TableMetadata metadata = null;
    for (MetadataLogEntry metadataLogEntry : metadataLogEntries) {
      try {
        metadata = TableMetadataParser.read(io, metadataLogEntry.file());
        break;
      } catch (Exception e) {
        LOG.error("Failed to load {}", metadataLogEntry, e);
      }
    }
    return metadata;
  }

  /**
   * Returns locations of manifest lists in a table.
   *
   * @param table table for which manifestList needs to be fetched
   * @return the location of manifest Lists
   */
  public static List<String> manifestListLocations(Table table) {
    Iterable<Snapshot> snapshots = table.snapshots();
    List<String> manifestListLocations = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestListLocations.add(manifestListLocation);
      }
    }
    return manifestListLocations;
  }

  /**
   * Emits a human-readable metadata graph of the Iceberg table's current snapshot
   * @param table Iceberg table
   * @param out output stream to emit to
   */
  public static void printTable(Table table, PrintStream out) {
    out.println("------------------------------------------------------------");
    out.println("Table " + table.name());
    out.println("------------------------------------------------------------");
    printSnapshot(table, table.currentSnapshot(), out);
  }

  /**
   * Emits a human-readable metadata graph of the given snapshot
   * @param table Iceberg table
   * @param snapshot Iceberg table snapshot
   * @param out output print stream to emit to
   */
  public static void printSnapshot(Table table, Snapshot snapshot, PrintStream out) {
    out.println(snapshot);
    Iterator<ManifestFile> allManifests = snapshot.allManifests().iterator();
    while (allManifests.hasNext()) {
      ManifestFile mf = allManifests.next();
      out.println(" \\---" + mf);

      if (mf.content().equals(ManifestContent.DATA)) {
        ManifestReader<DataFile> dataReader = ManifestFiles.read(mf, table.io(), table.specs());
        try (CloseableIterator<ManifestEntry<DataFile>> dataEntries = dataReader.entries().iterator()) {
          while (dataEntries.hasNext()) {
            out.println("     +---" + dataEntries.next());
            out.println();
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }

      } else if (mf.content().equals(ManifestContent.DELETES)) {
        ManifestReader<DeleteFile> deleteReader = ManifestFiles.readDeleteManifest(mf, table.io(), table.specs());
        try (CloseableIterator<ManifestEntry<DeleteFile>> deleteEntries = deleteReader.entries().iterator()) {
          while (deleteEntries.hasNext()) {
            out.println("     +---" + deleteEntries.next());
            out.println();
          }
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }
}
