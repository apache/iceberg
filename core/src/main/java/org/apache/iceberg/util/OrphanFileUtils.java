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
package org.apache.iceberg.util;

import java.util.Set;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.FileInfo;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helpers for orphan-file cleanup actions.
 *
 * <p>The key concern is that two independent Iceberg tables may point at the same underlying
 * location. When orphan-file cleanup runs for one table it lists the whole location and deletes
 * everything not referenced by that single table's metadata &mdash; including the other table's
 * files, which corrupts it. This is especially easy to hit when a {@code SHOW CREATE TABLE} DDL is
 * copied to a test table while forgetting to change the {@code LOCATION}.
 *
 * <p>Every Iceberg {@code metadata.json} carries the creating table's immutable {@code table-uuid}.
 * By comparing the {@code table-uuid} of the metadata files found in a table's metadata directory
 * we can detect whether another table shares the location, before any file is deleted.
 */
public class OrphanFileUtils {
  private static final Logger LOG = LoggerFactory.getLogger(OrphanFileUtils.class);

  private static boolean isMetadataJson(String name) {
    return name.endsWith(".metadata.json") || name.endsWith(".metadata.json.gz");
  }

  private OrphanFileUtils() {}

  /**
   * Returns {@code true} if another Iceberg table appears to share the same metadata location as
   * the given table.
   *
   * <p>Lists the table's metadata directory via its own (storage-agnostic) {@link FileIO} and, for
   * every {@code metadata.json} not in this table's own history (current + {@code
   * previousFiles()}), reads its {@code table-uuid}:
   *
   * <ul>
   *   <li>same uuid &rarr; older version of this table, ignored;
   *   <li>different uuid &rarr; another table shares the location, returns {@code true};
   *   <li>uuid missing/unreadable (legacy, corrupt, or compressed) &rarr; treated as a conflict,
   *       returns {@code true} (fail-safe).
   * </ul>
   *
   * <p>Requires {@link SupportsPrefixOperations} for FileIO; otherwise a {@link
   * ValidationException} is thrown. The caller may choose to skip the check when the storage
   * backend is known to be used by a single table. On the no-conflict path only a single listing is
   * done and no metadata file is read.
   *
   * @param table the table whose location is about to be cleaned
   * @throws ValidationException if the table's {@code FileIO} does not support prefix operations
   */
  public static boolean hasOtherTableInLocation(Table table) {
    TableOperations ops = ((HasTableOperations) table).operations();
    TableMetadata current = ops.current();

    String myUuid = current.uuid();
    Set<String> myMetadataFiles = Sets.newHashSet();
    myMetadataFiles.add(new Path(current.metadataFileLocation()).getName());
    for (TableMetadata.MetadataLogEntry entry : current.previousFiles()) {
      myMetadataFiles.add(new Path(entry.file()).getName());
    }

    Path metadataDir = new Path(current.metadataFileLocation()).getParent();

    FileIO io = table.io();
    if (!(io instanceof SupportsPrefixOperations)) {
      throw new ValidationException(
          "Cannot detect location conflicts: the table's FileIO (%s) does not support prefix "
              + "operations, which are required to inspect the metadata directory '%s'. Use a FileIO "
              + "that implements SupportsPrefixOperations, or let the caller skip the check if the "
              + "storage backend is known to be used by a single table.",
          io.getClass().getName(), metadataDir);
    }

    String prefix = metadataDir.toString();
    if (!prefix.endsWith("/")) {
      prefix = prefix + "/";
    }

    SupportsPrefixOperations prefixIo = (SupportsPrefixOperations) io;
    for (FileInfo info : prefixIo.listPrefix(prefix)) {
      String name = new Path(info.location()).getName();
      if (!isMetadataJson(name) || myMetadataFiles.contains(name)) {
        continue;
      }

      String otherUuid = readTableUuid(table, info.location());
      if (otherUuid == null || !otherUuid.equals(myUuid)) {
        LOG.warn(
            "Another table (uuid={}) belonging to metadata file {} shares the metadata location with this table (uuid={}); "
                + "treating the location as conflicting",
            otherUuid,
            info.location(),
            myUuid);
        return true;
      }
    }

    return false;
  }

  /**
   * Reads the {@code table-uuid} from a metadata file, transparently handling gzip-compressed
   * metadata ({@code .metadata.json.gz}). Returns {@code null} if the uuid cannot be determined
   * (legacy file without a uuid, corrupt file, or any read failure).
   */
  @VisibleForTesting
  static String readTableUuid(Table table, String metadataLocation) {
    try {
      return TableMetadataParser.read(table.io(), metadataLocation).uuid();
    } catch (Exception e) {
      LOG.warn("Failed to read table-uuid from {}; treating as unreadable", metadataLocation, e);
      return null;
    }
  }
}
