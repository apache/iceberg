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
package org.apache.iceberg.flink.sink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.flink.annotation.Internal;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.flink.maintenance.operator.DVPosition;
import org.apache.iceberg.flink.maintenance.operator.DeletionVectorHelper;
import org.apache.iceberg.flink.maintenance.operator.EqualityKeyReader;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.ContentFileUtil;

/**
 * Reports where the live rows of a committed data file are, which is how the primary key index
 * learns about data it did not write itself.
 *
 * <p>Only the equality fields and the row position are read, and rows already covered by a deletion
 * vector are skipped since they are not live.
 */
@Internal
class PkIndexFileReader {

  private final Table table;
  private final EqualityKeyReader keyReader;
  private final DeletionVectorHelper deletionVectorHelper;

  PkIndexFileReader(Table table, Set<Integer> equalityFieldIds) {
    this.table = table;
    this.keyReader = new EqualityKeyReader(table.io(), table.schema(), equalityFieldIds);
    this.deletionVectorHelper = new DeletionVectorHelper(table);
  }

  /**
   * Reports every live row of one data file.
   *
   * @param task the data file and the delete files that apply to it
   * @param out receives one entry per live row
   * @return the number of rows reported
   */
  long read(PkIndexReadTask task, Consumer<PkIndexEntry> out) {
    DataFile file = task.file();
    PositionDeleteIndex deleted = deletedPositions(file, task.deletes());
    byte[] partition =
        keyReader.encodePartition(file.partition(), spec(file.specId()).partitionType());
    try {
      return keyReader.readLiveRows(
          file,
          deleted,
          (key, position) ->
              out.accept(
                  new PkIndexEntry(
                      key,
                      new DVPosition(
                          file.location(),
                          position,
                          file.specId(),
                          partition,
                          PkIndexEntry.UNKNOWN_SEQUENCE))));
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read data file for the primary key index: " + file.location(), e);
    }
  }

  private PositionDeleteIndex deletedPositions(DataFile file, DeleteFile[] deletes) {
    List<DeleteFile> attached = Lists.newArrayList();
    for (DeleteFile deleteFile : deletes) {
      if (ContentFileUtil.isDV(deleteFile)) {
        attached.add(deleteFile);
      } else {
        // The sink refuses to start when the branch holds equality deletes, and a V3 table has no
        // standalone position deletes, so anything else means the table is not in the shape this
        // write path assumes.
        throw new IllegalStateException(
            String.format(
                "Cannot build the primary key index of table %s: data file %s has a %s delete file "
                    + "(%s) attached, but only deletion vectors are supported.",
                table.name(),
                file.location(),
                deleteFile.content() == FileContent.EQUALITY_DELETES ? "equality" : "position",
                deleteFile.location()));
      }
    }

    return deletionVectorHelper.load(file.location(), attached);
  }

  private PartitionSpec spec(int specId) {
    PartitionSpec spec = table.specs().get(specId);
    if (spec == null) {
      table.refresh();
      spec = table.specs().get(specId);
    }

    Preconditions.checkState(
        spec != null, "Cannot find partition spec %s in table %s", specId, table.name());
    return spec;
  }
}
