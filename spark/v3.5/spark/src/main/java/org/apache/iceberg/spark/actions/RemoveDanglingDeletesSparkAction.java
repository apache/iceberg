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
package org.apache.iceberg.spark.actions;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.actions.RemoveDanglingDeleteFiles;
import org.apache.iceberg.actions.RemoveDanglingDeleteFilesAction;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * An action that removes dangling delete files from the current snapshot. A delete file is dangling
 * if its deletes no longer applies to any live data files.
 */
class RemoveDanglingDeletesSparkAction
    extends BaseSnapshotUpdateSparkAction<RemoveDanglingDeletesSparkAction>
    implements RemoveDanglingDeleteFiles {

  private final Table table;
  private final RemoveDanglingDeleteFilesAction action;

  protected RemoveDanglingDeletesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.action = new RemoveDanglingDeleteFilesAction(table, this::findDanglingDeletes);
  }

  @Override
  protected RemoveDanglingDeletesSparkAction self() {
    return this;
  }

  public RemoveDanglingDeletesSparkAction toBranch(String targetBranch) {
    action.toBranch(targetBranch);
    return this;
  }

  @Override
  public Result execute() {
    commitSummary().forEach(action::set);
    String desc = String.format("Removing dangling delete files in %s", table.name());
    return withJobGroupInfo(newJobGroupInfo("REMOVE-DELETES", desc), action::execute);
  }

  private DeleteFileSet findDanglingDeletes(Snapshot snapshot) {
    Broadcast<Table> tableBroadcast =
        sparkContext().broadcast(SerializableTableWithSize.copyOf(table));

    JavaPairRDD<DeleteFileKey, Void> referencedDeletes =
        sparkContext()
            .parallelize(ImmutableList.of(snapshot.snapshotId()), 1)
            .flatMap(
                snapshotId -> {
                  TableScan scan = tableBroadcast.value().newScan().useSnapshot(snapshotId);
                  return new ClosingIterator<>(new DeleteFileKeyIterator(scan.planFiles()));
                })
            .mapToPair(key -> new Tuple2<>(key, (Void) null));

    List<ManifestFileBean> deleteManifests =
        snapshot.deleteManifests(table.io()).stream().map(ManifestFileBean::fromManifest).toList();
    JavaPairRDD<DeleteFileKey, DeleteFile> allDeletes =
        sparkContext()
            .parallelize(deleteManifests, deleteManifests.size())
            .flatMap(
                manifest -> {
                  ManifestReader<DeleteFile> reader =
                      ManifestFiles.readDeleteManifest(
                          manifest, tableBroadcast.value().io(), tableBroadcast.value().specs());
                  return new ClosingIterator<>(reader.iterator());
                })
            .mapToPair(file -> new Tuple2<>(new DeleteFileKey(file), file.copyWithoutStats()));

    DeleteFileSet danglingDeletes = DeleteFileSet.create();
    allDeletes
        .subtractByKey(referencedDeletes)
        .values()
        .toLocalIterator()
        .forEachRemaining(danglingDeletes::add);
    return danglingDeletes;
  }

  private static class DeleteFileKeyIterator implements CloseableIterator<DeleteFileKey> {
    private final Closeable closeable;
    private final Iterator<DeleteFileKey> iterator;

    DeleteFileKeyIterator(CloseableIterable<FileScanTask> tasks) {
      this.closeable = tasks;
      this.iterator =
          StreamSupport.stream(tasks.spliterator(), false)
              .flatMap(task -> task.deletes().stream())
              .map(DeleteFileKey::new)
              .distinct()
              .iterator();
    }

    @Override
    public void close() throws IOException {
      closeable.close();
    }

    @Override
    public boolean hasNext() {
      return iterator.hasNext();
    }

    @Override
    public DeleteFileKey next() {
      return iterator.next();
    }
  }

  private static final class DeleteFileKey implements Serializable {
    private final String location;
    private final Long contentOffset;
    private final Long contentSizeInBytes;

    DeleteFileKey(DeleteFile file) {
      this.location = file.location();
      this.contentOffset = file.contentOffset();
      this.contentSizeInBytes = file.contentSizeInBytes();
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }

      if (other instanceof DeleteFileKey that) {
        return Objects.equals(location, that.location)
            && Objects.equals(contentOffset, that.contentOffset)
            && Objects.equals(contentSizeInBytes, that.contentSizeInBytes);
      }

      return false;
    }

    @Override
    public int hashCode() {
      return Objects.hash(location, contentOffset, contentSizeInBytes);
    }
  }
}
