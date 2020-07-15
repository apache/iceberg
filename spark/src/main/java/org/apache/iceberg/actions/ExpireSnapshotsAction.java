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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestEntry;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.ExpireSnapshotUtil;
import org.apache.iceberg.util.ExpireSnapshotUtil.ManifestExpirationChanges;
import org.apache.iceberg.util.ExpireSnapshotUtil.SnapshotExpirationChanges;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotsAction extends BaseAction<ExpireSnapshotResults> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

  private final Table table;
  private final TableOperations ops;
  private final ExpireSnapshots localExpireSnapshots;
  private final TableMetadata base;
  private SparkSession session;

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };
  private Consumer<String> deleteFunc = defaultDelete;


  ExpireSnapshotsAction(SparkSession session, Table table) {
    this.session = session;
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
    this.base = ops.current();
    this.localExpireSnapshots = table.expireSnapshots().cleanUpFiles(false);
  }

  public ExpireSnapshotsAction expireSnapshotId(long expireSnapshotId) {
    localExpireSnapshots.expireSnapshotId(expireSnapshotId);
    return this;
  }

  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    localExpireSnapshots.expireOlderThan(timestampMillis);
    return this;
  }

  public ExpireSnapshotsAction retainLast(int numSnapshots) {
    localExpireSnapshots.retainLast(numSnapshots);
    return this;
  }

  public ExpireSnapshotsAction deleteWith(Consumer<String> newDeleteFunc) {
    deleteFunc = newDeleteFunc;
    return this;
  }


  @Override
  protected Table table() {
    return table;
  }

  /**
   * Execute is a synonym for commit in this implementation. Calling either commit or execute will
   * launch the Spark equivalent of RemoveSnapshots.
   *
   * @return nothing
   */
  @Override
  public ExpireSnapshotResults execute() {
    localExpireSnapshots.commit();

    //Locally determine which snapshots have been expired
    SnapshotExpirationChanges expiredSnapshotChanges =
        ExpireSnapshotUtil.getExpiredSnapshots(ops, base);

    //Locally determine which manifests will need to be scanned, reverted, deleted
    ManifestExpirationChanges manifestExpirationChanges =
        ExpireSnapshotUtil
            .determineManifestChangesFromSnapshotExpiration(
                expiredSnapshotChanges.getCurrentSnapshots(),
                expiredSnapshotChanges.getValidSnapshotIds(),
                expiredSnapshotChanges.getExpiredSnapshotIds(),
                base, ops);

    FileIO io = SparkUtil.serializableFileIO(table);

    //Going the RDD Route because our reader functions all work with full Manifest Files
    JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(session.sparkContext());

    JavaRDD<ManifestFile> manifestsToScan =
        javaSparkContext
            .parallelize(new LinkedList<>(manifestExpirationChanges.getManifestsToScan()));

    JavaRDD<ManifestFile> manifestsToRevert =
        javaSparkContext
            .parallelize(new LinkedList<>(manifestExpirationChanges.getManifestsToRevert()));

    FileIO serializableIO = SparkUtil.serializableFileIO(table);

    Broadcast<Map<Integer, PartitionSpec>> broadcastedSpecLookup =
        javaSparkContext.broadcast(ops.current().specsById());

    Broadcast<Set<Long>> broadcastValidIDs =
        javaSparkContext.broadcast(expiredSnapshotChanges.getValidSnapshotIds());

    JavaRDD<String> filesToDeleteFromScan = manifestsToScan.mapPartitions(manifests -> {
      Map<Integer, PartitionSpec> specLookup = broadcastedSpecLookup.getValue();
      Set<Long> validIds = broadcastValidIDs.getValue();
      Set<String> filesToDelete = new HashSet<>();
      Tasks.foreach(ImmutableList.copyOf(manifests))
          .retry(3).suppressFailureWhenFinished()
          .executeWith(ThreadPools.getWorkerPool())
          .onFailure((item, exc) -> LOG
              .warn("Failed to get deleted files: this may cause orphaned data files", exc))
          .run(manifest -> {
            // the manifest has deletes, scan it to find files to delete
            try (ManifestReader<?> reader = ManifestFiles
                .open(manifest, serializableIO, specLookup)) {
              for (ManifestEntry<?> entry : reader.entries()) {
                // if the snapshot ID of the DELETE entry is no longer valid, the data can be deleted
                if (entry.status() == ManifestEntry.Status.DELETED &&
                    !validIds.contains(entry.snapshotId())) {
                  // use toString to ensure the path will not change (Utf8 is reused)
                  filesToDelete.add(entry.file().path().toString());
                }
              }
            } catch (IOException e) {
              throw new UncheckedIOException(
                  String.format("Failed to read manifest file: %s", manifest), e);
            }
          });
      return filesToDelete.iterator();
    });

    JavaRDD<String> filesToDeleteFromRevert = manifestsToRevert.mapPartitions(manifests -> {
      Map<Integer, PartitionSpec> specLookup = broadcastedSpecLookup.getValue();
      Set<String> filesToDelete = new HashSet<>();
      Tasks.foreach(ImmutableList.copyOf(manifests))
          .retry(3).suppressFailureWhenFinished()
          .executeWith(ThreadPools.getWorkerPool())
          .onFailure((item, exc) -> LOG
              .warn("Failed to get deleted files: this may cause orphaned data files", exc))
          .run(manifest -> {
            // the manifest has deletes, scan it to find files to delete
            try (ManifestReader<?> reader = ManifestFiles
                .open(manifest, serializableIO, specLookup)) {
              for (ManifestEntry<?> entry : reader.entries()) {
                // delete any ADDED file from manifests that were reverted
                if (entry.status() == ManifestEntry.Status.ADDED) {
                  // use toString to ensure the path will not change (Utf8 is reused)
                  filesToDelete.add(entry.file().path().toString());
                }
              }
            } catch (IOException e) {
              throw new UncheckedIOException(
                  String.format("Failed to read manifest file: %s", manifest), e);
            }
          });
      return filesToDelete.iterator();
    });

    Set<String> dataFilesToDelete = new HashSet<>(
        filesToDeleteFromRevert.union(filesToDeleteFromScan).collect());

    LOG.warn("Deleting {} data files", dataFilesToDelete.size());

    return new ExpireSnapshotResults(
        deleteManifestFiles(manifestExpirationChanges.getManifestsToDelete()),
        deleteManifestLists(manifestExpirationChanges.getManifestListsToDelete()),
        deleteDataFiles(dataFilesToDelete));
  }

  private Long deleteManifestFiles(Set<String> manifestsToDelete) {
    LOG.warn("Manifests to delete: {}", Joiner.on(", ").join(manifestsToDelete));
    AtomicReference<Long> deleteCount = new AtomicReference<>(0L);

    Tasks.foreach(manifestsToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for manifest: {}", manifest, exc))
        .run(file -> {
          deleteFunc.accept(file);
          deleteCount.updateAndGet(v -> v + 1);
        });
    return deleteCount.get();
  }

  private Long deleteManifestLists(Set<String> manifestListsToDelete) {
    LOG.warn("Manifests Lists to delete: {}", Joiner.on(", ").join(manifestListsToDelete));
    AtomicReference<Long> deleteCount = new AtomicReference<>(0L);

    Tasks.foreach(manifestListsToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((list, exc) -> LOG.warn("Delete failed for manifest list: {}", list, exc))
        .run(file -> {
          deleteFunc.accept(file);
          deleteCount.updateAndGet(v -> v + 1);
        });
    return deleteCount.get();
  }

  private Long deleteDataFiles(Set<String> dataFilesToDelete) {
    LOG.warn("Data Files to delete: {}", Joiner.on(", ").join(dataFilesToDelete));
    AtomicReference<Long> deleteCount = new AtomicReference<>(0L);

    Tasks.foreach(dataFilesToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Delete failed for data file: {}", file, exc))
        .run(file -> {
          deleteFunc.accept(file);
          deleteCount.updateAndGet(v -> v + 1);
        });
    return deleteCount.get();
  }
}
