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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestExpirationManager;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.iceberg.util.ExpireSnapshotUtil;
import org.apache.iceberg.util.ExpireSnapshotUtil.ManifestExpirationChanges;
import org.apache.iceberg.util.ExpireSnapshotUtil.SnapshotExpirationChanges;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotsAction extends BaseAction<ExpireSnapshotActionResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Table table;
  private final TableOperations ops;
  private final ExpireSnapshots localExpireSnapshots;
  private final TableMetadata base;

  private int numTasks;

  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };
  private Consumer<String> deleteFunc = defaultDelete;


  ExpireSnapshotsAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
    this.numTasks = sparkContext.defaultParallelism();
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
    this.base = ops.current();
    this.localExpireSnapshots = table.expireSnapshots().deleteExpiredFiles(false);
  }

  public ExpireSnapshotsAction expireSnapshotId(long expireSnapshotId) {
    localExpireSnapshots.expireSnapshotId(expireSnapshotId);
    return this;
  }

  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    localExpireSnapshots.expireOlderThan(timestampMillis);
    return this;
  }

  /**
   * Sets the target number of Spark Tasks to generate from the collection of manifests to Scan. This value is
   * used as a guide and cannot generate more tasks than there are manifests to scan. We use this value for both
   * manifests we are scanning for reversions as well as deletions so the max number of tasks will be twice this
   * parameter.
   *
   * By Default will use the context's default parallelism.
   * @param numTasks target number of Spark tasks
   * @return this object for method chaining
   */
  public ExpireSnapshotsAction withParallelism(int numTasks) {
    this.numTasks = numTasks;
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

  @Override
  public ExpireSnapshotActionResult execute() {
    localExpireSnapshots.commit();

    TableMetadata currentMetadata = ops.refresh();

    //Locally determine which snapshots have been expired
    SnapshotExpirationChanges expiredSnapshotChanges =
        ExpireSnapshotUtil.getExpiredSnapshots(currentMetadata, base);

    //Locally determine which manifests will need to be scanned, reverted, deleted
    ManifestExpirationChanges manifestChanges =
        ExpireSnapshotUtil.determineManifestChangesFromSnapshotExpiration(
            expiredSnapshotChanges.validSnapshotIds(), expiredSnapshotChanges.expiredSnapshotIds(), currentMetadata,
            base, ops.io());

    JavaRDD<ManifestFile> manifestsToScan =
        sparkContext.parallelize(Lists.newLinkedList(manifestChanges.manifestsToScan()), numTasks);

    JavaRDD<ManifestFile> manifestsToRevert =
        sparkContext.parallelize(Lists.newLinkedList(manifestChanges.manifestsToRevert()), numTasks);

    Map<Integer, PartitionSpec> specLookup = ops.current().specsById();

    Broadcast<Set<Long>> broadcastValidIDs = sparkContext.broadcast(expiredSnapshotChanges.validSnapshotIds());

    Broadcast<FileIO> io = sparkContext.broadcast(SparkUtil.serializableFileIO(table));

    JavaRDD<String> filesToDeleteFromScan = manifestsToScan.mapPartitions(manifests -> {
      Set<Long> validIds = broadcastValidIDs.getValue();
      Set<String> filesToDelete = new HashSet<>();
      filesToDelete.addAll(ManifestExpirationManager
          .scanManifestsForAbandonedDeletedFiles(Sets.newHashSet(manifests), validIds, specLookup, io.getValue()));
      return filesToDelete.iterator();
    });

    JavaRDD<String> filesToDeleteFromRevert = manifestsToRevert.mapPartitions(manifests -> {
      Set<String> filesToDelete = new HashSet<>();
      filesToDelete.addAll(ManifestExpirationManager
          .scanManifestsForRevertingAddedFiles(Sets.newHashSet(manifests), specLookup, io.getValue()));
      return filesToDelete.iterator();
    });

    Set<String> dataFilesToDelete = new HashSet<>(filesToDeleteFromRevert.union(filesToDeleteFromScan).collect());

    LOG.warn("Deleting {} data files", dataFilesToDelete.size());

    return new ExpireSnapshotActionResult(
        deleteFiles(manifestChanges.manifestsToDelete(), "Manifest"),
        deleteFiles(manifestChanges.manifestListsToDelete(), "ManifestList"),
        deleteFiles(dataFilesToDelete, "Data File"));
  }

  private Long deleteFiles(Set<String> paths, String fileType) {
    LOG.warn("{}s to delete: {}", fileType, Joiner.on(", ").join(paths));
    AtomicReference<Long> deleteCount = new AtomicReference<>(0L);

    Tasks.foreach(paths)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for {}: {}", fileType, manifest, exc))
        .run(file -> {
          deleteFunc.accept(file);
          deleteCount.updateAndGet(v -> v + 1);
        });
    return deleteCount.get();
  }

}
