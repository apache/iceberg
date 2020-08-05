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
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
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
  private static final String DATAFILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFESTLIST = "Manifest List";
  private static final String OTHER = "Other";

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
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
    this.base = ops.current();
    this.localExpireSnapshots = table.expireSnapshots().cleanExpiredFiles(false);
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

  private Dataset<Row> appendTypeString(Dataset<Row> ds, String type) {
    return ds.select(new Column("file_path"), functions.lit(type).as("DataFile"));
  }

  private Dataset<Row> getValidFileDF() {
    return appendTypeString(buildValidDataFileDF(spark), DATAFILE)
        .union(appendTypeString(buildManifestFileDF(spark), MANIFEST))
        .union(appendTypeString(buildManifestListDF(spark, table), MANIFESTLIST))
        .union(appendTypeString(buildOtherMetadataFileDF(spark, ops), OTHER));
  }

  private Set<String> getFilesOfType(List<Row> files, String type) {
    return files.stream()
        .filter(row -> row.getString(1).equals(type))
        .map(row -> row.getString(0))
        .collect(Collectors.toSet());
  }

  @Override
  public ExpireSnapshotActionResult execute() {

    Dataset<Row> originalFiles = getValidFileDF().persist();
    originalFiles.count(); // Trigger Persist

    localExpireSnapshots.commit();

    Dataset<Row> validFiles = getValidFileDF();

    List<Row> filesToDelete = originalFiles.except(validFiles).collectAsList();

    LOG.warn("Deleting {} files", filesToDelete.size());
    return new ExpireSnapshotActionResult(
        deleteFiles(getFilesOfType(filesToDelete, DATAFILE), DATAFILE),
        deleteFiles(getFilesOfType(filesToDelete, MANIFEST), MANIFEST),
        deleteFiles(getFilesOfType(filesToDelete, MANIFESTLIST), MANIFESTLIST),
        deleteFiles(getFilesOfType(filesToDelete, OTHER), OTHER));
  }

  private Long deleteFiles(Set<String> paths, String fileType) {
    LOG.warn("{}s to delete: {}", fileType, Joiner.on(", ").join(paths));
    AtomicReference<Long> deleteCount = new AtomicReference<>(0L);

    Tasks.foreach(paths)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(ThreadPools.getWorkerPool())
        .onFailure((manifest, exc) -> LOG.warn("Delete failed for {}: {}", fileType, manifest, exc))
        .run(file -> {
          deleteFunc.accept(file);
          deleteCount.updateAndGet(v -> v + 1);
        });
    return deleteCount.get();
  }

}
