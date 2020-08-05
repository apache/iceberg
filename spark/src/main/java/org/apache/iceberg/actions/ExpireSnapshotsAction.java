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

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpireSnapshotsAction extends BaseAction<ExpireSnapshotsActionResult> {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = MoreExecutors.newDirectExecutorService();

  private final SparkSession spark;
  private final Table table;
  private final TableOperations ops;
  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private Long expireSnapshotIdValue = null;
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;

  ExpireSnapshotsAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  protected Table table() {
    return table;
  }

  /**
   * An executor service used when deleting files. Only used during the local delete phase of this Spark action
   * @param executorService the service to use
   * @return this for method chaining
   */
  public ExpireSnapshotsAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  public ExpireSnapshotsAction expireSnapshotId(long expireSnapshotId) {
    this.expireSnapshotIdValue = expireSnapshotId;
    return this;
  }

  public ExpireSnapshotsAction expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  public ExpireSnapshotsAction retainLast(int numSnapshots) {
    this.retainLastValue = numSnapshots;
    return this;
  }

  public ExpireSnapshotsAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshotsActionResult execute() {
    //Metadata before Expiration
    Dataset<Row> originalFiles = buildValidFileDF().persist();
    originalFiles.count(); // Action to trigger persist

    //Perform Expiration
    ExpireSnapshots expireSnaps = table.expireSnapshots().cleanExpiredFiles(false);
    if (expireSnapshotIdValue != null) {
      expireSnaps = expireSnaps.expireSnapshotId(expireSnapshotIdValue);
    }
    if (expireOlderThanValue != null) {
      expireSnaps = expireSnaps.expireOlderThan(expireOlderThanValue);
    }
    if (retainLastValue != null) {
      expireSnaps = expireSnaps.retainLast(retainLastValue);
    }
    expireSnaps.commit();

    // Metadata after Expiration
    Dataset<Row> validFiles = buildValidFileDF();
    Dataset<Row> filesToDelete = originalFiles.except(validFiles);

    ExpireSnapshotsActionResult result =  deleteFiles(filesToDelete.toLocalIterator());
    originalFiles.unpersist();
    return result;
  }

  private Dataset<Row> appendTypeString(Dataset<Row> ds, String type) {
    return ds.select(new Column("file_path"), functions.lit(type).as("file_type"));
  }

  private Dataset<Row> buildValidFileDF() {
    return appendTypeString(buildValidDataFileDF(spark), DATA_FILE)
        .union(appendTypeString(buildManifestFileDF(spark), MANIFEST))
        .union(appendTypeString(buildManifestListDF(spark, table), MANIFEST_LIST));
  }

  private ExpireSnapshotsActionResult deleteFiles(Iterator<Row> paths) {
    AtomicLong dataFileCount = new AtomicLong(0L);
    AtomicLong manifestCount = new AtomicLong(0L);
    AtomicLong manifestListCount = new AtomicLong(0L);

    Tasks.foreach(paths)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(deleteExecutorService)
        .onFailure((fileInfo, exc) ->
            LOG.warn("Delete failed for {}: {}", fileInfo.getString(1), fileInfo.getString(0), exc))
        .run(fileInfo -> {
          String file = fileInfo.getString(0);
          String type = fileInfo.getString(1);
          deleteFunc.accept(file);
          switch (type) {
            case DATA_FILE:
              dataFileCount.incrementAndGet();
              LOG.trace("Deleted Data File: {}", file);
              break;
            case MANIFEST:
              manifestCount.incrementAndGet();
              LOG.warn("Deleted Manifest: {}", file);
              break;
            case MANIFEST_LIST:
              manifestListCount.incrementAndGet();
              LOG.warn("Deleted Manifest List: {}", file);
              break;
          }
        });
    LOG.warn("Deleted {} total files", dataFileCount.get() + manifestCount.get() + manifestListCount.get());
    return new ExpireSnapshotsActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get());
  }
}
