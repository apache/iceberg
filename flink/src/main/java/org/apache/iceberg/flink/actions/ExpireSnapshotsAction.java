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

package org.apache.iceberg.flink.actions;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Expressions;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.BaseExpireSnapshotsActionResult;
import org.apache.iceberg.actions.ExpireSnapshots;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

public class ExpireSnapshotsAction
    extends BaseFlinkAction<ExpireSnapshots, ExpireSnapshots.Result> implements ExpireSnapshots {
  private static final Logger LOG = LoggerFactory.getLogger(ExpireSnapshotsAction.class);

  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  private final Table table;
  private final TableOperations ops;
  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private final Set<Long> expiredSnapshotIds = Sets.newHashSet();
  private Long expireOlderThanValue = null;
  private Integer retainLastValue = null;
  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = null;
  private Iterator<Row> expiredFiles = null;

  public ExpireSnapshotsAction(StreamExecutionEnvironment env, Table table) {
    super(env);
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(table.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot expire snapshots: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected ExpireSnapshots self() {
    return this;
  }

  @Override
  public ExpireSnapshots expireSnapshotId(long snapshotId) {
    this.expiredSnapshotIds.add(snapshotId);
    return this;
  }

  @Override
  public ExpireSnapshots expireOlderThan(long timestampMillis) {
    this.expireOlderThanValue = timestampMillis;
    return this;
  }

  @Override
  public ExpireSnapshots retainLast(int numSnapshots) {
    Preconditions.checkArgument(1 <= numSnapshots,
        "Number of snapshots to retain must be at least 1, cannot be: %s", numSnapshots);
    this.retainLastValue = numSnapshots;
    return this;
  }

  @Override
  public ExpireSnapshots deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public ExpireSnapshots executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public Result execute() {
    return deleteFiles(expire());
  }

  private org.apache.flink.table.api.Table buildValidFileTable(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata, table.io());

    org.apache.flink.table.api.Table dataFileTable =  buildValidDataFileTable(staticTable, ops).select(
        Expressions.$("file_path"), Expressions.lit(DATA_FILE).as("file_type"));

    org.apache.flink.table.api.Table manifestTable =  buildManifestFileTable(staticTable, ops).select(
        Expressions.$("file_path"), Expressions.lit(MANIFEST).as("file_type"));

    org.apache.flink.table.api.Table manifestListTable =  buildManifestListTable(staticTable, ops).select(
        Expressions.$("file_path"), Expressions.lit(MANIFEST_LIST).as("file_type"));

    return dataFileTable.unionAll(manifestTable).unionAll(manifestListTable).distinct();
  }

  /**
   * Expires snapshots and commits the changes to the table, returning a Dataset of files to delete.
   * <p>
   * This does not delete data files. To delete data files, run {@link #execute()}.
   * <p>
   * This may be called before or after {@link #execute()} is called to return the expired file list.
   *
   * @return a Dataset of files that are no longer referenced by the table
   */
  public Iterator<Row> expire() {
    if (expiredFiles == null) {
      // fetch metadata before expiration
      org.apache.flink.table.api.Table originalFiles = buildValidFileTable(ops.current());

      // perform expiration
      org.apache.iceberg.ExpireSnapshots expireSnapshots = table.expireSnapshots().cleanExpiredFiles(false);
      for (long id : expiredSnapshotIds) {
        expireSnapshots = expireSnapshots.expireSnapshotId(id);
      }

      if (expireOlderThanValue != null) {
        expireSnapshots = expireSnapshots.expireOlderThan(expireOlderThanValue);
      }

      if (retainLastValue != null) {
        expireSnapshots = expireSnapshots.retainLast(retainLastValue);
      }

      expireSnapshots.commit();

      // fetch metadata after expiration
      org.apache.flink.table.api.Table validFiles = buildValidFileTable(ops.refresh());

      // determine expired files
      EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
      TableEnvironment batchTableEnv = TableEnvironment.create(settings);

      org.apache.flink.table.api.Table originalFileTable = batchTableEnv.fromValues(
          (Iterable<Row>) () -> originalFiles.select(Expressions.$("*")).execute().collect());

      org.apache.flink.table.api.Table validFileTable = batchTableEnv.fromValues(
          (Iterable<Row>) () -> validFiles.select(Expressions.$("*")).execute().collect());

      expiredFiles = originalFileTable.minus(validFileTable).execute().collect();
    }
    return expiredFiles;
  }

  /**
   * Deletes files passed to it based on their type.
   *
   * @param expired an Iterator of Spark Rows of the structure (path: String, type: String)
   * @return Statistics on which files were deleted
   */
  private BaseExpireSnapshotsActionResult deleteFiles(Iterator<Row> expired) {
    AtomicLong dataFileCount = new AtomicLong(0L);
    AtomicLong manifestCount = new AtomicLong(0L);
    AtomicLong manifestListCount = new AtomicLong(0L);

    Tasks.foreach(expired)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(deleteExecutorService)
        .onFailure((fileInfo, exc) -> {
          String file = fileInfo.getFieldAs(0);
          String type = fileInfo.getFieldAs(1);
          LOG.warn("Delete failed for {}: {}", type, file, exc);
        })
        .run(fileInfo -> {
          String file = fileInfo.getFieldAs(0);
          String type = fileInfo.getFieldAs(1);
          deleteFunc.accept(file);
          switch (type) {
            case DATA_FILE:
              dataFileCount.incrementAndGet();
              LOG.trace("Deleted Data File: {}", file);
              break;
            case MANIFEST:
              manifestCount.incrementAndGet();
              LOG.debug("Deleted Manifest: {}", file);
              break;
            case MANIFEST_LIST:
              manifestListCount.incrementAndGet();
              LOG.debug("Deleted Manifest List: {}", file);
              break;
          }
        });

    LOG.info("Deleted {} total files", dataFileCount.get() + manifestCount.get() + manifestListCount.get());
    return new BaseExpireSnapshotsActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get());
  }
}
