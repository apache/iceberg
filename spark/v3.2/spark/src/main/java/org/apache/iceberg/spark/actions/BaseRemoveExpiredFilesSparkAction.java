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

import java.io.File;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.actions.BaseRemoveExpiredFilesActionResult;
import org.apache.iceberg.actions.RemoveExpiredFiles;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BaseRemoveExpiredFilesSparkAction extends BaseSparkAction<RemoveExpiredFiles, RemoveExpiredFiles.Result>
    implements RemoveExpiredFiles {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRemoveExpiredFilesSparkAction.class);
  private static final ExecutorService DEFAULT_EXECUTOR_SERVICE = null;
  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  private final Table table;
  private ExecutorService executorService = DEFAULT_EXECUTOR_SERVICE;
  private String targetVersion;
  private Table targetTable;

  private AtomicLong dataFileCount = new AtomicLong(0L);
  private AtomicLong manifestCount = new AtomicLong(0L);
  private AtomicLong manifestListCount = new AtomicLong(0L);

  private final Consumer<String> deleteFunc = new Consumer<String>() {
    @Override
    public void accept(String file) {
      table.io().deleteFile(file);
    }
  };

  public BaseRemoveExpiredFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
  }

  @Override
  protected RemoveExpiredFiles self() {
    return this;
  }

  @Override
  public RemoveExpiredFiles executeWith(ExecutorService service) {
    this.executorService = service;
    return this;
  }

  @Override
  public RemoveExpiredFiles targetVersion(String tVersion) {
    Preconditions.checkArgument(tVersion != null && !tVersion.isEmpty(), "Target version file('%s') cannot be empty.",
        tVersion);

    String tVersionFile = tVersion;
    if (!tVersionFile.contains(File.separator)) {
      tVersionFile = ((HasTableOperations) table).operations().metadataFileLocation(tVersionFile);
    }

    Preconditions.checkArgument(fileExist(tVersionFile), "Version file('%s') doesn't exist.", tVersionFile);
    this.targetVersion = tVersionFile;
    return this;
  }

  @Override
  public RemoveExpiredFiles.Result execute() {
    JobGroupInfo info = newJobGroupInfo("REMOVE-EXPIRED-FILES", jobDesc());
    return withJobGroupInfo(info, this::doExecute);
  }

  private String jobDesc() {
    return String.format("Remove expired files in version '%s' of table %s.", targetVersion, table.name());
  }

  private RemoveExpiredFiles.Result doExecute() {
    targetTable = newStaticTable(targetVersion, table.io());

    deleteFiles(expiredFiles().collectAsList().iterator());

    return new BaseRemoveExpiredFilesActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get());
  }

  private Dataset<Row> expiredFiles() {
    Dataset<Row> originalFiles = buildValidFileDF(currentMetadata(table));
    Dataset<Row> validFiles = buildValidFileDF(currentMetadata(targetTable));
    return originalFiles.except(validFiles);
  }

  private TableMetadata currentMetadata(Table tbl) {
    return ((HasTableOperations) tbl).operations().current();
  }

  private Dataset<Row> buildValidFileDF(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata, this.table.io());
    return appendTypeString(buildValidDataFileDF(staticTable), DATA_FILE)
        .union(appendTypeString(buildManifestFileDF(staticTable), MANIFEST))
        .union(appendTypeString(buildManifestListDF(staticTable), MANIFEST_LIST));
  }

  private Dataset<Row> appendTypeString(Dataset<Row> ds, String type) {
    return ds.select(new Column("file_path"), functions.lit(type).as("file_type"));
  }

  private void deleteFiles(Iterator<Row> filesToDelete) {
    Tasks.foreach(filesToDelete)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(executorService)
        .onFailure((fileInfo, exc) -> {
          String file = fileInfo.getString(0);
          String type = fileInfo.getString(1);
          LOG.warn("Delete failed for {}: {}", type, file, exc);
        })
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
              LOG.debug("Deleted Manifest: {}", file);
              break;
            case MANIFEST_LIST:
              manifestListCount.incrementAndGet();
              LOG.debug("Deleted Manifest List: {}", file);
              break;
          }
        });

    LOG.info("Deleted {} total files", dataFileCount.get() + manifestCount.get() + manifestListCount.get());
  }

  private boolean fileExist(String path) {
    if (path == null || path.trim().isEmpty()) {
      return false;
    }
    return table.io().newInputFile(path).exists();
  }
}

