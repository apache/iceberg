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

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.actions.RemoveFiles;
import org.apache.iceberg.actions.RemoveFilesActionResult;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that performs the same operation as {@link RemoveFiles} but uses Spark
 * to determine the files that needs to be deleted. The action uses metadata tables to
 * find the files to be deleted.
 * Deletes are performed locally after retrieving the results from the Spark executors.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class BaseRemoveFilesSparkAction
    extends BaseSparkAction<RemoveFiles, RemoveFiles.Result> implements RemoveFiles {
  private static final Logger LOG = LoggerFactory.getLogger(BaseRemoveFilesSparkAction.class);

  private static final String DATA_FILE = "Data File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";

  private static final String STREAM_RESULTS = "stream-results";

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = null;

  private final Table table;
  private final TableOperations ops;
  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      ops.io().deleteFile(file);
    }
  };

  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;

  public BaseRemoveFilesSparkAction(SparkSession spark, Table table) {
    super(spark);
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
  }

  @Override
  protected RemoveFiles self() {
    return this;
  }

  @Override
  public Result execute() {
    JobGroupInfo info = newJobGroupInfo("DROP-TABLE", "DROP-TABLE");
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    boolean streamResults = PropertyUtil.propertyAsBoolean(options(), STREAM_RESULTS, false);
    Dataset<Row> validFileDataset = buildValidFileDF(ops.current());
    RemoveFilesActionResult result;
    if (streamResults) {
      result = deleteFiles(validFileDataset.toLocalIterator());
    } else {
      result = deleteFiles(validFileDataset.collectAsList().iterator());
    }
    return result;
  }

  private Dataset<Row> appendTypeString(Dataset<Row> ds, String type) {
    return ds.select(new Column("file_path"), functions.lit(type).as("file_type"));
  }

  private Dataset<Row> buildValidFileDF(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata, this.table.io());
    return appendTypeString(buildValidDataFileDF(staticTable), DATA_FILE)
        .union(appendTypeString(buildManifestFileDF(staticTable), MANIFEST))
        .union(appendTypeString(buildManifestListDF(staticTable), MANIFEST_LIST));
  }

  /**
   * Deletes files passed to it based on their type.
   *
   * @param deleted an Iterator of Spark Rows of the structure (path: String, type: String)
   * @return Statistics on which files were deleted
   */
  private RemoveFilesActionResult deleteFiles(Iterator<Row> deleted) {
    AtomicLong dataFileCount = new AtomicLong(0L);
    AtomicLong manifestCount = new AtomicLong(0L);
    AtomicLong manifestListCount = new AtomicLong(0L);

    Tasks.foreach(deleted)
        .retry(3).stopRetryOn(NotFoundException.class).suppressFailureWhenFinished()
        .executeWith(deleteExecutorService)
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
    return new RemoveFilesActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get());
  }

  @Override
  public RemoveFiles deleteWith(Consumer<String> deleteFn) {
    this.deleteFunc = deleteFn;
    return this;

  }

  @Override
  public RemoveFiles executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }
}
