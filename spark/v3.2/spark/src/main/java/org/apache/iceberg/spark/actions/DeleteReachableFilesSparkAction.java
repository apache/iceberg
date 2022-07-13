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
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.actions.BaseDeleteReachableFilesActionResult;
import org.apache.iceberg.actions.DeleteReachableFiles;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

/**
 * An implementation of {@link DeleteReachableFiles} that uses metadata tables in Spark
 * to determine which files should be deleted.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class DeleteReachableFilesSparkAction
    extends BaseSparkAction<DeleteReachableFilesSparkAction> implements DeleteReachableFiles {

  public static final String STREAM_RESULTS = "stream-results";
  public static final boolean STREAM_RESULTS_DEFAULT = false;

  private static final Logger LOG = LoggerFactory.getLogger(DeleteReachableFilesSparkAction.class);

  private final String metadataFileLocation;
  private final Consumer<String> defaultDelete = new Consumer<String>() {
    @Override
    public void accept(String file) {
      io.deleteFile(file);
    }
  };

  private Consumer<String> deleteFunc = defaultDelete;
  private ExecutorService deleteExecutorService = null;
  private FileIO io = new HadoopFileIO(spark().sessionState().newHadoopConf());

  DeleteReachableFilesSparkAction(SparkSession spark, String metadataFileLocation) {
    super(spark);
    this.metadataFileLocation = metadataFileLocation;
  }

  @Override
  protected DeleteReachableFilesSparkAction self() {
    return this;
  }

  @Override
  public DeleteReachableFilesSparkAction io(FileIO fileIO) {
    this.io = fileIO;
    return this;
  }

  @Override
  public DeleteReachableFilesSparkAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public DeleteReachableFilesSparkAction executeDeleteWith(ExecutorService executorService) {
    this.deleteExecutorService = executorService;
    return this;
  }

  @Override
  public Result execute() {
    Preconditions.checkArgument(io != null, "File IO cannot be null");
    String jobDesc = String.format("Deleting files reachable from %s", metadataFileLocation);
    JobGroupInfo info = newJobGroupInfo("DELETE-REACHABLE-FILES", jobDesc);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    TableMetadata metadata = TableMetadataParser.read(io, metadataFileLocation);

    ValidationException.check(
        PropertyUtil.propertyAsBoolean(metadata.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot delete files: GC is disabled (deleting files may corrupt other tables)");

    Dataset<Row> reachableFileDF = buildReachableFileDF(metadata).distinct();

    boolean streamResults = PropertyUtil.propertyAsBoolean(options(), STREAM_RESULTS, STREAM_RESULTS_DEFAULT);
    if (streamResults) {
      return deleteFiles(reachableFileDF.toLocalIterator());
    } else {
      return deleteFiles(reachableFileDF.collectAsList().iterator());
    }
  }

  private Dataset<Row> buildReachableFileDF(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata, io);
    return withFileType(buildValidContentFileDF(staticTable), CONTENT_FILE)
        .union(withFileType(buildManifestFileDF(staticTable), MANIFEST))
        .union(withFileType(buildManifestListDF(staticTable), MANIFEST_LIST))
        .union(withFileType(buildAllReachableOtherMetadataFileDF(staticTable), OTHERS));
  }

  /**
   * Deletes files passed to it.
   *
   * @param deleted an Iterator of Spark Rows of the structure (path: String, type: String)
   * @return Statistics on which files were deleted
   */
  private BaseDeleteReachableFilesActionResult deleteFiles(Iterator<Row> deleted) {
    AtomicLong dataFileCount = new AtomicLong(0L);
    AtomicLong manifestCount = new AtomicLong(0L);
    AtomicLong manifestListCount = new AtomicLong(0L);
    AtomicLong otherFilesCount = new AtomicLong(0L);

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
            case CONTENT_FILE:
              dataFileCount.incrementAndGet();
              LOG.trace("Deleted Content File: {}", file);
              break;
            case MANIFEST:
              manifestCount.incrementAndGet();
              LOG.debug("Deleted Manifest: {}", file);
              break;
            case MANIFEST_LIST:
              manifestListCount.incrementAndGet();
              LOG.debug("Deleted Manifest List: {}", file);
              break;
            case OTHERS:
              otherFilesCount.incrementAndGet();
              LOG.debug("Others: {}", file);
              break;
          }
        });

    long filesCount = dataFileCount.get() + manifestCount.get() + manifestListCount.get() + otherFilesCount.get();
    LOG.info("Total files removed: {}", filesCount);
    return new BaseDeleteReachableFilesActionResult(dataFileCount.get(), manifestCount.get(), manifestListCount.get(),
        otherFilesCount.get());
  }
}
