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

import static org.apache.iceberg.TableProperties.GC_ENABLED;
import static org.apache.iceberg.TableProperties.GC_ENABLED_DEFAULT;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.apache.iceberg.ReachableFileUtil;
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
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of {@link DeleteReachableFiles} that uses metadata tables in Spark to determine
 * which files should be deleted.
 */
@SuppressWarnings("UnnecessaryAnonymousClass")
public class BaseDeleteReachableFilesSparkAction
    extends BaseSparkAction<DeleteReachableFiles, DeleteReachableFiles.Result>
    implements DeleteReachableFiles {
  private static final Logger LOG =
      LoggerFactory.getLogger(BaseDeleteReachableFilesSparkAction.class);

  private static final String CONTENT_FILE = "Content File";
  private static final String MANIFEST = "Manifest";
  private static final String MANIFEST_LIST = "Manifest List";
  private static final String OTHERS = "Others";

  private static final String STREAM_RESULTS = "stream-results";

  // Creates an executor service that runs each task in the thread that invokes execute/submit.
  private static final ExecutorService DEFAULT_DELETE_EXECUTOR_SERVICE = null;

  private final TableMetadata tableMetadata;

  private final Consumer<String> defaultDelete =
      new Consumer<String>() {
        @Override
        public void accept(String file) {
          io.deleteFile(file);
        }
      };

  private Consumer<String> removeFunc = defaultDelete;
  private ExecutorService removeExecutorService = DEFAULT_DELETE_EXECUTOR_SERVICE;
  private FileIO io = new HadoopFileIO(spark().sessionState().newHadoopConf());

  public BaseDeleteReachableFilesSparkAction(SparkSession spark, String metadataLocation) {
    super(spark);
    this.tableMetadata = TableMetadataParser.read(io, metadataLocation);
    ValidationException.check(
        PropertyUtil.propertyAsBoolean(tableMetadata.properties(), GC_ENABLED, GC_ENABLED_DEFAULT),
        "Cannot remove files: GC is disabled (deleting files may corrupt other tables)");
  }

  @Override
  protected DeleteReachableFiles self() {
    return this;
  }

  @Override
  public DeleteReachableFiles io(FileIO fileIO) {
    this.io = fileIO;
    return this;
  }

  @Override
  public DeleteReachableFiles deleteWith(Consumer<String> deleteFunc) {
    this.removeFunc = deleteFunc;
    return this;
  }

  @Override
  public DeleteReachableFiles executeDeleteWith(ExecutorService executorService) {
    this.removeExecutorService = executorService;
    return this;
  }

  @Override
  public Result execute() {
    Preconditions.checkArgument(io != null, "File IO cannot be null");
    String msg =
        String.format("Removing files reachable from %s", tableMetadata.metadataFileLocation());
    JobGroupInfo info = newJobGroupInfo("REMOVE-FILES", msg);
    return withJobGroupInfo(info, this::doExecute);
  }

  private Result doExecute() {
    boolean streamResults = PropertyUtil.propertyAsBoolean(options(), STREAM_RESULTS, false);
    Dataset<Row> validFileDF = buildValidFileDF(tableMetadata).distinct();
    if (streamResults) {
      return deleteFiles(validFileDF.toLocalIterator());
    } else {
      return deleteFiles(validFileDF.collectAsList().iterator());
    }
  }

  private Dataset<Row> projectFilePathWithType(Dataset<Row> ds, String type) {
    return ds.select(functions.col("file_path"), functions.lit(type).as("file_type"));
  }

  private Dataset<Row> buildValidFileDF(TableMetadata metadata) {
    Table staticTable = newStaticTable(metadata, io);
    return projectFilePathWithType(buildValidContentFileDF(staticTable), CONTENT_FILE)
        .union(projectFilePathWithType(buildManifestFileDF(staticTable), MANIFEST))
        .union(projectFilePathWithType(buildManifestListDF(staticTable), MANIFEST_LIST))
        .union(projectFilePathWithType(buildOtherMetadataFileDF(staticTable), OTHERS));
  }

  @Override
  protected Dataset<Row> buildOtherMetadataFileDF(Table table) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.addAll(ReachableFileUtil.metadataFileLocations(table, true));
    otherMetadataFiles.add(ReachableFileUtil.versionHintLocation(table));
    otherMetadataFiles.addAll(ReachableFileUtil.statisticsFilesLocations(table));
    return spark().createDataset(otherMetadataFiles, Encoders.STRING()).toDF("file_path");
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
        .retry(3)
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .executeWith(removeExecutorService)
        .onFailure(
            (fileInfo, exc) -> {
              String file = fileInfo.getString(0);
              String type = fileInfo.getString(1);
              LOG.warn("Delete failed for {}: {}", type, file, exc);
            })
        .run(
            fileInfo -> {
              String file = fileInfo.getString(0);
              String type = fileInfo.getString(1);
              removeFunc.accept(file);
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

    long filesCount =
        dataFileCount.get() + manifestCount.get() + manifestListCount.get() + otherFilesCount.get();
    LOG.info("Total files removed: {}", filesCount);
    return new BaseDeleteReachableFilesActionResult(
        dataFileCount.get(), manifestCount.get(), manifestListCount.get(), otherFilesCount.get());
  }
}
