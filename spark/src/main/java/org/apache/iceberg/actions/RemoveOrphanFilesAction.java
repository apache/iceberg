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

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.util.Tasks;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.SerializableConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An action that removes orphan metadata and data files by listing a given location and comparing
 * the actual files in that location with data and metadata files referenced by all valid snapshots.
 * The location must be accessible for listing via the Hadoop {@link FileSystem}.
 * <p>
 * By default, this action cleans up the table location returned by {@link Table#location()} and
 * removes unreachable files that are older than 3 days using {@link Table#io()}. The behavior can be modified
 * by passing a custom location to {@link #location} and a custom timestamp to {@link #olderThan(long)}.
 * For example, someone might point this action to the data folder to clean up only orphan data files.
 * In addition, there is a way to configure an alternative delete method via {@link #deleteWith(Consumer)}.
 * <p>
 * <em>Note:</em> It is dangerous to call this action with a short retention interval as it might corrupt
 * the state of the table if another operation is writing at the same time.
 */
public class RemoveOrphanFilesAction extends BaseAction<List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveOrphanFilesAction.class);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final SerializableConfiguration hadoopConf;
  private final int partitionDiscoveryParallelism;
  private final Table table;
  private final TableOperations ops;

  private String location = null;
  private long olderThanTimestamp = System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3);
  private Consumer<String> deleteFunc = new Consumer<String>() {
    @Override
    public void accept(String file) {
      table.io().deleteFile(file);
    }
  };

  RemoveOrphanFilesAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    this.partitionDiscoveryParallelism = spark.sessionState().conf().parallelPartitionDiscoveryParallelism();
    this.table = table;
    this.ops = ((HasTableOperations) table).operations();
    this.location = table.location();
  }

  @Override
  protected Table table() {
    return table;
  }

  /**
   * Removes orphan files in the given location.
   *
   * @param newLocation a location
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  /**
   * Removes orphan files that are older than the given timestamp.
   *
   * @param newOlderThanTimestamp a timestamp in milliseconds
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  /**
   * Passes an alternative delete implementation that will be used to delete orphan files.
   *
   * @param newDeleteFunc a delete func
   * @return this for method chaining
   */
  public RemoveOrphanFilesAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public List<String> execute() {
    Dataset<Row> validDataFileDF = buildValidDataFileDF();
    Dataset<Row> validMetadataFileDF = buildValidMetadataFileDF();
    Dataset<Row> validFileDF = validDataFileDF.union(validMetadataFileDF);
    Dataset<Row> actualFileDF = buildActualFileDF();

    Column joinCond = validFileDF.col("file_path").equalTo(actualFileDF.col("file_path"));
    List<String> orphanFiles = actualFileDF.join(validFileDF, joinCond, "leftanti")
        .as(Encoders.STRING())
        .collectAsList();

    Tasks.foreach(orphanFiles)
        .noRetry()
        .suppressFailureWhenFinished()
        .onFailure((file, exc) -> LOG.warn("Failed to delete file: {}", file, exc))
        .run(deleteFunc::accept);

    return orphanFiles;
  }

  private Dataset<Row> buildValidDataFileDF() {
    String allDataFilesMetadataTable = metadataTableName(MetadataTableType.ALL_DATA_FILES);
    return spark.read().format("iceberg")
        .load(allDataFilesMetadataTable)
        .select("file_path");
  }

  private Dataset<Row> buildValidMetadataFileDF() {
    String allManifestsMetadataTable = metadataTableName(MetadataTableType.ALL_MANIFESTS);
    Dataset<Row> manifestDF = spark.read().format("iceberg")
        .load(allManifestsMetadataTable)
        .selectExpr("path as file_path");

    List<String> otherMetadataFiles = Lists.newArrayList();

    for (Snapshot snapshot : table.snapshots()) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        otherMetadataFiles.add(manifestListLocation);
      }
    }

    otherMetadataFiles.add(ops.metadataFileLocation("version-hint.text"));

    TableMetadata metadata = ops.current();
    otherMetadataFiles.add(metadata.file().location());
    for (TableMetadata.MetadataLogEntry previousMetadataFile : metadata.previousFiles()) {
      otherMetadataFiles.add(previousMetadataFile.file());
    }

    Dataset<Row> otherMetadataFileDF = spark
        .createDataset(otherMetadataFiles, Encoders.STRING())
        .toDF("file_path");

    return manifestDF.union(otherMetadataFileDF);
  }

  private Dataset<Row> buildActualFileDF() {
    List<String> subDirs = Lists.newArrayList();
    List<String> matchingFiles = Lists.newArrayList();

    Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

    // list at most 3 levels and only dirs that have less than 10 direct sub dirs on the driver
    listDirRecursively(location, predicate, hadoopConf.value(), 3, 10, subDirs, matchingFiles);

    JavaRDD<String> matchingFileRDD = sparkContext.parallelize(matchingFiles, 1);

    if (subDirs.isEmpty()) {
      return spark.createDataset(matchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
    }

    int parallelism = Math.min(subDirs.size(), partitionDiscoveryParallelism);
    JavaRDD<String> subDirRDD = sparkContext.parallelize(subDirs, parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext.broadcast(hadoopConf);
    JavaRDD<String> matchingLeafFileRDD = subDirRDD.mapPartitions(listDirsRecursively(conf, olderThanTimestamp));

    JavaRDD<String> completeMatchingFileRDD = matchingFileRDD.union(matchingLeafFileRDD);
    return spark.createDataset(completeMatchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
  }

  private static void listDirRecursively(
      String dir, Predicate<FileStatus> predicate, Configuration conf, int maxDepth,
      int maxDirectSubDirs, List<String> remainingSubDirs, List<String> matchingFiles) {

    // stop listing whenever we reach the max depth
    if (maxDepth <= 0) {
      remainingSubDirs.add(dir);
      return;
    }

    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);

      List<String> subDirs = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
        if (file.isDirectory()) {
          subDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          matchingFiles.add(file.getPath().toString());
        }
      }

      // stop listing if the number of direct sub dirs is bigger than maxDirectSubDirs
      if (subDirs.size() > maxDirectSubDirs) {
        remainingSubDirs.addAll(subDirs);
        return;
      }

      for (String subDir : subDirs) {
        listDirRecursively(subDir, predicate, conf, maxDepth - 1, maxDirectSubDirs, remainingSubDirs, matchingFiles);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }

  private static FlatMapFunction<Iterator<String>, String> listDirsRecursively(
      Broadcast<SerializableConfiguration> conf,
      long olderThanTimestamp) {

    return (FlatMapFunction<Iterator<String>, String>) dirs -> {
      List<String> subDirs = Lists.newArrayList();
      List<String> files = Lists.newArrayList();

      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;

      int maxDepth = 2000;
      int maxDirectSubDirs = Integer.MAX_VALUE;

      dirs.forEachRemaining(dir -> {
        listDirRecursively(dir, predicate, conf.value().value(), maxDepth, maxDirectSubDirs, subDirs, files);
      });

      if (!subDirs.isEmpty()) {
        throw new RuntimeException("Could not list subdirectories, reached maximum subdirectory depth: " + maxDepth);
      }

      return files.iterator();
    };
  }
}
