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

package org.apache.iceberg;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.hadoop.HiddenPathFilter;
import org.apache.iceberg.io.FileIO;
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
import parquet.Preconditions;

public class RemoveOrphanFilesAction implements Action<RemoveOrphanFilesActionResult> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveOrphanFilesAction.class);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final SerializableConfiguration hadoopConf;
  private final FileIO fileIO;
  private final int partitionDiscoveryParallelism;
  private final String dataLocation;

  private String allDataFilesTable = null;
  private Long olderThanTimestamp = null;
  private boolean dryRun = false;

  RemoveOrphanFilesAction(SparkSession spark, Table table) {
    this.spark = spark;
    this.sparkContext = new JavaSparkContext(spark.sparkContext());
    this.hadoopConf = new SerializableConfiguration(spark.sessionState().newHadoopConf());
    this.fileIO = table.io();
    this.partitionDiscoveryParallelism = spark.sessionState().conf().parallelPartitionDiscoveryParallelism();
    this.dataLocation = table.locationProvider().dataLocation();
  }

  public RemoveOrphanFilesAction allDataFilesTable(String newAllDataFilesTable) {
    this.allDataFilesTable = newAllDataFilesTable;
    return this;
  }

  public RemoveOrphanFilesAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  public RemoveOrphanFilesAction dryRun(boolean newDryRun) {
    this.dryRun = newDryRun;
    return this;
  }

  @Override
  public RemoveOrphanFilesActionResult execute() {
    Preconditions.checkArgument(allDataFilesTable != null, "allDataFilesTable must be set");
    Preconditions.checkArgument(olderThanTimestamp != null, "olderThanTimestamp should be set");

    Dataset<Row> validDataFileDF = buildValidDataFileDF();
    Dataset<Row> actualDataFileDF = buildActualDataFileDF();

    Column joinCond = validDataFileDF.col("file_path").equalTo(actualDataFileDF.col("file_path"));
    List<String> orphanDataFiles = actualDataFileDF.join(validDataFileDF, joinCond, "leftanti")
        .as(Encoders.STRING())
        .collectAsList();

    if (!dryRun) {
      Tasks.foreach(orphanDataFiles)
          .noRetry()
          .suppressFailureWhenFinished()
          .onFailure((file, exc) -> LOG.warn("Failed to delete data file: {}", file, exc))
          .run(fileIO::deleteFile);
    }

    return new RemoveOrphanFilesActionResult(orphanDataFiles);
  }

  private Dataset<Row> buildValidDataFileDF() {
    return spark.read().format("iceberg")
        .load(allDataFilesTable)
        .select("file_path")
        .distinct();
  }

  private Dataset<Row> buildActualDataFileDF() {
    List<String> topLevelDirs = Lists.newArrayList();
    List<String> matchingTopLevelFiles = Lists.newArrayList();

    try {
      Path dataPath = new Path(dataLocation);
      FileSystem fs = dataPath.getFileSystem(hadoopConf.value());

      for (FileStatus file : fs.listStatus(dataPath, HiddenPathFilter.get())) {
        // TODO: handle custom metadata folders
        // we need to ignore the metadata folder when data is written to the root table location
        if (file.isDirectory() && !"metadata".equals(file.getPath().getName())) {
          topLevelDirs.add(file.getPath().toString());
        } else if (file.isFile() && file.getModificationTime() < olderThanTimestamp) {
          matchingTopLevelFiles.add(file.getPath().toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to determine top-level files and dirs");
    }

    JavaRDD<String> matchingTopLevelFileRDD = sparkContext.parallelize(matchingTopLevelFiles, 1);

    if (topLevelDirs.isEmpty()) {
      return spark.createDataset(matchingTopLevelFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
    }

    int parallelism = Math.min(topLevelDirs.size(), partitionDiscoveryParallelism);
    JavaRDD<String> topLevelDirRDD = sparkContext.parallelize(topLevelDirs, parallelism);

    Broadcast<SerializableConfiguration> conf = sparkContext.broadcast(hadoopConf);
    JavaRDD<String> matchingLeafFileRDD = topLevelDirRDD.mapPartitions(listDirsRecursively(conf, olderThanTimestamp));

    JavaRDD<String> matchingFileRDD = matchingTopLevelFileRDD.union(matchingLeafFileRDD);
    return spark.createDataset(matchingFileRDD.rdd(), Encoders.STRING()).toDF("file_path");
  }

  private static FlatMapFunction<Iterator<String>, String> listDirsRecursively(
      Broadcast<SerializableConfiguration> conf,
      long olderThanTimestamp) {

    return (FlatMapFunction<Iterator<String>, String>) dirs -> {
      List<String> files = Lists.newArrayList();
      Predicate<FileStatus> predicate = file -> file.getModificationTime() < olderThanTimestamp;
      dirs.forEachRemaining(dir -> {
        List<String> dirFiles = listDirRecursively(dir, predicate, conf.value().value());
        files.addAll(dirFiles);
      });
      return files.iterator();
    };
  }

  private static List<String> listDirRecursively(String dir, Predicate<FileStatus> predicate, Configuration conf) {
    try {
      Path path = new Path(dir);
      FileSystem fs = path.getFileSystem(conf);

      List<String> childDirs = Lists.newArrayList();
      List<String> matchingFiles = Lists.newArrayList();

      for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
        if (file.isDirectory()) {
          childDirs.add(file.getPath().toString());
        } else if (file.isFile() && predicate.test(file)) {
          matchingFiles.add(file.getPath().toString());
        }
      }

      for (String childDir : childDirs) {
        List<String> childDirFiles = listDirRecursively(childDir, predicate, conf);
        matchingFiles.addAll(childDirFiles);
      }

      return matchingFiles;
    } catch (IOException e) {
      throw new RuntimeIOException(e);
    }
  }
}
