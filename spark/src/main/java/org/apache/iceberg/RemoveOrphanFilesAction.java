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
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import parquet.Preconditions;

public class RemoveOrphanFilesAction implements Action<List<String>> {

  private static final Logger LOG = LoggerFactory.getLogger(RemoveOrphanFilesAction.class);

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final SerializableConfiguration hadoopConf;
  private final int partitionDiscoveryParallelism;
  private final Table table;
  private final TableOperations ops;

  private String location = null;
  private Long olderThanTimestamp = null;
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

  public RemoveOrphanFilesAction location(String newLocation) {
    this.location = newLocation;
    return this;
  }

  public RemoveOrphanFilesAction olderThan(long newOlderThanTimestamp) {
    this.olderThanTimestamp = newOlderThanTimestamp;
    return this;
  }

  public RemoveOrphanFilesAction deleteWith(Consumer<String> newDeleteFunc) {
    this.deleteFunc = newDeleteFunc;
    return this;
  }

  @Override
  public List<String> execute() {
    Preconditions.checkArgument(olderThanTimestamp != null, "olderThanTimestamp must be set");

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
    List<String> topLevelDirs = Lists.newArrayList();
    List<String> matchingTopLevelFiles = Lists.newArrayList();

    try {
      Path path = new Path(location);
      FileSystem fs = path.getFileSystem(hadoopConf.value());

      for (FileStatus file : fs.listStatus(path, HiddenPathFilter.get())) {
        if (file.isDirectory()) {
          topLevelDirs.add(file.getPath().toString());
        } else if (file.isFile() && file.getModificationTime() < olderThanTimestamp) {
          matchingTopLevelFiles.add(file.getPath().toString());
        }
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to determine top-level files and dirs in {}", location);
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

  private String metadataTableName(MetadataTableType type) {
    String tableName = table.toString();
    if (tableName.contains("/")) {
      return tableName + "#" + type;
    } else {
      return tableName.replaceFirst("(hadoop\\.)|(hive\\.)", "") + "." + type;
    }
  }
}
