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

import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ContentFile;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.ReachableFileUtil;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.JobGroupInfo;
import org.apache.iceberg.spark.JobGroupUtils;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.spark.source.SerializableTableWithSize;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

abstract class BaseSparkAction<ThisT> {

  protected static final String CONTENT_FILE = "Content File";
  protected static final String MANIFEST = "Manifest";
  protected static final String MANIFEST_LIST = "Manifest List";
  protected static final String OTHERS = "Others";

  protected static final String FILE_PATH = "file_path";
  protected static final String FILE_TYPE = "file_type";
  protected static final String LAST_MODIFIED = "last_modified";

  private static final AtomicInteger JOB_COUNTER = new AtomicInteger();

  private final SparkSession spark;
  private final JavaSparkContext sparkContext;
  private final Map<String, String> options = Maps.newHashMap();

  protected BaseSparkAction(SparkSession spark) {
    this.spark = spark;
    this.sparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  protected SparkSession spark() {
    return spark;
  }

  protected JavaSparkContext sparkContext() {
    return sparkContext;
  }

  protected abstract ThisT self();

  public ThisT option(String name, String value) {
    options.put(name, value);
    return self();
  }

  public ThisT options(Map<String, String> newOptions) {
    options.putAll(newOptions);
    return self();
  }

  protected Map<String, String> options() {
    return options;
  }

  protected <T> T withJobGroupInfo(JobGroupInfo info, Supplier<T> supplier) {
    SparkContext context = spark().sparkContext();
    JobGroupInfo previousInfo = JobGroupUtils.getJobGroupInfo(context);
    try {
      JobGroupUtils.setJobGroupInfo(context, info);
      return supplier.get();
    } finally {
      JobGroupUtils.setJobGroupInfo(context, previousInfo);
    }
  }

  protected JobGroupInfo newJobGroupInfo(String groupId, String desc) {
    return new JobGroupInfo(groupId + "-" + JOB_COUNTER.incrementAndGet(), desc, false);
  }

  protected Table newStaticTable(TableMetadata metadata, FileIO io) {
    String metadataFileLocation = metadata.metadataFileLocation();
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, io);
    return new BaseTable(ops, metadataFileLocation);
  }

  // builds a DF of delete and data file path and type by reading all manifests
  protected Dataset<Row> buildValidContentFileWithTypeDF(Table table) {
    Broadcast<Table> tableBroadcast =
        sparkContext.broadcast(SerializableTableWithSize.copyOf(table));

    Dataset<ManifestFileBean> allManifests =
        loadMetadataTable(table, ALL_MANIFESTS)
            .selectExpr(
                "content",
                "path",
                "length",
                "partition_spec_id as partitionSpecId",
                "added_snapshot_id as addedSnapshotId")
            .dropDuplicates("path")
            .repartition(
                spark
                    .sessionState()
                    .conf()
                    .numShufflePartitions()) // avoid adaptive execution combining tasks
            .as(Encoders.bean(ManifestFileBean.class));

    return allManifests
        .flatMap(
            new ReadManifest(tableBroadcast), Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
        .toDF(FILE_PATH, FILE_TYPE);
  }

  // builds a DF of delete and data file paths by reading all manifests
  protected Dataset<Row> buildValidContentFileDF(Table table) {
    return buildValidContentFileWithTypeDF(table).select(FILE_PATH);
  }

  protected Dataset<Row> buildManifestFileDF(Table table) {
    return loadMetadataTable(table, ALL_MANIFESTS).select(col("path").as(FILE_PATH));
  }

  protected Dataset<Row> buildManifestListDF(Table table) {
    List<String> manifestLists = ReachableFileUtil.manifestListLocations(table);
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF(FILE_PATH);
  }

  protected Dataset<Row> buildOtherMetadataFileDF(Table table) {
    return buildOtherMetadataFileDF(
        table, false /* include all reachable previous metadata locations */);
  }

  protected Dataset<Row> buildAllReachableOtherMetadataFileDF(Table table) {
    return buildOtherMetadataFileDF(
        table, true /* include all reachable previous metadata locations */);
  }

  private Dataset<Row> buildOtherMetadataFileDF(
      Table table, boolean includePreviousMetadataLocations) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.addAll(
        ReachableFileUtil.metadataFileLocations(table, includePreviousMetadataLocations));
    otherMetadataFiles.add(ReachableFileUtil.versionHintLocation(table));
    return spark.createDataset(otherMetadataFiles, Encoders.STRING()).toDF(FILE_PATH);
  }

  protected Dataset<Row> buildValidMetadataFileDF(Table table) {
    Dataset<Row> manifestDF = buildManifestFileDF(table);
    Dataset<Row> manifestListDF = buildManifestListDF(table);
    Dataset<Row> otherMetadataFileDF = buildOtherMetadataFileDF(table);

    return manifestDF.union(otherMetadataFileDF).union(manifestListDF);
  }

  protected Dataset<Row> withFileType(Dataset<Row> ds, String type) {
    return ds.withColumn(FILE_TYPE, lit(type));
  }

  protected Dataset<Row> loadMetadataTable(Table table, MetadataTableType type) {
    return SparkTableUtil.loadMetadataTable(spark, table, type);
  }

  private static class ReadManifest
      implements FlatMapFunction<ManifestFileBean, Tuple2<String, String>> {
    private final Broadcast<Table> table;

    ReadManifest(Broadcast<Table> table) {
      this.table = table;
    }

    @Override
    public Iterator<Tuple2<String, String>> call(ManifestFileBean manifest) {
      return new ClosingIterator<>(entries(manifest));
    }

    public CloseableIterator<Tuple2<String, String>> entries(ManifestFileBean manifest) {
      FileIO io = table.getValue().io();
      Map<Integer, PartitionSpec> specs = table.getValue().specs();
      ImmutableList<String> projection =
          ImmutableList.of(DataFile.FILE_PATH.name(), DataFile.CONTENT.name());

      switch (manifest.content()) {
        case DATA:
          return CloseableIterator.transform(
              ManifestFiles.read(manifest, io, specs).select(projection).iterator(),
              ReadManifest::contentFileWithType);
        case DELETES:
          return CloseableIterator.transform(
              ManifestFiles.readDeleteManifest(manifest, io, specs).select(projection).iterator(),
              ReadManifest::contentFileWithType);
        default:
          throw new IllegalArgumentException(
              "Unsupported manifest content type:" + manifest.content());
      }
    }

    static Tuple2<String, String> contentFileWithType(ContentFile<?> file) {
      return new Tuple2<>(file.path().toString(), file.content().toString());
    }
  }
}
