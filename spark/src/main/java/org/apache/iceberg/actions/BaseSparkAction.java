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
import java.util.List;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

abstract class BaseSparkAction<R> extends BaseAction<R> implements Action<R> {

  protected Dataset<Row> buildValidDataFileDF(SparkSession spark) {
    return buildValidDataFileDF(spark, table().name());
  }

  protected Dataset<Row> buildValidDataFileDF(SparkSession spark, String tableName) {
    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
    Broadcast<FileIO> ioBroadcast = context.broadcast(SparkUtil.serializableFileIO(table()));
    String allManifestsMetadataTable = metadataTableName(tableName, MetadataTableType.ALL_MANIFESTS);

    Dataset<ManifestFileBean> allManifests = spark.read().format("iceberg").load(allManifestsMetadataTable)
        .selectExpr("path", "length", "partition_spec_id as partitionSpecId", "added_snapshot_id as addedSnapshotId")
        .dropDuplicates("path")
        .repartition(spark.sessionState().conf().numShufflePartitions()) // avoid adaptive execution combining tasks
        .as(Encoders.bean(ManifestFileBean.class));

    return allManifests.flatMap(new ReadManifest(ioBroadcast), Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestFileDF(SparkSession spark, String tableName) {
    String allManifestsMetadataTable = metadataTableName(tableName, MetadataTableType.ALL_MANIFESTS);
    return spark.read().format("iceberg").load(allManifestsMetadataTable).selectExpr("path as file_path");
  }

  protected Dataset<Row> buildManifestListDF(SparkSession spark, Table table) {
    List<String> manifestLists = getManifestListPaths(table.snapshots());
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestListDF(SparkSession spark, String metadataFileLocation) {
    StaticTableOperations ops = new StaticTableOperations(metadataFileLocation, table().io());
    return buildManifestListDF(spark, new BaseTable(ops, table().name()));
  }

  protected Dataset<Row> buildOtherMetadataFileDF(SparkSession spark, TableOperations ops) {
    List<String> otherMetadataFiles = getOtherMetadataFilePaths(ops);
    return spark.createDataset(otherMetadataFiles, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildValidMetadataFileDF(SparkSession spark, Table table, TableOperations ops) {
    Dataset<Row> manifestDF = buildManifestFileDF(spark, table.name());
    Dataset<Row> manifestListDF = buildManifestListDF(spark, table);
    Dataset<Row> otherMetadataFileDF = buildOtherMetadataFileDF(spark, ops);

    return manifestDF.union(otherMetadataFileDF).union(manifestListDF);
  }

  private static class ReadManifest implements FlatMapFunction<ManifestFileBean, String> {
    private final Broadcast<FileIO> io;

    ReadManifest(Broadcast<FileIO> io) {
      this.io = io;
    }

    @Override
    public Iterator<String> call(ManifestFileBean manifest) {
      return new ClosingIterator<>(ManifestFiles.readPaths(manifest, io.getValue()).iterator());
    }
  }
}
