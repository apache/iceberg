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
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.io.ClosingIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.iceberg.MetadataTableType.ALL_MANIFESTS;

abstract class BaseSparkAction<R> implements Action<R> {

  protected abstract Table table();

  /**
   * Returns all the path locations of all Manifest Lists for a given list of snapshots
   * @param snapshots snapshots
   * @return the paths of the Manifest Lists
   */
  private List<String> getManifestListPaths(Iterable<Snapshot> snapshots) {
    List<String> manifestLists = Lists.newArrayList();
    for (Snapshot snapshot : snapshots) {
      String manifestListLocation = snapshot.manifestListLocation();
      if (manifestListLocation != null) {
        manifestLists.add(manifestListLocation);
      }
    }
    return manifestLists;
  }

  /**
   * Returns all Metadata file paths which may not be in the current metadata. Specifically
   * this includes "version-hint" files as well as entries in metadata.previousFiles.
   * @param ops TableOperations for the table we will be getting paths from
   * @return a list of paths to metadata files
   */
  private List<String> getOtherMetadataFilePaths(TableOperations ops) {
    List<String> otherMetadataFiles = Lists.newArrayList();
    otherMetadataFiles.add(ops.metadataFileLocation("version-hint.text"));

    TableMetadata metadata = ops.current();
    otherMetadataFiles.add(metadata.metadataFileLocation());
    for (TableMetadata.MetadataLogEntry previousMetadataFile : metadata.previousFiles()) {
      otherMetadataFiles.add(previousMetadataFile.file());
    }
    return otherMetadataFiles;
  }

  protected Dataset<Row> buildValidDataFileDF(SparkSession spark) {
    return buildValidDataFileDF(spark, table().name());
  }

  protected Dataset<Row> buildValidDataFileDF(SparkSession spark, String tableName) {
    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());
    Broadcast<FileIO> ioBroadcast = context.broadcast(SparkUtil.serializableFileIO(table()));

    Dataset<ManifestFileBean> allManifests = loadMetadataTable(spark, tableName, table().location(), ALL_MANIFESTS)
        .selectExpr("path", "length", "partition_spec_id as partitionSpecId", "added_snapshot_id as addedSnapshotId")
        .dropDuplicates("path")
        .repartition(spark.sessionState().conf().numShufflePartitions()) // avoid adaptive execution combining tasks
        .as(Encoders.bean(ManifestFileBean.class));

    return allManifests.flatMap(new ReadManifest(ioBroadcast), Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildManifestFileDF(SparkSession spark, String tableName) {
    return loadMetadataTable(spark, tableName, table().location(), ALL_MANIFESTS).selectExpr("path as file_path");
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

  // Attempt to use Spark3 Catalog resolution if available on the path
  private static final DynMethods.UnboundMethod LOAD_CATALOG = DynMethods.builder("loadCatalogMetadataTable")
      .hiddenImpl("org.apache.iceberg.spark.Spark3Util",
          SparkSession.class, String.class, MetadataTableType.class)
      .orNoop()
      .build();

  private static Dataset<Row> loadCatalogMetadataTable(SparkSession spark, String tableName, MetadataTableType type) {
    Preconditions.checkArgument(!LOAD_CATALOG.isNoop(), "Cannot find Spark3Util class but Spark3 is in use");
    return LOAD_CATALOG.asStatic().invoke(spark, tableName, type);
  }

  protected static Dataset<Row> loadMetadataTable(SparkSession spark, String tableName, String tableLocation,
                                                  MetadataTableType type) {
    DataFrameReader dataFrameReader = spark.read().format("iceberg");
    if (tableName.contains("/")) {
      // Hadoop Table or Metadata location passed, load without a catalog
      return dataFrameReader.load(tableName + "#" + type);
    }

    // Try DSV2 catalog based name based resolution
    if (spark.version().startsWith("3")) {
      Dataset<Row> catalogMetadataTable = loadCatalogMetadataTable(spark, tableName, type);
      if (catalogMetadataTable != null) {
        return catalogMetadataTable;
      }
    }

    // Catalog based resolution failed, our catalog may be a non-DatasourceV2 Catalog
    if (tableName.startsWith("hadoop.")) {
      // Try loading by location as Hadoop table without Catalog
      return dataFrameReader.load(tableLocation + "#" + type);
    } else if (tableName.startsWith("hive")) {
      // Try loading by name as a Hive table without Catalog
      return dataFrameReader.load(tableName.replaceFirst("hive\\.", "") + "." + type);
    } else {
      throw new IllegalArgumentException(String.format(
          "Cannot find the metadata table for %s of type %s", tableName, type));
    }
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
