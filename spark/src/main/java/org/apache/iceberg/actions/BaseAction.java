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

import java.util.List;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

abstract class BaseAction<R> implements Action<R> {

  protected abstract Table table();

  protected String metadataTableName(MetadataTableType type) {
    String tableName = table().toString();
    if (tableName.contains("/")) {
      return tableName + "#" + type;
    } else if (tableName.startsWith("hadoop.")) {
      // for HadoopCatalog tables, use the table location to load the metadata table
      // because IcebergCatalog uses HiveCatalog when the table is identified by name
      return table().location() + "#" + type;
    } else if (tableName.startsWith("hive.")) {
      // HiveCatalog prepend a logical name which we need to drop for Spark 2.4
      return tableName.replaceFirst("hive\\.", "") + "." + type;
    } else {
      return tableName + "." + type;
    }
  }

  /**
   * Returns all the path locations of all Manifest Lists for a given table
   * @param table the table
   * @return the paths of the Manifest Lists
   */
  protected List<String> getManifestListPaths(Table table) {
    List<String> manifestLists = Lists.newArrayList();
    for (Snapshot snapshot : table.snapshots()) {
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
  protected List<String> getOtherMetadataFilePaths(TableOperations ops) {
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
    String allDataFilesMetadataTable = metadataTableName(MetadataTableType.ALL_DATA_FILES);
    return spark.read().format("iceberg").load(allDataFilesMetadataTable).select("file_path");
  }

  protected Dataset<Row> buildManifestFileDF(SparkSession spark) {
    String allManifestsMetadataTable = metadataTableName(MetadataTableType.ALL_MANIFESTS);
    return spark.read().format("iceberg").load(allManifestsMetadataTable).selectExpr("path as file_path");
  }

  protected Dataset<Row> buildManifestListDF(SparkSession spark, Table table) {
    List<String> manifestLists = getManifestListPaths(table);
    return spark.createDataset(manifestLists, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildOtherMetadataFileDF(SparkSession spark, TableOperations ops) {
    List<String> otherMetadataFiles = getOtherMetadataFilePaths(ops);
    return spark.createDataset(otherMetadataFiles, Encoders.STRING()).toDF("file_path");
  }

  protected Dataset<Row> buildValidMetadataFileDF(SparkSession spark, Table table, TableOperations ops) {
    Dataset<Row> manifestDF = buildManifestFileDF(spark);
    Dataset<Row> manifestListDF = buildManifestListDF(spark, table);
    Dataset<Row> otherMetadataFileDF = buildOtherMetadataFileDF(spark, ops);

    return manifestDF.union(otherMetadataFileDF).union(manifestListDF);
  }
}
