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

package org.apache.iceberg.hive.legacy;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveClientPool;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LegacyHiveTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveTableOperations.class);

  private final HiveClientPool metaClients;
  private final String databaseName;
  private final String tableName;
  private final Configuration conf;

  private FileIO fileIO;

  protected LegacyHiveTableOperations(Configuration conf, HiveClientPool metaClients, String database, String table) {
    this.conf = conf;
    this.metaClients = metaClients;
    this.databaseName = database;
    this.tableName = table;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = new HadoopFileIO(conf);
    }

    return fileIO;
  }

  @Override
  protected void doRefresh() {
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(databaseName, tableName));

      Schema schema = LegacyHiveTableUtils.getSchema(hiveTable);
      PartitionSpec spec = LegacyHiveTableUtils.getPartitionSpec(hiveTable, schema);

      TableMetadata metadata = TableMetadata.newTableMetadata(schema, spec, hiveTable.getSd().getLocation(),
          LegacyHiveTableUtils.getTableProperties(hiveTable));
      setCurrentMetadata(metadata);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info from metastore %s.%s", databaseName, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during refresh", e);
    }
    setShouldRefresh(false);
  }

  /**
   * Returns an {@link Iterable} of {@link Iterable}s of {@link DataFile}s which belong to the current table and
   * match the partition predicates from the given expression.
   *
   * Each element in the outer {@link Iterable} maps to an {@link Iterable} of {@link DataFile}s originating from the
   * same directory
   */
  Iterable<Iterable<DataFile>> getFilesByFilter(Expression expression) {
    Iterable<DirectoryInfo> matchingDirectories;
    if (current().spec().fields().isEmpty()) {
      matchingDirectories = ImmutableList.of(getDirectoryInfo());
    } else {
      matchingDirectories = getDirectoryInfosByFilter(expression);
    }

    Iterable<Iterable<DataFile>> filesPerDirectory = Iterables.transform(
        matchingDirectories,
        directory -> Iterables.transform(
            FileSystemUtils.listFiles(directory.location(), conf),
            file -> createDataFile(file, current().spec(), directory.partitionData(), directory.format())));

    // Note that we return an Iterable of Iterables here so that the TableScan can process iterables of individual
    // directories in parallel hence resulting in a parallel file listing
    return filesPerDirectory;
  }

  private DirectoryInfo getDirectoryInfo() {
    Preconditions.checkArgument(current().spec().fields().isEmpty(),
        "getDirectoryInfo only allowed for unpartitioned tables");
    try {
      org.apache.hadoop.hive.metastore.api.Table hiveTable =
          metaClients.run(client -> client.getTable(databaseName, tableName));

      return LegacyHiveTableUtils.toDirectoryInfo(hiveTable);
    } catch (TException e) {
      String errMsg = String.format("Failed to get table info for %s.%s from metastore", databaseName, tableName);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getDirectoryInfo", e);
    }
  }

  private List<DirectoryInfo> getDirectoryInfosByFilter(Expression expression) {
    Preconditions.checkArgument(!current().spec().fields().isEmpty(),
        "getDirectoryInfosByFilter only allowed for partitioned tables");
    try {
      LOG.info("Fetching partitions for {}.{} with expression: {}", databaseName, tableName, expression);
      Set<String> partitionColumnNames = current().spec()
          .identitySourceIds()
          .stream()
          .map(id -> current().schema().findColumnName(id))
          .collect(Collectors.toSet());
      Expression simplified = HiveExpressions.simplifyPartitionFilter(expression, partitionColumnNames);
      LOG.info("Simplified expression for {}.{} to {}", databaseName, tableName, simplified);

      final List<Partition> partitions;
      if (simplified.equals(Expressions.alwaysFalse())) {
        // If simplifyPartitionFilter returns FALSE, no partitions are going to match the filter expression
        partitions = ImmutableList.of();
      } else if (simplified.equals(Expressions.alwaysTrue())) {
        // If simplifyPartitionFilter returns TRUE, all partitions are going to match the filter expression
        partitions = metaClients.run(client -> client.listPartitionsByFilter(
            databaseName, tableName, null, (short) -1));
      } else {
        String partitionFilterString = HiveExpressions.toPartitionFilterString(simplified);
        LOG.info("Listing partitions for {}.{} with filter string: {}", databaseName, tableName, partitionFilterString);
        partitions = metaClients.run(
            client -> client.listPartitionsByFilter(databaseName, tableName, partitionFilterString, (short) -1));
      }

      return LegacyHiveTableUtils.toDirectoryInfos(partitions, current().spec());
    } catch (TException e) {
      String errMsg = String.format("Failed to get partition info for %s.%s + expression %s from metastore",
          databaseName, tableName, expression);
      throw new RuntimeException(errMsg, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted in call to getPartitionsByFilter", e);
    }
  }

  private static DataFile createDataFile(FileStatus fileStatus, PartitionSpec partitionSpec, StructLike partitionData,
      FileFormat format) {
    DataFiles.Builder builder = DataFiles.builder(partitionSpec)
        .withPath(fileStatus.getPath().toString())
        .withFormat(format)
        .withFileSizeInBytes(fileStatus.getLen())
        .withMetrics(new Metrics(10000L, null, null, null, null, null));

    if (partitionSpec.fields().isEmpty()) {
      return builder.build();
    } else {
      return builder.withPartition(partitionData).build();
    }
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }

  @Override
  public String metadataFileLocation(String filename) {
    throw new UnsupportedOperationException(
        "Metadata file location not available for Hive tables without Iceberg metadata");
  }

  @Override
  public LocationProvider locationProvider() {
    throw new UnsupportedOperationException("Writes not supported for Hive tables without Iceberg metadata");
  }
}
