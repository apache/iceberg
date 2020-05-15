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

package org.apache.iceberg.hadoop;

import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AllDataFilesTable;
import org.apache.iceberg.AllEntriesTable;
import org.apache.iceberg.AllManifestsTable;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFilesTable;
import org.apache.iceberg.HistoryTable;
import org.apache.iceberg.ManifestEntriesTable;
import org.apache.iceberg.ManifestsTable;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionsTable;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SnapshotsTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Tables;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Implementation of Iceberg tables that uses the Hadoop FileSystem
 * to store metadata and manifests.
 */
public class HadoopTables implements Tables, Configurable {
  private Configuration conf;

  public HadoopTables() {
    this(new Configuration());
  }

  public HadoopTables(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Loads the table location from a FileSystem path location.
   *
   * @param location a path URI (e.g. hdfs:///warehouse/my_table/)
   * @return table implementation
   */
  @Override
  public Table load(String location) {
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      // try to resolve a metadata table, which we encode as URI fragments
      // e.g. hdfs:///warehouse/my_table#snapshots
      int hashIndex = location.lastIndexOf('#');
      if (hashIndex != -1 && location.length() - 1 != hashIndex) {
        // we found char '#', and it is not the last char of location
        String baseTable = location.substring(0, hashIndex);
        String metaTable = location.substring(hashIndex + 1);
        MetadataTableType type = MetadataTableType.from(metaTable);
        if (type != null) {
          return loadMetadataTable(baseTable, type);
        } else {
          throw new NoSuchTableException("Table does not exist at location: " + location);
        }
      } else {
        throw new NoSuchTableException("Table does not exist at location: " + location);
      }
    }

    return new BaseTable(ops, location);
  }

  private Table loadMetadataTable(String location, MetadataTableType type) {
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    }

    Table baseTable = new BaseTable(ops, location);

    switch (type) {
      case ENTRIES:
        return new ManifestEntriesTable(ops, baseTable);
      case FILES:
        return new DataFilesTable(ops, baseTable);
      case HISTORY:
        return new HistoryTable(ops, baseTable);
      case SNAPSHOTS:
        return new SnapshotsTable(ops, baseTable);
      case MANIFESTS:
        return new ManifestsTable(ops, baseTable);
      case PARTITIONS:
        return new PartitionsTable(ops, baseTable);
      case ALL_DATA_FILES:
        return new AllDataFilesTable(ops, baseTable);
      case ALL_MANIFESTS:
        return new AllManifestsTable(ops, baseTable);
      case ALL_ENTRIES:
        return new AllEntriesTable(ops, baseTable);
      default:
        throw new NoSuchTableException(String.format("Unknown metadata table type: %s for %s", type, location));
    }
  }

  /**
   * Create a table using the FileSystem implementation resolve from
   * location.
   *
   * @param schema iceberg schema used to create the table
   * @param spec partitioning spec, if null the table will be unpartitioned
   * @param properties a string map of table properties, initialized to empty if null
   * @param location a path URI (e.g. hdfs:///warehouse/my_table)
   * @return newly created table implementation
   */
  @Override
  public Table create(Schema schema, PartitionSpec spec, Map<String, String> properties,
                      String location) {
    Preconditions.checkNotNull(schema, "A table schema is required");

    TableOperations ops = newTableOps(location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists at location: " + location);
    }

    Map<String, String> tableProps = properties == null ? ImmutableMap.of() : properties;
    PartitionSpec partitionSpec = spec == null ? PartitionSpec.unpartitioned() : spec;
    TableMetadata metadata = TableMetadata.newTableMetadata(schema, partitionSpec, location, tableProps);
    ops.commit(null, metadata);

    return new BaseTable(ops, location);
  }

  private TableOperations newTableOps(String location) {
    return new HadoopTableOperations(new Path(location), conf);
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }
}
