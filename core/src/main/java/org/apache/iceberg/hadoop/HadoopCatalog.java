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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.RuntimeIOException;

public class HadoopCatalog implements Catalog, Configurable {

  private Configuration conf;

  public HadoopCatalog() {
    this(new Configuration());
  }

  public HadoopCatalog(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Table getTable(TableIdentifier tableIdentifier) {
    String location = location(tableIdentifier);
    TableOperations ops = newTableOps(location);
    if (ops.current() == null) {
      throw new NoSuchTableException("Table does not exist at location: " + location);
    }

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

  /**
   * Create a table using the FileSystem implementation resolve from location.
   *
   * @param tableIdentifier an identifier to identify this table in a namespace.
   * @param schema the schema for this table, can not be null.
   * @param spec the partition spec for this table, can not be null.
   * @param tableProperties can be null or empty
   */
  @Override
  public Table createTable(
      TableIdentifier tableIdentifier, Schema schema, PartitionSpec spec, Map<String, String> tableProperties) {
    String location = location(tableIdentifier);
    TableOperations ops = newTableOps(location);
    if (ops.current() != null) {
      throw new AlreadyExistsException("Table already exists at location: " + location);
    }

    TableMetadata metadata = TableMetadata.newTableMetadata(ops,
        schema,
        spec,
        location,
        tableProperties == null ? new HashMap<>() : tableProperties);
    ops.commit(null, metadata);

    return new BaseTable(ops, location);
  }

  @Override
  public void dropTable(TableIdentifier tableIdentifier) {
    BaseTable table = (BaseTable) getTable(tableIdentifier);
    Path metadataLocation = new Path(table.operations().current().file().location()).getParent();
    FileSystem fs = Util.getFs(metadataLocation, getConf());
    try {
      if (!fs.delete(metadataLocation, true)) {
        throw new RuntimeException("Failed to delete " + metadataLocation);
      }
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete " + metadataLocation);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Hadoop path-based tables cannot be renamed");
  }

  private static String location(TableIdentifier tableIdentifier) {
    Preconditions.checkArgument(!tableIdentifier.hasNamespace(), "For Hadoop tables the namespace must be empty");
    return new Path(tableIdentifier.name()).toString();
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
