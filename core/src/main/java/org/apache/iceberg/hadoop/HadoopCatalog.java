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
import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.RuntimeIOException;

/**
 * HadoopCatalog provides a way to use table names like db.table to work with path-based tables under a common
 * location. It uses a specified directory under a specified filesystem as the warehouse directory, and organizes
 * multiple levels directories that mapped to the database, namespace and the table respectively. The HadoopCatalog
 * takes a location as the warehouse directory. When creating a table such as $db.$tbl, it creates $db/$tbl
 * directory under the warehouse directory, and put the table metadata into that directory.
 *
 * The HadoopCatalog now supports {@link org.apache.iceberg.catalog.Catalog#createTable},
 * {@link org.apache.iceberg.catalog.Catalog#dropTable}, the {@link org.apache.iceberg.catalog.Catalog#renameTable}
 * is not supported yet.
 *
 * Note: The HadoopCatalog requires that the underlying file system supports atomic rename.
 */
public class HadoopCatalog extends BaseMetastoreCatalog implements Closeable {
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final Configuration conf;
  private String warehouseLocation;

  /**
   * The constructor of the HadoopCatalog. It uses the passed location as its warehouse directory.
   *
   * @param conf The Hadoop configuration
   * @param warehouseLocation The location used as warehouse directory
   */
  public HadoopCatalog(Configuration conf, String warehouseLocation) {
    Preconditions.checkArgument(warehouseLocation != null && !warehouseLocation.equals(""),
        "no location provided for warehouse");

    this.conf = conf;
    this.warehouseLocation = warehouseLocation.replaceAll("/*$", "");
  }

  /**
   * The constructor of the HadoopCatalog. It gets the value of <code>fs.defaultFS</code> property
   * from the passed Hadoop configuration as its default file system, and use the default directory
   * <code>iceberg/warehouse</code> as the warehouse directory.
   *
   * @param conf The Hadoop configuration
   */
  public HadoopCatalog(Configuration conf) {
    this.conf = conf;
    this.warehouseLocation = conf.get("fs.defaultFS") + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
  }

  @Override
  public Table createTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, String location, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);
    Preconditions.checkArgument(location == null, "Cannot set a custom location for a path-based table");
    return super.createTable(identifier, schema, spec, null, properties);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);
    return new HadoopTableOperations(new Path(defaultWarehouseLocation(identifier)), conf);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String tableName = tableIdentifier.name();
    StringBuilder sb = new StringBuilder();

    sb.append(warehouseLocation).append('/');
    for (String level : tableIdentifier.namespace().levels()) {
      sb.append(level).append('/');
    }
    sb.append(tableName);

    return sb.toString();
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
        "Missing database in table identifier: %s", identifier);

    Path tablePath = new Path(defaultWarehouseLocation(identifier));
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    FileSystem fs = Util.getFs(tablePath, conf);
    try {
      if (purge && lastMetadata != null) {
        // Since the data files and the metadata files may store in different locations,
        // so it has to call dropTableData to force delete the data file.
        dropTableData(ops.io(), lastMetadata);
      }
      fs.delete(tablePath, true /* recursive */);
      return true;
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to delete file: %s", tablePath);
    }
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException("Cannot rename Hadoop tables");
  }

  @Override
  public void close() throws IOException {
  }

}
