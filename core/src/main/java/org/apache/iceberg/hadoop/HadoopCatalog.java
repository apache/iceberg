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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.RuntimeIOException;

/**
 * The HadoopCatalog provides a way to manage the path-based table just like catalog. It uses
 * a specified directory under a specified filesystem as the warehouse directory, and organizes
 * two levels directories that mapped to the database and the table respectively. The HadoopCatalog
 * takes a URI as its root directory, and creates a child directory "iceberg/warehouse" as the
 * warehouse directory. When creating a table such as $db.$tbl, it creates $db.db/$tbl under
 * iceberg/warehouse, and put the table metadata into the directory.
 *
 * The HadoopCatalog now supports createtable, droptable, the renametable is not supported yet.
 *
 * Note: The HadoopCatalog requires the rename operation of the filesystem is an atomic operation.
 */
public class HadoopCatalog extends BaseMetastoreCatalog implements Closeable {
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final Configuration conf;
  private String warehouseUri;

  public HadoopCatalog(Configuration conf, String warehouseUri) {
    this.conf = conf;

    if (warehouseUri != null) {
      this.warehouseUri = warehouseUri;
    } else {
      String fsRoot = conf.get("fs.defaultFS");
      Path warehousePath = new Path(fsRoot, ICEBERG_HADOOP_WAREHOUSE_BASE);
      try {
        FileSystem fs = Util.getFs(warehousePath, conf);
        if (!fs.isDirectory(warehousePath)) {
          if (!fs.mkdirs(warehousePath)) {
            throw new IOException("failed to create warehouse for hadoop catalog");
          }
        }
        this.warehouseUri = fsRoot + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
      } catch (IOException e) {
        throw new RuntimeIOException("failed to create directory for warehouse", e);
      }
    }
  }

  public HadoopCatalog(Configuration conf) {
    this(conf, null);
  }

  @Override
  public org.apache.iceberg.Table createTable(
      TableIdentifier identifier, Schema schema, PartitionSpec spec, Map<String, String> properties) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
            "Missing database in table identifier: %s", identifier);
    Path tablePath = new Path(defaultWarehouseLocation(identifier));
    try {
      FileSystem fs = Util.getFs(tablePath, conf);
      if (!fs.isDirectory(tablePath)) {
        fs.mkdirs(tablePath);
      } else {
        throw new AlreadyExistsException("the directory %s for table %s is already exists.",
          tablePath.toString(), identifier.toString());
      }
    } catch (IOException e) {
      throw new RuntimeIOException("failed to create directory", e);
    }
    return super.createTable(identifier, schema, spec, null, properties);
  }

  public org.apache.iceberg.Table createTable(
          TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
            "Missing database in table identifier: %s", identifier);
    return createTable(identifier, schema, spec, null, null);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    Preconditions.checkArgument(identifier.namespace().levels().length >= 1,
                "Missing database in table identifier: %s", identifier);
    return new HadoopTableOperations(new Path(defaultWarehouseLocation(identifier)), conf);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    String[] levels = tableIdentifier.namespace().levels();
    String tableName = tableIdentifier.name();
    
    List<String> path = new ArrayList<>();
    path.add(warehouseUri);
    path.add(levels[0] + ".db");
    for (int i = 1; i < levels.length; i++){
      path.add(levels[i]);
    }
    path.add(tableName);

    return Joiner.on("/").join(path.iterator());
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
  public void close() throws IOException { }

}
