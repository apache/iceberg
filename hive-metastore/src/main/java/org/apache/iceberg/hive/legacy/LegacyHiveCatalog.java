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

import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A {@link HiveCatalog} which uses Hive metadata to read tables. Features like time travel, snapshot isolation and
 * incremental computation are not supported along with any WRITE operations to either the data or metadata.
 */
public class LegacyHiveCatalog extends HiveCatalog {

  private static final Logger LOG = LoggerFactory.getLogger(LegacyHiveCatalog.class);

  public LegacyHiveCatalog(Configuration conf) {
    super(conf);
  }

  @Override
  @SuppressWarnings("CatchBlockLogException")
  public Table loadTable(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      TableOperations ops = newTableOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }

      return new LegacyHiveTable(ops, fullTableName(name(), identifier));
    } else if (isValidMetadataIdentifier(identifier)) {
      throw new UnsupportedOperationException(
          "Metadata views not supported for Hive tables without Iceberg metadata. Table: " + identifier);
    } else {
      throw new NoSuchTableException("Invalid table identifier: %s", identifier);
    }
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    String dbName = tableIdentifier.namespace().level(0);
    String tableName = tableIdentifier.name();
    return new LegacyHiveTableOperations(conf(), clientPool(), dbName, tableName);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    throw new UnsupportedOperationException(
        "Dropping tables not supported through legacy Hive catalog. Table: " + identifier);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    throw new UnsupportedOperationException(
        "Renaming tables not supported through legacy Hive catalog. From: " + from + " To: " + to);
  }

  @Override
  public Table createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec, String location,
      Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Creating tables not supported through legacy Hive catalog. Table: " + identifier);
  }

  @Override
  public Transaction newCreateTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
      String location, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Creating tables not supported through legacy Hive catalog. Table: " + identifier);
  }

  @Override
  public Transaction newReplaceTableTransaction(TableIdentifier identifier, Schema schema, PartitionSpec spec,
      String location, Map<String, String> properties, boolean orCreate) {
    throw new UnsupportedOperationException(
        "Replacing tables not supported through legacy Hive catalog. Table: " + identifier);
  }
}
