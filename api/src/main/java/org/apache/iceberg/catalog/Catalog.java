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

package org.apache.iceberg.catalog;

import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * A Catalog API for table create, drop, and load operations.
 */
public interface Catalog {

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param location a location for the table; leave null if unspecified
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties);

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @param properties a string map of table properties
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> properties) {
    return createTable(identifier, schema, spec, null, properties);
  }

  /**
   * Create a table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @param spec a partition spec
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec) {
    return createTable(identifier, schema, spec, null, null);
  }

  /**
   * Create an unpartitioned table.
   *
   * @param identifier a table identifier
   * @param schema a schema
   * @return a Table instance
   * @throws AlreadyExistsException if the table already exists
   */
  default Table createTable(
      TableIdentifier identifier,
      Schema schema) {
    return createTable(identifier, schema, PartitionSpec.unpartitioned(), null, null);
  }

  /**
   * Check whether table exists.
   *
   * @param identifier a table identifier
   * @return true if the table exists, false otherwise
   */
  default boolean tableExists(TableIdentifier identifier) {
    try {
      loadTable(identifier);
      return true;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Drop a table.
   *
   * @param identifier a table identifier
   * @return true if the table was dropped, false if the table did not exist
   */
  boolean dropTable(TableIdentifier identifier);

  /**
   * Rename a table.
   *
   * @param from identifier of the table to rename
   * @param to new table name
   * @throws NoSuchTableException if the table does not exist
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * Load a table.
   *
   * @param identifier a table identifier
   * @return instance of {@link Table} implementation referred by {@code tableIdentifier}
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(TableIdentifier identifier);

  /**
   * Register a table.
   *
   * @param identifier a table identifier
   * @param metadataFileLocation the location of a metadata file
   * @return a Table instance
   */
  Table registerTable(TableIdentifier identifier, String metadataFileLocation);
}
