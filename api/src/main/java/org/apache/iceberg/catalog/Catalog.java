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

import java.io.Closeable;
import java.util.Map;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

/**
 * Top level Catalog APIs that supports table DDLs and namespace listing.
 */
public interface Catalog extends Closeable {
  /**
   * creates the table or throws {@link AlreadyExistsException}.
   *
   * @param tableIdentifier an identifier to identify this table in a namespace.
   * @param schema the schema for this table, can not be null.
   * @param spec the partition spec for this table, can not be null.
   * @param tableProperties can be null or empty
   * @return Table instance that was created
   */
  Table createTable(
      TableIdentifier tableIdentifier,
      Schema schema,
      PartitionSpec spec,
      Map<String, String> tableProperties);

  /**
   * Check if table exists or not.
   *
   * @param tableIdentifier an identifier to identify this table in a namespace.
   * @return true if table exists, false if it doesn't.
   */
  default boolean tableExists(TableIdentifier tableIdentifier) {
    try {
      getTable(tableIdentifier);
      return false;
    } catch (NoSuchTableException e) {
      return false;
    }
  }

  /**
   * Drops the table if it exists, otherwise throws {@link NoSuchTableException}
   * The implementation should not delete the underlying data but ensure that a
   * subsequent call to {@link Catalog#tableExists(TableIdentifier)} returns false.
   * <p>
   * If the table does not exists it will throw {@link NoSuchTableException}
   *
   * @param tableIdentifier an identifier to identify this table in a namespace.
   */
  void dropTable(TableIdentifier tableIdentifier);

  /**
   * Renames a table. If {@code from} does not exists throws {@link NoSuchTableException}
   * If {@code to} exists than throws {@link AlreadyExistsException}.
   *
   * @param from original name of the table.
   * @param to expected new name of the table.
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * gets the table or throws {@link NoSuchTableException}
   *
   * @param tableIdentifier an identifier to identify this table in a namespace.
   * @return instance of {@link Table} implementation referred by {@code tableIdentifier}
   */
  Table getTable(TableIdentifier tableIdentifier);
}
