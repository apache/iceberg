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

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;

import java.util.List;

public interface Catalog {
  /**
   * creates the table or throws {@link AlreadyExistsException}.
   * @param schema
   * @param spec
   * @param tableIdentifier
   * @return Table instance that was created
   */
  Table createTable(Schema schema, PartitionSpec spec, TableIdentifier tableIdentifier);

  /**
   * check if tables exists or not.
   * @param tableIdentifier
   * @return true if table exists, false if it doesn't.
   */
  boolean tableExists(TableIdentifier tableIdentifier);

  /**
   * Drops the table if it exists, otherwise throws {@link NoSuchTableException}
   * @param tableIdentifier
   * @param shouldDeleteData should the data corresponding to this table be deleted
   */
  void dropTable(TableIdentifier tableIdentifier, boolean shouldDeleteData);

  /**
   * Renames a table. If @{code from} does not exists throws {@link NoSuchTableException}
   * If {@code to} exists than throws {@link AlreadyExistsException}.
   * @param from
   * @param to
   */
  void renameTable(TableIdentifier from, TableIdentifier to);

  /**
   * Returns list of all the tables {@link TableIdentifier tables} under the provided schema.
   * @param schemaIdentifier
   * @return List of {@link TableIdentifier} under the specified schemaIdentifier.
   */
  List<TableIdentifier> listTables(SchemaIdentifier schemaIdentifier);
}
