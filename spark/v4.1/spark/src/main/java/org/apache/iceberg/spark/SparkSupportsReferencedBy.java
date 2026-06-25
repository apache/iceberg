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
package org.apache.iceberg.spark;

import java.util.List;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.View;

/**
 * A Spark Catalog API extension for loading tables and views with a referenced-by view chain.
 * Extends the loading contract to accept the chain of views referencing a table.
 */
public interface SparkSupportsReferencedBy {

  /**
   * Load a table with the referenced-by view chain.
   *
   * @param identifier a table identifier
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return instance of {@link Table} referred by the identifier
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, List<TableIdentifier> referencedBy)
      throws NoSuchTableException;

  /**
   * Load a table at a specific version with the referenced-by view chain.
   *
   * @param identifier a table identifier
   * @param version the version string
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return instance of {@link Table} referred by the identifier at the specified version
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, String version, List<TableIdentifier> referencedBy)
      throws NoSuchTableException;

  /**
   * Load a table at a specific timestamp with the referenced-by view chain.
   *
   * @param identifier a table identifier
   * @param timestamp the timestamp in microseconds
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return instance of {@link Table} referred by the identifier as of the specified timestamp
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, long timestamp, List<TableIdentifier> referencedBy)
      throws NoSuchTableException;

  /**
   * Load a view with the referenced-by view chain.
   *
   * @param identifier a view identifier
   * @param referencedBy ordered list of view identifiers from outermost to innermost
   * @return instance of {@link View} referred by the identifier
   * @throws NoSuchViewException if the view does not exist
   */
  View loadView(Identifier identifier, List<TableIdentifier> referencedBy)
      throws NoSuchViewException;
}
