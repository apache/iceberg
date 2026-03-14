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

import java.util.Map;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.View;

/**
 * A Spark Catalog API extension for context-aware table and view loading. Extends the loading
 * contract to accept additional context such as the chain of views referencing a table.
 */
public interface SparkContextAwareCatalog {

  /**
   * Load a table with additional loading context.
   *
   * @param identifier a table identifier
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link Table} referred by the identifier
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a table at a specific version with additional loading context.
   *
   * @param identifier a table identifier
   * @param version the version string
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link Table} referred by the identifier at the specified version
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, String version, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a table at a specific timestamp with additional loading context.
   *
   * @param identifier a table identifier
   * @param timestamp the timestamp in microseconds
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link Table} referred by the identifier as of the specified timestamp
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, long timestamp, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a view with additional loading context.
   *
   * @param identifier a view identifier
   * @param loadingContext additional context as key-value pairs
   * @return instance of {@link View} referred by the identifier
   * @throws NoSuchViewException if the view does not exist
   */
  View loadView(Identifier identifier, Map<String, Object> loadingContext)
      throws NoSuchViewException;
}
