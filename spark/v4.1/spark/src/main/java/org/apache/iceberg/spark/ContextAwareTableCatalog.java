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
 * Extension interface for Spark catalogs that support loading tables and views with additional
 * context. The loading context can carry information such as the chain of views that reference a
 * table, enabling catalogs to make context-aware decisions (e.g., scoped credential vending).
 */
public interface ContextAwareTableCatalog {

  /**
   * Load a table with additional context.
   *
   * @param identifier the table identifier
   * @param loadingContext additional context as key-value pairs (e.g., view reference chain)
   * @return the loaded table
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a table at a specific version with additional context.
   *
   * @param identifier the table identifier
   * @param version the version string (snapshot ID or reference name)
   * @param loadingContext additional context as key-value pairs
   * @return the loaded table at the specified version
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, String version, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a table at a specific timestamp with additional context.
   *
   * @param identifier the table identifier
   * @param timestamp the timestamp in microseconds
   * @param loadingContext additional context as key-value pairs
   * @return the loaded table as of the specified timestamp
   * @throws NoSuchTableException if the table does not exist
   */
  Table loadTable(Identifier identifier, long timestamp, Map<String, Object> loadingContext)
      throws NoSuchTableException;

  /**
   * Load a view with additional context.
   *
   * @param identifier the view identifier
   * @param loadingContext additional context as key-value pairs (e.g., view reference chain)
   * @return the loaded view
   * @throws NoSuchViewException if the view does not exist
   */
  View loadView(Identifier identifier, Map<String, Object> loadingContext)
      throws NoSuchViewException;
}
