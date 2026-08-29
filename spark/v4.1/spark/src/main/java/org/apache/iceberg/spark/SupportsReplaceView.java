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
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchViewException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.View;
import org.apache.spark.sql.connector.catalog.ViewCatalog;
import org.apache.spark.sql.types.StructType;

public interface SupportsReplaceView extends ViewCatalog {
  /**
   * Replace a view in the catalog
   *
   * @param ident a view identifier
   * @param sql the SQL text that defines the view
   * @param currentCatalog the current catalog
   * @param currentNamespace the current namespace
   * @param schema the view query output schema
   * @param queryColumnNames the query column names
   * @param columnAliases the column aliases
   * @param columnComments the column comments
   * @param properties the view properties
   * @throws NoSuchViewException If the view doesn't exist or is a table
   * @throws NoSuchNamespaceException If the identifier namespace does not exist (optional)
   */
  View replaceView(
      Identifier ident,
      String sql,
      String currentCatalog,
      String[] currentNamespace,
      StructType schema,
      String[] queryColumnNames,
      String[] columnAliases,
      String[] columnComments,
      Map<String, String> properties)
      throws NoSuchViewException, NoSuchNamespaceException;
}
