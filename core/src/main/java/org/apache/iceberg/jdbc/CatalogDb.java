/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.jdbc;

import java.util.List;

public interface CatalogDb extends AutoCloseable {
  // initialize CatalogDb
  void initialize() throws CatalogDbException;

  void updateTable(
      String namespaceName,
      String tableName,
      String oldPointer,
      String newPointer) throws CatalogDbException;

  void renameTable(String sourceNamespace, String sourceTable, String newNamespace, String newTable) throws CatalogDbException;

  void insertTable(String catalogName, String namespaceName, String tableName, String tablePointer) throws CatalogDbException;

  /**
   * Get current table pointer. In order to be useful in a check-and-create transaction, this will not throw if table doesn't exist.
   *
   * @return String to table pointer or null if table not exists.
   */
  String getTablePointer(String namespaceName, String tableName) throws CatalogDbException;

  boolean dropTable(String namespaceName, String tableName) throws CatalogDbException;

  List<String> listTables(String namespaceName) throws CatalogDbException;

  List<String> listNamespaceByPrefix(String namespacePrefix) throws CatalogDbException;

  boolean namespaceExists(String namespaceName) throws CatalogDbException;

  boolean isNamespaceEmpty(String namespaceName) throws CatalogDbException;
}
