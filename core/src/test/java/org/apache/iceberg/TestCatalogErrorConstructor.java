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
package org.apache.iceberg;

import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

/** Throws an error on initialization to simulate class not found error. */
public class TestCatalogErrorConstructor extends BaseMetastoreCatalog {
  static {
    if (true) {
      throw new NoClassDefFoundError("Error while initializing class");
    }
  }

  public TestCatalogErrorConstructor() {}

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return null;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return null;
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    return false;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {}

  @Override
  public void initialize(String name, Map<String, String> properties) {}
}
