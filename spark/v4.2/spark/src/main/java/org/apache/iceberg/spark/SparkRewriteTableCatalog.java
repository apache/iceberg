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
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.SparkRewriteTable;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class SparkRewriteTableCatalog implements TableCatalog, SupportsFunctions {

  private static final String CLASS_NAME = SparkRewriteTableCatalog.class.getName();
  private static final SparkTableCache TABLE_CACHE = SparkTableCache.get();

  private String name = null;

  @Override
  public Identifier[] listTables(String[] namespace) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support listing tables");
  }

  @Override
  public SparkRewriteTable loadTable(Identifier ident) throws NoSuchTableException {
    validateNoNamespace(ident);

    String groupId = ident.name();
    Table table = TABLE_CACHE.get(groupId);

    if (table == null) {
      throw new NoSuchTableException(ident);
    }

    return new SparkRewriteTable(table, groupId);
  }

  @Override
  public SparkTable loadTable(Identifier ident, String version) throws NoSuchTableException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support time travel");
  }

  @Override
  public SparkTable loadTable(Identifier ident, long timestampMicros) throws NoSuchTableException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support time travel");
  }

  @Override
  public void invalidateTable(Identifier ident) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support table invalidation");
  }

  @Override
  public SparkTable createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support creating tables");
  }

  @Override
  public SparkTable alterTable(Identifier ident, TableChange... changes) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support altering tables");
  }

  @Override
  public boolean dropTable(Identifier ident) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support dropping tables");
  }

  @Override
  public boolean purgeTable(Identifier ident) throws UnsupportedOperationException {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support purging tables");
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent) {
    throw new UnsupportedOperationException(CLASS_NAME + " does not support renaming tables");
  }

  @Override
  public void initialize(String catalogName, CaseInsensitiveStringMap options) {
    this.name = catalogName;
  }

  @Override
  public String name() {
    return name;
  }

  private void validateNoNamespace(Identifier ident) {
    Preconditions.checkArgument(
        ident.namespace().length == 0,
        "%s does not support namespaces, but got: %s",
        CLASS_NAME,
        String.join(".", ident.namespace()));
  }
}
