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
package org.apache.iceberg.spark.source;

import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class TestSparkCatalog<T extends TableCatalog & FunctionCatalog & SupportsNamespaces>
    extends SparkSessionCatalog<T> {

  private static final Map<Identifier, Table> tableMap = Maps.newHashMap();

  public static void setTable(Identifier ident, Table table) {
    Preconditions.checkArgument(
        !tableMap.containsKey(ident), "Cannot set " + ident + ". It is already set");
    tableMap.put(ident, table);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    if (tableMap.containsKey(ident)) {
      return tableMap.get(ident);
    }

    TableIdentifier tableIdentifier = Spark3Util.identifierToTableIdentifier(ident);
    Namespace namespace = tableIdentifier.namespace();

    TestTables.TestTable table = TestTables.load(tableIdentifier.toString());
    if (table == null && namespace.equals(Namespace.of("default"))) {
      table = TestTables.load(tableIdentifier.name());
    }

    return new SparkTable(table, false);
  }

  public static void clearTables() {
    tableMap.clear();
  }
}
