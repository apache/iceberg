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

import org.apache.iceberg.spark.SparkSessionCatalog;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public class TestSparkCatalog<T extends TableCatalog & SupportsNamespaces>
    extends SparkSessionCatalog<T> {

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    String[] parts = ident.name().split("\\$", 2);
    if (parts.length == 2) {
      TestTables.TestTable table = TestTables.load(parts[0]);
      String[] metadataColumns = parts[1].split(",");
      return new SparkTestTable(table, metadataColumns, false);
    } else {
      TestTables.TestTable table = TestTables.load(ident.name());
      return new SparkTestTable(table, null, false);
    }
  }
}
