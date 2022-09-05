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
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.SupportsDelete;
import org.apache.spark.sql.connector.catalog.SupportsRead;
import org.apache.spark.sql.connector.catalog.SupportsWrite;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCapability;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.write.LogicalWriteInfo;
import org.apache.spark.sql.connector.write.WriteBuilder;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * An implementation of StagedTable that mimics the behavior of Spark's non-atomic CTAS and RTAS.
 *
 * <p>A Spark catalog can implement StagingTableCatalog to support atomic operations by producing
 * StagedTable. But if a catalog implements StagingTableCatalog, Spark expects the catalog to be
 * able to produce a StagedTable for any table loaded by the catalog. This assumption doesn't always
 * work, as in the case of {@link SparkSessionCatalog}, which supports atomic operations can produce
 * a StagedTable for Iceberg tables, but wraps the session catalog and cannot necessarily produce a
 * working StagedTable implementation for tables that it loads.
 *
 * <p>The work-around is this class, which implements the StagedTable interface but does not have
 * atomic behavior. Instead, the StagedTable interface is used to implement the behavior of the
 * non-atomic SQL plans that will create a table, write, and will drop the table to roll back.
 *
 * <p>This StagedTable implements SupportsRead, SupportsWrite, and SupportsDelete by passing the
 * calls to the real table. Implementing those interfaces is safe because Spark will not use them
 * unless the table supports them and returns the corresponding capabilities from {@link
 * #capabilities()}.
 */
public class RollbackStagedTable
    implements StagedTable, SupportsRead, SupportsWrite, SupportsDelete {
  private final TableCatalog catalog;
  private final Identifier ident;
  private final Table table;

  public RollbackStagedTable(TableCatalog catalog, Identifier ident, Table table) {
    this.catalog = catalog;
    this.ident = ident;
    this.table = table;
  }

  @Override
  public void commitStagedChanges() {
    // the changes have already been committed to the table at the end of the write
  }

  @Override
  public void abortStagedChanges() {
    // roll back changes by dropping the table
    catalog.dropTable(ident);
  }

  @Override
  public String name() {
    return table.name();
  }

  @Override
  public StructType schema() {
    return table.schema();
  }

  @Override
  public Transform[] partitioning() {
    return table.partitioning();
  }

  @Override
  public Map<String, String> properties() {
    return table.properties();
  }

  @Override
  public Set<TableCapability> capabilities() {
    return table.capabilities();
  }

  @Override
  public void deleteWhere(Filter[] filters) {
    call(SupportsDelete.class, t -> t.deleteWhere(filters));
  }

  @Override
  public ScanBuilder newScanBuilder(CaseInsensitiveStringMap options) {
    return callReturning(SupportsRead.class, t -> t.newScanBuilder(options));
  }

  @Override
  public WriteBuilder newWriteBuilder(LogicalWriteInfo info) {
    return callReturning(SupportsWrite.class, t -> t.newWriteBuilder(info));
  }

  private <T> void call(Class<? extends T> requiredClass, Consumer<T> task) {
    callReturning(
        requiredClass,
        inst -> {
          task.accept(inst);
          return null;
        });
  }

  private <T, R> R callReturning(Class<? extends T> requiredClass, Function<T, R> task) {
    if (requiredClass.isInstance(table)) {
      return task.apply(requiredClass.cast(table));
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Table does not implement %s: %s (%s)",
              requiredClass.getSimpleName(), table.name(), table.getClass().getName()));
    }
  }
}
