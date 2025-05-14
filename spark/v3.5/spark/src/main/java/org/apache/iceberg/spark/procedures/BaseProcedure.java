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
package org.apache.iceberg.spark.procedures;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.MoreExecutors;
import org.apache.iceberg.relocated.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.Spark3Util.CatalogAndIdentifier;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.spark.procedures.SparkProcedures.ProcedureBuilder;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.iceberg.catalog.Procedure;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.datasources.SparkExpressionConverter;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.Option;

abstract class BaseProcedure implements Procedure {
  protected static final DataType STRING_MAP =
      DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType);
  protected static final DataType STRING_ARRAY = DataTypes.createArrayType(DataTypes.StringType);

  private final SparkSession spark;
  private final TableCatalog tableCatalog;

  private SparkActions actions;
  private ExecutorService executorService = null;

  protected BaseProcedure(TableCatalog tableCatalog) {
    this.spark = SparkSession.active();
    this.tableCatalog = tableCatalog;
  }

  protected SparkSession spark() {
    return this.spark;
  }

  protected SparkActions actions() {
    if (actions == null) {
      this.actions = SparkActions.get(spark);
    }
    return actions;
  }

  protected TableCatalog tableCatalog() {
    return this.tableCatalog;
  }

  protected <T> T modifyIcebergTable(Identifier ident, Function<org.apache.iceberg.Table, T> func) {
    try {
      return execute(ident, true, func);
    } finally {
      closeService();
    }
  }

  protected <T> T withIcebergTable(Identifier ident, Function<org.apache.iceberg.Table, T> func) {
    try {
      return execute(ident, false, func);
    } finally {
      closeService();
    }
  }

  private <T> T execute(
      Identifier ident, boolean refreshSparkCache, Function<org.apache.iceberg.Table, T> func) {
    SparkTable sparkTable = loadSparkTable(ident);
    org.apache.iceberg.Table icebergTable = sparkTable.table();

    T result = func.apply(icebergTable);

    if (refreshSparkCache) {
      refreshSparkCache(ident, sparkTable);
    }

    return result;
  }

  protected Identifier toIdentifier(String identifierAsString, String argName) {
    CatalogAndIdentifier catalogAndIdentifier =
        toCatalogAndIdentifier(identifierAsString, argName, tableCatalog);

    Preconditions.checkArgument(
        catalogAndIdentifier.catalog().equals(tableCatalog),
        "Cannot run procedure in catalog '%s': '%s' is a table in catalog '%s'",
        tableCatalog.name(),
        identifierAsString,
        catalogAndIdentifier.catalog().name());

    return catalogAndIdentifier.identifier();
  }

  protected CatalogAndIdentifier toCatalogAndIdentifier(
      String identifierAsString, String argName, CatalogPlugin catalog) {
    Preconditions.checkArgument(
        identifierAsString != null && !identifierAsString.isEmpty(),
        "Cannot handle an empty identifier for argument %s",
        argName);

    return Spark3Util.catalogAndIdentifier(
        "identifier for arg " + argName, spark, identifierAsString, catalog);
  }

  protected SparkTable loadSparkTable(Identifier ident) {
    try {
      Table table = tableCatalog.loadTable(ident);
      ValidationException.check(
          table instanceof SparkTable, "%s is not %s", ident, SparkTable.class.getName());
      return (SparkTable) table;
    } catch (NoSuchTableException e) {
      String errMsg =
          String.format("Couldn't load table '%s' in catalog '%s'", ident, tableCatalog.name());
      throw new RuntimeException(errMsg, e);
    }
  }

  protected Dataset<Row> loadRows(Identifier tableIdent, Map<String, String> options) {
    String tableName = Spark3Util.quotedFullIdentifier(tableCatalog().name(), tableIdent);
    return spark().read().options(options).table(tableName);
  }

  protected void refreshSparkCache(Identifier ident, Table table) {
    CacheManager cacheManager = spark.sharedState().cacheManager();
    DataSourceV2Relation relation =
        DataSourceV2Relation.create(table, Option.apply(tableCatalog), Option.apply(ident));
    cacheManager.recacheByPlan(spark, relation);
  }

  protected Expression filterExpression(Identifier ident, String where) {
    try {
      String name = Spark3Util.quotedFullIdentifier(tableCatalog.name(), ident);
      org.apache.spark.sql.catalyst.expressions.Expression expression =
          SparkExpressionConverter.collectResolvedSparkExpression(spark, name, where);
      return SparkExpressionConverter.convertToIcebergExpression(expression);
    } catch (AnalysisException e) {
      throw new IllegalArgumentException("Cannot parse predicates in where option: " + where, e);
    }
  }

  protected InternalRow newInternalRow(Object... values) {
    return new GenericInternalRow(values);
  }

  protected abstract static class Builder<T extends BaseProcedure> implements ProcedureBuilder {
    private TableCatalog tableCatalog;

    @Override
    public Builder<T> withTableCatalog(TableCatalog newTableCatalog) {
      this.tableCatalog = newTableCatalog;
      return this;
    }

    @Override
    public T build() {
      return doBuild();
    }

    protected abstract T doBuild();

    TableCatalog tableCatalog() {
      return tableCatalog;
    }
  }

  /**
   * Closes this procedure's executor service if a new one was created with {@link
   * BaseProcedure#executorService(int, String)}. Does not block for any remaining tasks.
   */
  protected void closeService() {
    if (executorService != null) {
      executorService.shutdown();
    }
  }

  /**
   * Starts a new executor service which can be used by this procedure in its work. The pool will be
   * automatically shut down if {@link #withIcebergTable(Identifier, Function)} or {@link
   * #modifyIcebergTable(Identifier, Function)} are called. If these methods are not used then the
   * service can be shut down with {@link #closeService()} or left to be closed when this class is
   * finalized.
   *
   * @param threadPoolSize number of threads in the service
   * @param nameFormat name prefix for threads created in this service
   * @return the new executor service owned by this procedure
   */
  protected ExecutorService executorService(int threadPoolSize, String nameFormat) {
    Preconditions.checkArgument(
        executorService == null, "Cannot create a new executor service, one already exists.");
    Preconditions.checkArgument(
        nameFormat != null, "Cannot create a service with null nameFormat arg");
    this.executorService =
        MoreExecutors.getExitingExecutorService(
            (ThreadPoolExecutor)
                Executors.newFixedThreadPool(
                    threadPoolSize,
                    new ThreadFactoryBuilder()
                        .setDaemon(true)
                        .setNameFormat(nameFormat + "-%d")
                        .build()));

    return executorService;
  }
}
