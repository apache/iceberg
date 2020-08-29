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

import java.util.function.Function;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.source.SparkTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.catalyst.parser.ParserInterface;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Procedure;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.CacheManager;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import scala.Option;
import scala.collection.Seq;

abstract class BaseProcedure implements Procedure {
  private final TableCatalog catalog;

  protected BaseProcedure(TableCatalog catalog) {
    this.catalog = catalog;
  }

  protected <T> T modifyIcebergTable(String namespace, String tableName, Function<org.apache.iceberg.Table, T> func) {
    Preconditions.checkArgument(!namespace.isEmpty(), "Namespace cannot be empty");
    Preconditions.checkArgument(!tableName.isEmpty(), "Table name cannot be empty");

    Identifier ident = toIdentifier(namespace, tableName);
    SparkTable sparkTable = loadSparkTable(ident);
    org.apache.iceberg.Table icebergTable = sparkTable.table();

    T result = func.apply(icebergTable);

    refreshSparkCache(ident, sparkTable);

    return result;
  }

  protected Identifier toIdentifier(String namespaceAsString, String name) {
    Seq<String> nameParts = parseMultipartIdentifier(namespaceAsString);
    String[] namespace = new String[nameParts.size()];
    nameParts.copyToArray(namespace);
    return Identifier.of(namespace, name);
  }

  private Seq<String> parseMultipartIdentifier(String identifierAsString) {
    try {
      ParserInterface parser = SparkSession.active().sessionState().sqlParser();
      return parser.parseMultipartIdentifier(identifierAsString);
    } catch (ParseException e) {
      throw new RuntimeException("Couldn't parse identifier: " + identifierAsString, e);
    }
  }

  protected SparkTable loadSparkTable(Identifier ident) {
    try {
      Table table = catalog.loadTable(ident);
      ValidationException.check(table instanceof SparkTable, "%s is not %s", ident, SparkTable.class.getName());
      return (SparkTable) table;
    } catch (NoSuchTableException e) {
      throw new RuntimeException(String.format("Couldn't load table '%s' in catalog '%s'", ident, catalog.name()), e);
    }
  }

  protected void refreshSparkCache(Identifier ident, Table table) {
    SparkSession spark = SparkSession.active();
    CacheManager cacheManager = spark.sharedState().cacheManager();
    DataSourceV2Relation relation = DataSourceV2Relation.create(table, Option.apply(catalog), Option.apply(ident));
    cacheManager.recacheByPlan(spark, relation);
  }
}
