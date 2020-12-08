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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/**
 * The IcebergSource loads/writes tables with format "iceberg". It can load paths and tables.
 *
 * How paths/tables are loaded when using spark.read().format("iceberg").path(table)
 *
 *  table = "file:/path/to/table" -> loads a HadoopTable at given path
 *  table = "catalog.`file:/path/to/table`" -> loads a HadoopTable at given path using settings from 'catalog'
 *  table = "catalog.namespace.`file:/path/to/table`" -> fails. Namespace doesn't exist for paths
 *  table = "tablename" -> loads currentCatalog.currentNamespace.tablename
 *  table = "xxx.tablename" -> if xxx is a catalog load "tablename" from the specified catalog. Otherwise
 *          load "xxx.tablename" from current catalog
 *  table = "xxx.yyy.tablename" -> if xxx is a catalog load "yyy.tablename" from the specified catalog. Otherwise
 *          load "xxx.yyy.tablename" from current catalog
 *
 */
public class IcebergSource implements DataSourceRegister, SupportsCatalogOptions {
  @Override
  public String shortName() {
    return "iceberg";
  }

  @Override
  public StructType inferSchema(CaseInsensitiveStringMap options) {
    return null;
  }

  @Override
  public Transform[] inferPartitioning(CaseInsensitiveStringMap options) {
    return getTable(null, null, options).partitioning();
  }

  @Override
  public boolean supportsExternalMetadata() {
    return true;
  }

  @Override
  public Table getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    String catalogName = extractCatalog(new CaseInsensitiveStringMap(options));
    Identifier ident = extractIdentifier(new CaseInsensitiveStringMap(options));
    CatalogManager catalogManager = SparkSession.active().sessionState().catalogManager();
    CatalogPlugin catalog = catalogManager.catalog(catalogName);
    try {
      if (catalog instanceof TableCatalog) {
        return ((TableCatalog) catalog).loadTable(ident);
      }
    } catch (NoSuchTableException e) {
      // throwing an iceberg NoSuchTableException because the Spark one is typed and cant be thrown from this interface
      throw new org.apache.iceberg.exceptions.NoSuchTableException(e, "Cannot find table for %s.", ident);
    }
    // throwing an iceberg NoSuchTableException because the Spark one is typed and cant be thrown from this interface
    throw new org.apache.iceberg.exceptions.NoSuchTableException("Cannot find table for %s.", ident);
  }

  private Spark3Util.CatalogAndIdentifier catalogAndIdentifier(CaseInsensitiveStringMap options) {
    Preconditions.checkArgument(options.containsKey("path"), "Cannot open table: path is not set");
    String path = options.get("path");
    try {
      return Spark3Util.catalogAndIdentifier(SparkSession.active(), path);
    } catch (ParseException e) {
      List<String> ident = new ArrayList<>();
      ident.add(path);
      return Spark3Util.catalogAndIdentifier(SparkSession.active(), ident);
    }
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).identifier();
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    return catalogAndIdentifier(options).catalog().name();
  }
}
