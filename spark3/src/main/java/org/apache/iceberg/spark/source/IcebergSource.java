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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.util.Pair;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.parser.ParseException;
import org.apache.spark.sql.connector.catalog.CatalogManager;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.SupportsCatalogOptions;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import scala.collection.Seq;

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
  public SparkTable getTable(StructType schema, Transform[] partitioning, Map<String, String> options) {
    throw new UnsupportedOperationException("Cannot get table directly. This implements SupportsCatalogOptions and should be called via that interface");
  }

  private Pair<String, TableIdentifier> tableIdentifier(CaseInsensitiveStringMap options) {
    CatalogManager catalogManager = SparkSession.active().sessionState().catalogManager();
    Namespace defaultNamespace = Namespace.of(catalogManager.currentNamespace());
    Preconditions.checkArgument(options.containsKey("path"), "Cannot open table: path is not set");
    String path = options.get("path");
    try {
      List<String> ident = scala.collection.JavaConverters.seqAsJavaList(SparkSession.active().sessionState().sqlParser().parseMultipartIdentifier(path));
      if (ident.size() == 1) {
        return Pair.of(null, TableIdentifier.of(defaultNamespace, ident.get(0)));
      } else if (ident.size() == 2) {
        if (catalogManager.isCatalogRegistered(ident.get(0))) {
          return Pair.of(ident.get(0), TableIdentifier.of(defaultNamespace, ident.get(1))); //todo what if path?
        } else {
          return Pair.of(null, TableIdentifier.of(ident.toArray(new String[0])));
        }
      } else {
        if (catalogManager.isCatalogRegistered(ident.get(0))) {
          return Pair.of(ident.get(0), TableIdentifier.of(ident.subList(1, ident.size()).toArray(new String[0])));
        } else {
          return Pair.of(null, TableIdentifier.of(ident.toArray(new String[0])));
        }
      }
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Identifier extractIdentifier(CaseInsensitiveStringMap options) {
    TableIdentifier tableIdentifier = tableIdentifier(options).second();
    return Identifier.of(tableIdentifier.namespace().levels(), tableIdentifier.name());
  }

  @Override
  public String extractCatalog(CaseInsensitiveStringMap options) {
    String catalogName = tableIdentifier(options).first();
    return (catalogName == null) ? SupportsCatalogOptions.super.extractCatalog(options) : catalogName;
  }
}
