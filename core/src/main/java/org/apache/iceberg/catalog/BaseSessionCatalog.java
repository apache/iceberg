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
package org.apache.iceberg.catalog;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;

public abstract class BaseSessionCatalog implements SessionCatalog {
  private final Cache<String, Catalog> catalogs =
      Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  private String name = null;
  private Map<String, String> properties = null;

  @Override
  public void initialize(String catalogName, Map<String, String> props) {
    this.name = catalogName;
    this.properties = props;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public Map<String, String> properties() {
    return properties;
  }

  public Catalog asCatalog(SessionContext context) {
    return catalogs.get(context.sessionId(), id -> new AsCatalog(context));
  }

  public <T> T withContext(SessionContext context, Function<Catalog, T> task) {
    return task.apply(asCatalog(context));
  }

  public class AsCatalog implements Catalog, SupportsNamespaces {
    private final SessionContext context;

    private AsCatalog(SessionContext context) {
      this.context = context;
    }

    @Override
    public String name() {
      return BaseSessionCatalog.this.name();
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace) {
      return BaseSessionCatalog.this.listTables(context, namespace);
    }

    @Override
    public TableBuilder buildTable(TableIdentifier ident, Schema schema) {
      return BaseSessionCatalog.this.buildTable(context, ident, schema);
    }

    @Override
    public Table registerTable(TableIdentifier ident, String metadataFileLocation) {
      return BaseSessionCatalog.this.registerTable(context, ident, metadataFileLocation);
    }

    @Override
    public boolean tableExists(TableIdentifier ident) {
      return BaseSessionCatalog.this.tableExists(context, ident);
    }

    @Override
    public Table loadTable(TableIdentifier ident) {
      return BaseSessionCatalog.this.loadTable(context, ident);
    }

    @Override
    public boolean dropTable(TableIdentifier ident) {
      return BaseSessionCatalog.this.dropTable(context, ident);
    }

    @Override
    public boolean dropTable(TableIdentifier ident, boolean purge) {
      if (purge) {
        return BaseSessionCatalog.this.purgeTable(context, ident);
      } else {
        return BaseSessionCatalog.this.dropTable(context, ident);
      }
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to) {
      BaseSessionCatalog.this.renameTable(context, from, to);
    }

    @Override
    public void invalidateTable(TableIdentifier ident) {
      BaseSessionCatalog.this.invalidateTable(context, ident);
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> metadata) {
      BaseSessionCatalog.this.createNamespace(context, namespace, metadata);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace) {
      return BaseSessionCatalog.this.listNamespaces(context, namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace) {
      return BaseSessionCatalog.this.loadNamespaceMetadata(context, namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
      return BaseSessionCatalog.this.dropNamespace(context, namespace);
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> updates) {
      return BaseSessionCatalog.this.updateNamespaceMetadata(
          context, namespace, updates, ImmutableSet.of());
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> removals) {
      return BaseSessionCatalog.this.updateNamespaceMetadata(
          context, namespace, ImmutableMap.of(), removals);
    }

    @Override
    public boolean namespaceExists(Namespace namespace) {
      return BaseSessionCatalog.this.namespaceExists(context, namespace);
    }
  }
}
