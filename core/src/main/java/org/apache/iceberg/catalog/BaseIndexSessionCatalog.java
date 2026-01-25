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
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.index.Index;
import org.apache.iceberg.index.IndexBuilder;
import org.apache.iceberg.index.IndexSummary;
import org.apache.iceberg.index.IndexType;

public abstract class BaseIndexSessionCatalog extends BaseViewSessionCatalog
    implements IndexSessionCatalog {

  private final Cache<String, IndexCatalog> indexCatalogs =
      Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  public IndexCatalog asIndexCatalog(SessionCatalog.SessionContext context) {
    return indexCatalogs.get(context.sessionId(), id -> new AsIndexCatalog(context));
  }

  public class AsIndexCatalog implements IndexCatalog {
    private final SessionCatalog.SessionContext context;

    private AsIndexCatalog(SessionCatalog.SessionContext context) {
      this.context = context;
    }

    @Override
    public String name() {
      return BaseIndexSessionCatalog.this.name();
    }

    @Override
    public List<IndexSummary> listIndexes(TableIdentifier tableIdentifier, IndexType... types) {
      return BaseIndexSessionCatalog.this.listIndexes(context, tableIdentifier, types);
    }

    @Override
    public Index loadIndex(IndexIdentifier identifier) {
      return BaseIndexSessionCatalog.this.loadIndex(context, identifier);
    }

    @Override
    public boolean indexExists(IndexIdentifier identifier) {
      return BaseIndexSessionCatalog.this.indexExists(context, identifier);
    }

    @Override
    public IndexBuilder buildIndex(IndexIdentifier identifier) {
      return BaseIndexSessionCatalog.this.buildIndex(context, identifier);
    }

    @Override
    public boolean dropIndex(IndexIdentifier identifier) {
      return BaseIndexSessionCatalog.this.dropIndex(context, identifier);
    }

    @Override
    public void invalidateIndex(IndexIdentifier identifier) {
      BaseIndexSessionCatalog.this.invalidateIndex(context, identifier);
    }

    @Override
    public Index registerIndex(IndexIdentifier identifier, String metadataFileLocation) {
      return BaseIndexSessionCatalog.this.registerIndex(context, identifier, metadataFileLocation);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      throw new UnsupportedOperationException(
          this.getClass().getSimpleName() + " doesn't support initialization");
    }
  }
}
