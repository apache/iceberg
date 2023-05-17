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
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;

public abstract class BaseViewSessionCatalog extends BaseSessionCatalog
    implements ViewSessionCatalog {

  private final Cache<String, ViewCatalog> catalogs =
      Caffeine.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();

  public ViewCatalog asViewCatalog(SessionContext context) {
    return catalogs.get(context.sessionId(), id -> new AsViewCatalog(context));
  }

  public class AsViewCatalog implements ViewCatalog {
    private final SessionContext context;

    private AsViewCatalog(SessionContext context) {
      this.context = context;
    }

    @Override
    public String name() {
      return BaseViewSessionCatalog.this.name();
    }

    @Override
    public List<TableIdentifier> listViews(Namespace namespace) {
      return BaseViewSessionCatalog.this.listViews(context, namespace);
    }

    @Override
    public View loadView(TableIdentifier identifier) {
      return BaseViewSessionCatalog.this.loadView(context, identifier);
    }

    @Override
    public boolean viewExists(TableIdentifier identifier) {
      return BaseViewSessionCatalog.this.viewExists(context, identifier);
    }

    @Override
    public ViewBuilder buildView(TableIdentifier identifier) {
      return BaseViewSessionCatalog.this.buildView(context, identifier);
    }

    @Override
    public boolean dropView(TableIdentifier identifier) {
      return BaseViewSessionCatalog.this.dropView(context, identifier);
    }

    @Override
    public void renameView(TableIdentifier from, TableIdentifier to) {
      BaseViewSessionCatalog.this.renameView(context, from, to);
    }

    @Override
    public void invalidateView(TableIdentifier identifier) {
      BaseViewSessionCatalog.this.invalidateView(context, identifier);
    }

    @Override
    public void initialize(String name, Map<String, String> properties) {
      throw new UnsupportedOperationException(
          this.getClass().getSimpleName() + " doesn't support initialization");
    }
  }
}
