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
package org.apache.iceberg.view;

import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MAX_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS;
import static org.apache.iceberg.TableProperties.COMMIT_MIN_RETRY_WAIT_MS_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES;
import static org.apache.iceberg.TableProperties.COMMIT_NUM_RETRIES_DEFAULT;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS;
import static org.apache.iceberg.TableProperties.COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT;

import java.util.List;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class ViewVersionReplace implements ReplaceViewVersion {
  private final ViewOperations ops;
  private final List<ViewRepresentation> viewRepresentationsToAdd = Lists.newArrayList();
  private ViewMetadata base;
  private Namespace defaultNamespace;
  private String defaultCatalog;
  private Schema schema;

  ViewVersionReplace(ViewOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ViewVersion apply() {
    Preconditions.checkState(
        !viewRepresentationsToAdd.isEmpty(), "Cannot replace view without specifying a query");
    Preconditions.checkState(null != schema, "Cannot replace view without specifying schema");

    this.base = ops.refresh();

    ViewVersion viewVersion = base.currentVersion();
    int maxVersionId =
        base.versions().stream()
            .map(ViewVersion::versionId)
            .max(Integer::compareTo)
            .orElseGet(viewVersion::versionId);

    return ImmutableViewVersion.builder()
        .versionId(maxVersionId + 1)
        .timestampMillis(System.currentTimeMillis())
        .schemaId(schema.schemaId())
        .defaultNamespace(defaultNamespace)
        .defaultCatalog(defaultCatalog)
        .putSummary("operation", "replace")
        .addAllRepresentations(viewRepresentationsToAdd)
        .build();
  }

  @Override
  public void commit() {
    Tasks.foreach(ops)
        .retry(
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_NUM_RETRIES, COMMIT_NUM_RETRIES_DEFAULT))
        .exponentialBackoff(
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_MIN_RETRY_WAIT_MS, COMMIT_MIN_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_MAX_RETRY_WAIT_MS, COMMIT_MAX_RETRY_WAIT_MS_DEFAULT),
            PropertyUtil.propertyAsInt(
                base.properties(), COMMIT_TOTAL_RETRY_TIME_MS, COMMIT_TOTAL_RETRY_TIME_MS_DEFAULT),
            2.0 /* exponential */)
        .onlyRetryOn(CommitFailedException.class)
        .run(
            taskOps -> {
              ViewVersion newVersion = apply();
              // nothing to do if the version didn't change
              if (this.base.currentVersion().equals(newVersion)) {
                return;
              }

              ViewMetadata updated =
                  ViewMetadata.buildFrom(this.base).setCurrentVersion(newVersion, schema).build();

              taskOps.commit(base, updated);
            });
  }

  @Override
  public ReplaceViewVersion withQuery(String dialect, String sql) {
    viewRepresentationsToAdd.add(
        ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
    return this;
  }

  @Override
  public ReplaceViewVersion withSchema(Schema newSchema) {
    this.schema = newSchema;
    return this;
  }

  @Override
  public ReplaceViewVersion withDefaultCatalog(String catalog) {
    this.defaultCatalog = catalog;
    return this;
  }

  @Override
  public ReplaceViewVersion withDefaultNamespace(Namespace namespace) {
    this.defaultNamespace = namespace;
    return this;
  }
}
