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
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class ViewVersionReplace implements ReplaceViewVersion {
  private final ViewOperations ops;
  private final List<ViewRepresentation> representations = Lists.newArrayList();
  private ViewMetadata base;
  private Namespace defaultNamespace = null;
  private String defaultCatalog = null;
  private Schema schema = null;

  ViewVersionReplace(ViewOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ViewVersion apply() {
    return internalApply().currentVersion();
  }

  ViewMetadata internalApply() {
    Preconditions.checkState(
        !representations.isEmpty(), "Cannot replace view without specifying a query");
    Preconditions.checkState(null != schema, "Cannot replace view without specifying schema");
    Preconditions.checkState(
        null != defaultNamespace, "Cannot replace view without specifying a default namespace");

    this.base = ops.refresh();

    ViewVersion viewVersion = base.currentVersion();
    int maxVersionId =
        base.versions().stream()
            .map(ViewVersion::versionId)
            .max(Integer::compareTo)
            .orElseGet(viewVersion::versionId);

    ViewVersion newVersion =
        ImmutableViewVersion.builder()
            .versionId(maxVersionId + 1)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(schema.schemaId())
            .defaultNamespace(defaultNamespace)
            .defaultCatalog(defaultCatalog)
            .putAllSummary(EnvironmentContext.get())
            .addAllRepresentations(representations)
            .addAllRepresentations(retainedRepresentations())
            .build();

    return ViewMetadata.buildFrom(base).setCurrentVersion(newVersion, schema).build();
  }

  /**
   * Returns the SQL representations of the current version whose dialect isn't being replaced, so
   * that replacing the version of a view that is shared by multiple engines doesn't drop the SQL of
   * the engines that aren't performing the replacement.
   *
   * <p>Nothing is retained when {@link ViewProperties#REPLACE_DROP_DIALECT_ALLOWED} is enabled,
   * because dropping those dialects is what that property asks for.
   *
   * @return the SQL representations of the current version that are retained
   */
  private List<ViewRepresentation> retainedRepresentations() {
    if (PropertyUtil.propertyAsBoolean(
        base.properties(),
        ViewProperties.REPLACE_DROP_DIALECT_ALLOWED,
        ViewProperties.REPLACE_DROP_DIALECT_ALLOWED_DEFAULT)) {
      return ImmutableList.of();
    }

    Set<String> replacedDialects = sqlDialectsFor(representations);
    return base.currentVersion().representations().stream()
        .filter(SQLViewRepresentation.class::isInstance)
        .map(SQLViewRepresentation.class::cast)
        .filter(sql -> !replacedDialects.contains(sql.dialect().toLowerCase(Locale.ROOT)))
        .collect(Collectors.toList());
  }

  private Set<String> sqlDialectsFor(List<ViewRepresentation> viewRepresentations) {
    return viewRepresentations.stream()
        .filter(SQLViewRepresentation.class::isInstance)
        .map(SQLViewRepresentation.class::cast)
        .map(sql -> sql.dialect().toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());
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
        .run(taskOps -> taskOps.commit(base, internalApply()));
  }

  @Override
  public ReplaceViewVersion withQuery(String dialect, String sql) {
    representations.add(ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
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
