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
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.Tasks;

class ViewVersionReplace implements ReplaceViewVersion {
  private final ViewOperations ops;
  private final List<ViewRepresentation> representations = Lists.newArrayList();
  private ViewMetadata base;
  private Namespace defaultNamespace = null;
  private String defaultCatalog = null;
  private Schema schema = null;

  private Map<String, String> fieldNameToDocChanges = Maps.newHashMap();

  ViewVersionReplace(ViewOperations ops) {
    this.ops = ops;
    this.base = ops.current();
  }

  @Override
  public ViewVersion apply() {
    return internalApply().currentVersion();
  }

  ViewMetadata internalApply() {
    this.base = ops.refresh();

    if (fieldNameToDocChanges.isEmpty()) {
      Preconditions.checkState(
          !representations.isEmpty(), "Cannot replace view without specifying a query");
      Preconditions.checkState(null != schema, "Cannot replace view without specifying schema");
      Preconditions.checkState(
          null != defaultNamespace, "Cannot replace view without specifying a default namespace");
    } else {
      Preconditions.checkState(
          representations.isEmpty(), "Cannot change column docs and replace representations");
      Preconditions.checkState(
          null == schema, "Cannot change column docs and also explicitly change schema");
      Preconditions.checkState(
          null == defaultNamespace, "Cannot change column docs and change default namespace");
      Schema currentSchema = ops.current().schema();
      List<Types.NestedField> newFields = Lists.newArrayList();

      Set<Integer> changedFieldIds = Sets.newHashSet();
      for (Map.Entry<String, String> docChange : fieldNameToDocChanges.entrySet()) {
        String name = docChange.getKey();
        String doc = docChange.getValue();
        Types.NestedField fieldToUpdate = currentSchema.findField(name);
        Preconditions.checkArgument(fieldToUpdate != null, "Field %s does not exist", name);
        Types.NestedField updatedField =
            Types.NestedField.of(
                fieldToUpdate.fieldId(),
                fieldToUpdate.isOptional(),
                fieldToUpdate.name(),
                fieldToUpdate.type(),
                doc);

        newFields.add(updatedField);
        changedFieldIds.add(fieldToUpdate.fieldId());
      }

      for (Types.NestedField field : currentSchema.columns()) {
        if (!changedFieldIds.contains(field.fieldId())) {
          newFields.add(field);
        }
      }

      this.schema =
          new Schema(newFields, currentSchema.getAliases(), currentSchema.identifierFieldIds());
    }

    ViewVersion viewVersion = base.currentVersion();
    int maxVersionId =
        base.versions().stream()
            .map(ViewVersion::versionId)
            .max(Integer::compareTo)
            .orElseGet(viewVersion::versionId);

    ViewVersion current = base.currentVersion();
    ViewVersion newVersion =
        ImmutableViewVersion.builder()
            .versionId(maxVersionId + 1)
            .timestampMillis(System.currentTimeMillis())
            .schemaId(schema.schemaId())
            .defaultNamespace(
                defaultNamespace != null ? defaultNamespace : current.defaultNamespace())
            .defaultCatalog(defaultCatalog != null ? defaultCatalog : current.defaultCatalog())
            .putAllSummary(EnvironmentContext.get())
            .addAllRepresentations(
                !representations.isEmpty() ? representations : current.representations())
            .build();

    return ViewMetadata.buildFrom(base).setCurrentVersion(newVersion, schema).build();
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

  @Override
  public ReplaceViewVersion withColumnDoc(String name, String doc) {
    Preconditions.checkArgument(name != null, "Field name cannot be null");
    Preconditions.checkArgument(
        !fieldNameToDocChanges.containsKey(name),
        "Cannot change docs for column %s multiple times in a single operation",
        name);
    fieldNameToDocChanges.put(name, doc);
    return this;
  }
}
