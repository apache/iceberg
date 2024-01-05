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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.EnvironmentContext;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;

public abstract class BaseMetastoreViewCatalog extends BaseMetastoreCatalog implements ViewCatalog {
  protected abstract ViewOperations newViewOps(TableIdentifier identifier);

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
  }

  @Override
  public String name() {
    return super.name();
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    if (isValidIdentifier(identifier)) {
      ViewOperations ops = newViewOps(identifier);
      if (ops.current() == null) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      } else {
        return new BaseView(newViewOps(identifier), ViewUtil.fullViewName(name(), identifier));
      }
    }

    throw new NoSuchViewException("Invalid view identifier: %s", identifier);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return new BaseViewBuilder(identifier);
  }

  protected class BaseViewBuilder implements ViewBuilder {
    private final TableIdentifier identifier;
    private final Map<String, String> properties = Maps.newHashMap();
    private final List<ViewRepresentation> representations = Lists.newArrayList();
    private Namespace defaultNamespace = null;
    private String defaultCatalog = null;
    private Schema schema = null;
    private String location = null;
    private Map<String, String> fieldNameToDocChanges = Maps.newHashMap();

    protected BaseViewBuilder(TableIdentifier identifier) {
      Preconditions.checkArgument(
          isValidIdentifier(identifier), "Invalid view identifier: %s", identifier);
      this.identifier = identifier;
    }

    @Override
    public ViewBuilder withSchema(Schema newSchema) {
      this.schema = newSchema;
      return this;
    }

    @Override
    public ViewBuilder withQuery(String dialect, String sql) {
      representations.add(
          ImmutableSQLViewRepresentation.builder().dialect(dialect).sql(sql).build());
      return this;
    }

    @Override
    public ViewBuilder withDefaultCatalog(String catalog) {
      this.defaultCatalog = catalog;
      return this;
    }

    @Override
    public ViewBuilder withDefaultNamespace(Namespace namespace) {
      this.defaultNamespace = namespace;
      return this;
    }

    @Override
    public ViewBuilder withColumnDoc(String name, String doc) {
      Preconditions.checkArgument(name != null, "Field name cannot be null");
      Preconditions.checkArgument(
          !fieldNameToDocChanges.containsKey(name),
          "Cannot change docs for column %s multiple times in a single operation",
          name);
      fieldNameToDocChanges.put(name, doc);
      return this;
    }

    @Override
    public ViewBuilder withProperties(Map<String, String> newProperties) {
      this.properties.putAll(newProperties);
      return this;
    }

    @Override
    public ViewBuilder withProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    @Override
    public ViewBuilder withLocation(String newLocation) {
      this.location = newLocation;
      return this;
    }

    @Override
    public View create() {
      return create(newViewOps(identifier));
    }

    @Override
    public View replace() {
      return replace(newViewOps(identifier));
    }

    @Override
    public View createOrReplace() {
      ViewOperations ops = newViewOps(identifier);
      if (null == ops.current()) {
        return create(ops);
      } else {
        return replace(ops);
      }
    }

    private View create(ViewOperations ops) {
      if (null != ops.current()) {
        throw new AlreadyExistsException("View already exists: %s", identifier);
      }

      Preconditions.checkState(
          !representations.isEmpty(), "Cannot create view without specifying a query");
      Preconditions.checkState(null != schema, "Cannot create view without specifying schema");
      Preconditions.checkState(
          null != defaultNamespace, "Cannot create view without specifying a default namespace");

      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(1)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      ViewMetadata viewMetadata =
          ViewMetadata.builder()
              .setProperties(properties)
              .setLocation(null != location ? location : defaultWarehouseLocation(identifier))
              .setCurrentVersion(viewVersion, schema)
              .build();

      try {
        ops.commit(null, viewMetadata);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("View was created concurrently: %s", identifier);
      }

      return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
    }

    private View replace(ViewOperations ops) {
      if (tableExists(identifier)) {
        throw new AlreadyExistsException("Table with same name already exists: %s", identifier);
      }

      if (null == ops.current()) {
        throw new NoSuchViewException("View does not exist: %s", identifier);
      }

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

        for (Types.NestedField field : schema.columns()) {
          if (!changedFieldIds.contains(field.fieldId())) {
            newFields.add(field);
          }
        }

        this.schema = new Schema(newFields, schema.getAliases(), schema.identifierFieldIds());
      }

      ViewMetadata metadata = ops.current();
      int maxVersionId =
          metadata.versions().stream()
              .map(ViewVersion::versionId)
              .max(Integer::compareTo)
              .orElseGet(metadata::currentVersionId);

      ViewVersion viewVersion =
          ImmutableViewVersion.builder()
              .versionId(maxVersionId + 1)
              .schemaId(schema.schemaId())
              .addAllRepresentations(representations)
              .defaultNamespace(defaultNamespace)
              .defaultCatalog(defaultCatalog)
              .timestampMillis(System.currentTimeMillis())
              .putAllSummary(EnvironmentContext.get())
              .build();

      ViewMetadata.Builder builder =
          ViewMetadata.buildFrom(metadata)
              .setProperties(properties)
              .setCurrentVersion(viewVersion, schema);

      if (null != location) {
        builder.setLocation(location);
      }

      ViewMetadata replacement = builder.build();

      try {
        ops.commit(metadata, replacement);
      } catch (CommitFailedException ignored) {
        throw new AlreadyExistsException("View was updated concurrently: %s", identifier);
      }

      return new BaseView(ops, ViewUtil.fullViewName(name(), identifier));
    }
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new BaseMetastoreViewCatalogTableBuilder(identifier, schema);
  }

  /** The purpose of this class is to add view detection when replacing a table */
  protected class BaseMetastoreViewCatalogTableBuilder extends BaseMetastoreCatalogTableBuilder {
    private final TableIdentifier identifier;

    public BaseMetastoreViewCatalogTableBuilder(TableIdentifier identifier, Schema schema) {
      super(identifier, schema);
      this.identifier = identifier;
    }

    @Override
    public Transaction replaceTransaction() {
      if (viewExists(identifier)) {
        throw new AlreadyExistsException("View with same name already exists: %s", identifier);
      }

      return super.replaceTransaction();
    }
  }
}
