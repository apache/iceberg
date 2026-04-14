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
package org.apache.iceberg.rest.events.operations;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/** Represents an operation performed on a catalog object. */
public interface CatalogOperation {
  OperationType operationType();

  class CreateNamespace implements CatalogOperation {
    private final Namespace namespace;
    private final Map<String, String> properties;

    public CreateNamespace(Namespace namespace, Map<String, String> properties) {
      this.namespace = namespace;
      this.properties = ImmutableMap.copyOf(properties);
    }

    @Override
    public OperationType operationType() {
      return OperationType.CREATE_NAMESPACE;
    }

    public Namespace namespace() {
      return namespace;
    }

    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CreateNamespace)) {
        return false;
      }
      CreateNamespace that = (CreateNamespace) other;
      return Objects.equals(namespace, that.namespace)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, properties);
    }
  }

  class CreateTable implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String tableUuid;
    private final List<MetadataUpdate> updates;

    public CreateTable(TableIdentifier identifier, String tableUuid, List<MetadataUpdate> updates) {
      this.identifier = identifier;
      this.tableUuid = tableUuid;
      this.updates = ImmutableList.copyOf(updates);
    }

    @Override
    public OperationType operationType() {
      return OperationType.CREATE_TABLE;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String tableUuid() {
      return tableUuid;
    }

    public List<MetadataUpdate> updates() {
      return updates;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CreateTable)) {
        return false;
      }
      CreateTable that = (CreateTable) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableUuid, that.tableUuid)
          && Objects.equals(updates, that.updates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableUuid, updates);
    }
  }

  class CreateView implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String viewUuid;
    private final List<MetadataUpdate> updates;

    public CreateView(TableIdentifier identifier, String viewUuid, List<MetadataUpdate> updates) {
      this.identifier = identifier;
      this.viewUuid = viewUuid;
      this.updates = ImmutableList.copyOf(updates);
    }

    @Override
    public OperationType operationType() {
      return OperationType.CREATE_VIEW;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String viewUuid() {
      return viewUuid;
    }

    public List<MetadataUpdate> updates() {
      return updates;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof CreateView)) {
        return false;
      }
      CreateView that = (CreateView) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(viewUuid, that.viewUuid)
          && Objects.equals(updates, that.updates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, viewUuid, updates);
    }
  }

  class Custom implements CatalogOperation {
    private final OperationType.CustomOperationType customOperationType;
    private final TableIdentifier identifier;
    private final Namespace namespace;
    private final String tableUuid;
    private final String viewUuid;
    private final Map<String, String> properties;

    public Custom(
        OperationType.CustomOperationType customOperationType,
        TableIdentifier identifier,
        Namespace namespace,
        String tableUuid,
        String viewUuid,
        Map<String, String> properties) {
      this.customOperationType = customOperationType;
      this.identifier = identifier;
      this.namespace = namespace;
      this.tableUuid = tableUuid;
      this.viewUuid = viewUuid;
      this.properties = properties != null ? ImmutableMap.copyOf(properties) : ImmutableMap.of();
    }

    @Override
    public OperationType operationType() {
      return OperationType.CUSTOM;
    }

    public OperationType.CustomOperationType customOperationType() {
      return customOperationType;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public Namespace namespace() {
      return namespace;
    }

    public String tableUuid() {
      return tableUuid;
    }

    public String viewUuid() {
      return viewUuid;
    }

    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof Custom)) {
        return false;
      }
      Custom that = (Custom) other;
      return Objects.equals(customOperationType, that.customOperationType)
          && Objects.equals(identifier, that.identifier)
          && Objects.equals(namespace, that.namespace)
          && Objects.equals(tableUuid, that.tableUuid)
          && Objects.equals(viewUuid, that.viewUuid)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(
          customOperationType, identifier, namespace, tableUuid, viewUuid, properties);
    }
  }

  class DropNamespace implements CatalogOperation {
    private final Namespace namespace;

    public DropNamespace(Namespace namespace) {
      this.namespace = namespace;
    }

    @Override
    public OperationType operationType() {
      return OperationType.DROP_NAMESPACE;
    }

    public Namespace namespace() {
      return namespace;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof DropNamespace)) {
        return false;
      }
      DropNamespace that = (DropNamespace) other;
      return Objects.equals(namespace, that.namespace);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(namespace);
    }
  }

  class DropTable implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String tableUuid;
    private final Boolean purge;

    public DropTable(TableIdentifier identifier, String tableUuid, Boolean purge) {
      this.identifier = identifier;
      this.tableUuid = tableUuid;
      this.purge = purge;
    }

    @Override
    public OperationType operationType() {
      return OperationType.DROP_TABLE;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String tableUuid() {
      return tableUuid;
    }

    public Boolean purge() {
      return purge;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof DropTable)) {
        return false;
      }
      DropTable that = (DropTable) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableUuid, that.tableUuid)
          && Objects.equals(purge, that.purge);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableUuid, purge);
    }
  }

  class DropView implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String viewUuid;

    public DropView(TableIdentifier identifier, String viewUuid) {
      this.identifier = identifier;
      this.viewUuid = viewUuid;
    }

    @Override
    public OperationType operationType() {
      return OperationType.DROP_VIEW;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String viewUuid() {
      return viewUuid;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof DropView)) {
        return false;
      }
      DropView that = (DropView) other;
      return Objects.equals(identifier, that.identifier) && Objects.equals(viewUuid, that.viewUuid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, viewUuid);
    }
  }

  class RegisterTable implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String tableUuid;
    private final List<MetadataUpdate> updates;

    public RegisterTable(
        TableIdentifier identifier, String tableUuid, List<MetadataUpdate> updates) {
      this.identifier = identifier;
      this.tableUuid = tableUuid;
      this.updates = ImmutableList.copyOf(updates);
    }

    @Override
    public OperationType operationType() {
      return OperationType.REGISTER_TABLE;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String tableUuid() {
      return tableUuid;
    }

    public List<MetadataUpdate> updates() {
      return updates;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RegisterTable)) {
        return false;
      }
      RegisterTable that = (RegisterTable) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableUuid, that.tableUuid)
          && Objects.equals(updates, that.updates);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableUuid, updates);
    }
  }

  class RenameTable implements CatalogOperation {
    private final TableIdentifier sourceIdentifier;
    private final TableIdentifier destIdentifier;
    private final String tableUuid;

    public RenameTable(
        TableIdentifier sourceIdentifier, TableIdentifier destIdentifier, String tableUuid) {
      this.sourceIdentifier = sourceIdentifier;
      this.destIdentifier = destIdentifier;
      this.tableUuid = tableUuid;
    }

    @Override
    public OperationType operationType() {
      return OperationType.RENAME_TABLE;
    }

    public TableIdentifier sourceIdentifier() {
      return sourceIdentifier;
    }

    public TableIdentifier destIdentifier() {
      return destIdentifier;
    }

    public String tableUuid() {
      return tableUuid;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RenameTable)) {
        return false;
      }
      RenameTable that = (RenameTable) other;
      return Objects.equals(sourceIdentifier, that.sourceIdentifier)
          && Objects.equals(destIdentifier, that.destIdentifier)
          && Objects.equals(tableUuid, that.tableUuid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceIdentifier, destIdentifier, tableUuid);
    }
  }

  class RenameView implements CatalogOperation {
    private final TableIdentifier sourceIdentifier;
    private final TableIdentifier destIdentifier;
    private final String viewUuid;

    public RenameView(
        TableIdentifier sourceIdentifier, TableIdentifier destIdentifier, String viewUuid) {
      this.sourceIdentifier = sourceIdentifier;
      this.destIdentifier = destIdentifier;
      this.viewUuid = viewUuid;
    }

    @Override
    public OperationType operationType() {
      return OperationType.RENAME_VIEW;
    }

    public TableIdentifier sourceIdentifier() {
      return sourceIdentifier;
    }

    public TableIdentifier destIdentifier() {
      return destIdentifier;
    }

    public String viewUuid() {
      return viewUuid;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof RenameView)) {
        return false;
      }
      RenameView that = (RenameView) other;
      return Objects.equals(sourceIdentifier, that.sourceIdentifier)
          && Objects.equals(destIdentifier, that.destIdentifier)
          && Objects.equals(viewUuid, that.viewUuid);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceIdentifier, destIdentifier, viewUuid);
    }
  }

  class UpdateNamespaceProperties implements CatalogOperation {
    private final Namespace namespace;
    private final List<String> removed;
    private final List<String> updated;
    private final List<String> missing;

    public UpdateNamespaceProperties(
        Namespace namespace, List<String> removed, List<String> updated, List<String> missing) {
      this.namespace = namespace;
      this.removed = ImmutableList.copyOf(removed);
      this.updated = ImmutableList.copyOf(updated);
      this.missing = missing != null ? ImmutableList.copyOf(missing) : ImmutableList.of();
    }

    @Override
    public OperationType operationType() {
      return OperationType.UPDATE_NAMESPACE_PROPERTIES;
    }

    public Namespace namespace() {
      return namespace;
    }

    public List<String> removed() {
      return removed;
    }

    public List<String> updated() {
      return updated;
    }

    public List<String> missing() {
      return missing;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof UpdateNamespaceProperties)) {
        return false;
      }
      UpdateNamespaceProperties that = (UpdateNamespaceProperties) other;
      return Objects.equals(namespace, that.namespace)
          && Objects.equals(removed, that.removed)
          && Objects.equals(updated, that.updated)
          && Objects.equals(missing, that.missing);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, removed, updated, missing);
    }
  }

  class UpdateTable implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String tableUuid;
    private final List<MetadataUpdate> updates;
    private final List<UpdateRequirement> requirements;

    public UpdateTable(
        TableIdentifier identifier,
        String tableUuid,
        List<MetadataUpdate> updates,
        List<UpdateRequirement> requirements) {
      this.identifier = identifier;
      this.tableUuid = tableUuid;
      this.updates = ImmutableList.copyOf(updates);
      this.requirements =
          requirements != null ? ImmutableList.copyOf(requirements) : ImmutableList.of();
    }

    @Override
    public OperationType operationType() {
      return OperationType.UPDATE_TABLE;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String tableUuid() {
      return tableUuid;
    }

    public List<MetadataUpdate> updates() {
      return updates;
    }

    public List<UpdateRequirement> requirements() {
      return requirements;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof UpdateTable)) {
        return false;
      }
      UpdateTable that = (UpdateTable) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(tableUuid, that.tableUuid)
          && Objects.equals(updates, that.updates)
          && Objects.equals(requirements, that.requirements);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, tableUuid, updates, requirements);
    }
  }

  class UpdateView implements CatalogOperation {
    private final TableIdentifier identifier;
    private final String viewUuid;
    private final List<MetadataUpdate> updates;
    private final List<UpdateRequirement> requirements;

    public UpdateView(
        TableIdentifier identifier,
        String viewUuid,
        List<MetadataUpdate> updates,
        List<UpdateRequirement> requirements) {
      this.identifier = identifier;
      this.viewUuid = viewUuid;
      this.updates = ImmutableList.copyOf(updates);
      this.requirements =
          requirements != null ? ImmutableList.copyOf(requirements) : ImmutableList.of();
    }

    @Override
    public OperationType operationType() {
      return OperationType.UPDATE_VIEW;
    }

    public TableIdentifier identifier() {
      return identifier;
    }

    public String viewUuid() {
      return viewUuid;
    }

    public List<MetadataUpdate> updates() {
      return updates;
    }

    public List<UpdateRequirement> requirements() {
      return requirements;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (!(other instanceof UpdateView)) {
        return false;
      }
      UpdateView that = (UpdateView) other;
      return Objects.equals(identifier, that.identifier)
          && Objects.equals(viewUuid, that.viewUuid)
          && Objects.equals(updates, that.updates)
          && Objects.equals(requirements, that.requirements);
    }

    @Override
    public int hashCode() {
      return Objects.hash(identifier, viewUuid, updates, requirements);
    }
  }
}
