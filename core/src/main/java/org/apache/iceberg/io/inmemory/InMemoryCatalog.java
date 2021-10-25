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

package org.apache.iceberg.io.inmemory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Catalog implementation that uses in-memory data-structures to
 * store the namespaces and tables.
 * This class doesn't touch external resources and
 * can be utilized to write unit tests without side effects.
 * It uses {@link InMemoryFileIO}.
 * <p>This catalog supports namespaces.
 * This catalog automatically creates namespaces if needed during table creation.
 */
public class InMemoryCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private final InMemoryCatalogDb catalogDb;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;

  public InMemoryCatalog() {
    catalogDb = new InMemoryCatalogDb();
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name != null ? name : InMemoryCatalog.class.getSimpleName();

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");

    this.io = new InMemoryFileIO();
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier identifier) {
    return new InMemoryTableOperations(io, identifier, catalogDb);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier identifier) {
    return SLASH.join(defaultNamespaceLocation(identifier.namespace()), identifier.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
  }

  @Override
  public Table createTable(
      TableIdentifier identifier,
      Schema schema,
      PartitionSpec spec,
      String location,
      Map<String, String> properties) {

    // Create namespace automatically if it does not exist.
    if (!namespaceExists(identifier.namespace())) {
      createNamespace(identifier.namespace());
    }

    return super.createTable(identifier, schema, spec, location, properties);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    if (!catalogDb.removeTable(identifier)) {
      LOG.info("Cannot drop table {} because table {} does not exist", identifier, identifier);
      return false;
    }

    if (purge && lastMetadata != null) {
      CatalogUtil.dropTableData(ops.io(), lastMetadata);
    }

    return true;
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    if (!namespaceExists(namespace) && !namespace.isEmpty()) {
      throw new NoSuchNamespaceException(
          "Cannot list tables for namespace %s because namespace %s does not exist", namespace, namespace);
    }

    return catalogDb.listTables().stream()
      .filter(t -> namespace.isEmpty() || t.namespace().equals(namespace))
      .sorted(Comparator.comparing(TableIdentifier::toString))
      .collect(Collectors.toList());
  }

  @Override
  public void renameTable(TableIdentifier fromIdentifier, TableIdentifier toIdentifier) {
    if (fromIdentifier.equals(toIdentifier)) {
      return;
    }

    if (!namespaceExists(toIdentifier.namespace())) {
      throw new NoSuchNamespaceException("Cannot rename %s to %s because the namespace %s does not exist",
          fromIdentifier, toIdentifier, toIdentifier.namespace());
    }

    if (catalogDb.currentMetadataLocation(fromIdentifier) == null) {
      throw new NoSuchTableException("Cannot rename %s to %s because the table %s does not exist",
          fromIdentifier, toIdentifier, fromIdentifier);
    }

    if (catalogDb.currentMetadataLocation(toIdentifier) != null) {
      throw new AlreadyExistsException(
        "Cannot rename %s to %s because the table %s already exists", fromIdentifier, toIdentifier, toIdentifier);
    }

    catalogDb.moveTableEntry(fromIdentifier, toIdentifier);
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    if (namespaceExists(namespace)) {
      throw new AlreadyExistsException("Cannot create namespace %s because namespace %s already exists",
          namespace, namespace);
    }

    catalogDb.putNamespaceEntry(namespace, metadata);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return catalogDb.namespaceProperties(namespace) != null;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<TableIdentifier> identifiers = listTables(namespace);
    if (!identifiers.isEmpty()) {
      throw new NamespaceNotEmptyException(
        "Namespace '%s' is not empty, contains %d table(s).", namespace, identifiers.size());
    }

    return catalogDb.removeNamespace(namespace);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    if (catalogDb.namespaceProperties(namespace) == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    catalogDb.putNamespaceProperties(namespace, properties);
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    if (catalogDb.namespaceProperties(namespace) == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    catalogDb.removeNamespaceProperties(namespace, properties);
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Map<String, String> properties = catalogDb.namespaceProperties(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    return new HashMap<>(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return catalogDb.listNamespaces().stream()
      .filter(n -> !n.isEmpty())
      .map(n -> n.level(0))
      .distinct()
      .sorted()
      .map(Namespace::of)
      .collect(Collectors.toList());
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    final String searchNamespaceString = namespace.isEmpty() ? "" : DOT.join(namespace.levels()) + ".";

    return catalogDb.listNamespaces().stream()
      .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
      .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
      .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    catalogDb.close();
  }
}
