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
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
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
 * It uses {@link InMemoryFileIO} by default.
 */
public class InMemoryCatalog extends BaseMetastoreCatalog implements SupportsNamespaces, Configurable, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(InMemoryCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final Joiner DOT = Joiner.on(".");

  private final InMemoryCatalogDb catalogDb;
  private FileIO io;
  private String catalogName;
  private String warehouseLocation;
  private Configuration conf;

  public InMemoryCatalog() {
    catalogDb = new InMemoryCatalogDb();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public String name() {
    return catalogName;
  }

  public FileIO getIo() {
    return io;
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    this.catalogName = name != null ? name : InMemoryCatalog.class.getSimpleName();

    String warehouse = properties.getOrDefault(CatalogProperties.WAREHOUSE_LOCATION, "");
    this.warehouseLocation = warehouse.replaceAll("/*$", "");

    String fileIOImpl = properties.getOrDefault(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getName());
    this.io = CatalogUtil.loadFileIO(fileIOImpl, properties, conf);
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    return new InMemoryTableOperations(io, tableIdentifier, catalogDb);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    return SLASH.join(defaultNamespaceLocation(table.namespace()), table.name());
  }

  private String defaultNamespaceLocation(Namespace namespace) {
    if (namespace.isEmpty()) {
      return warehouseLocation;
    } else {
      return SLASH.join(warehouseLocation, SLASH.join(namespace.levels()));
    }
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
      LOG.info("Skipping drop, table does not exist: {}", identifier);
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
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return catalogDb.getTableList().stream()
      .filter(t -> namespace.isEmpty() || t.namespace().equals(namespace))
      .sorted(Comparator.comparing(TableIdentifier::toString))
      .collect(Collectors.toList());
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    if (from.equals(to)) {
      return;
    }
    if (catalogDb.getTableLocation(from) == null) {
      throw new NoSuchTableException("'From' table: %s does not exist!", from);
    }
    if (catalogDb.getTableLocation(to) != null) {
      throw new AlreadyExistsException(
        "'To' table: %s already exists! Cannot rename 'from' table: %s", to, from);
    }
    catalogDb.moveTableEntry(from, to);
  }

  @Override
  public void createNamespace(Namespace namespace) {
    createNamespace(namespace, Collections.emptyMap());
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    catalogDb.putNamespaceEntryIfAbsent(namespace, metadata);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return catalogDb.getNamespaceProperties(namespace) != null;
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    if (!namespaceExists(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }

    List<TableIdentifier> tableIdentifiers = listTables(namespace);
    if (!tableIdentifiers.isEmpty()) {
      throw new NamespaceNotEmptyException(
        "Namespace %s is not empty. %s tables exist.", namespace, tableIdentifiers.size());
    }

    return catalogDb.removeNamespace(namespace);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    if (catalogDb.getNamespaceProperties(namespace) == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    catalogDb.putNamespaceProperties(namespace, properties);
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    if (catalogDb.getNamespaceProperties(namespace) == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    catalogDb.removeNamespaceProperties(namespace, properties);
    return true;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    Map<String, String> properties = catalogDb.getNamespaceProperties(namespace);
    if (properties == null) {
      throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
    }
    return new HashMap<>(properties);
  }

  @Override
  public List<Namespace> listNamespaces() {
    return catalogDb.getNamespaceList().stream()
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
    final int numLevels = namespace.levels().length;

    return catalogDb.getNamespaceList().stream()
      .filter(n -> DOT.join(n.levels()).startsWith(searchNamespaceString))
      .sorted(Comparator.comparing(n -> DOT.join(n.levels())))
      .collect(Collectors.toList());
  }

  @Override
  public void close() throws IOException {
    catalogDb.close();
  }
}
