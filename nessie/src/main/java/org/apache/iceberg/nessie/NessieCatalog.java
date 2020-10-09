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

package org.apache.iceberg.nessie;

import com.dremio.nessie.api.TreeApi;
import com.dremio.nessie.client.NessieClient;
import com.dremio.nessie.client.NessieClient.AuthType;
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableDelete;
import com.dremio.nessie.model.ImmutableMultiContents;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.MultiContents;
import com.dremio.nessie.model.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Nessie implementation of Iceberg Catalog.
 */
public class NessieCatalog extends BaseMetastoreCatalog implements AutoCloseable, SupportsNamespaces {

  public static final String CONF_NESSIE_URL = "nessie.url";
  public static final String CONF_NESSIE_USERNAME = "nessie.username";
  public static final String CONF_NESSIE_PASSWORD = "nessie.password";
  public static final String CONF_NESSIE_AUTH_TYPE = "nessie.auth_type";
  public static final String NESSIE_AUTH_TYPE_DEFAULT = "BASIC";
  public static final String CONF_NESSIE_REF = "nessie.ref";

  private static final Joiner SLASH = Joiner.on("/");
  private static final String ICEBERG_HADOOP_WAREHOUSE_BASE = "iceberg/warehouse";
  private final NessieClient client;
  private final String warehouseLocation;
  private final Configuration config;
  private final UpdateableReference reference;
  private final String name;

  /**
   * create a catalog from a hadoop configuration.
   */
  public NessieCatalog(Configuration config) {
    this("nessie", config);
  }

  /**
   * create a catalog from a hadoop configuration.
   */
  public NessieCatalog(Configuration config, String ref) {
    this("nessie", config, ref);
  }

  /**
   * Create a catalog with a known name from a hadoop configuration.
   */
  public NessieCatalog(String name, Configuration config) {
    this(name, config, null);
  }

  /**
   * Create a catalog with a known name from a hadoop configuration.
   */
  public NessieCatalog(String name, Configuration config, String ref) {
    this(name, config, ref, null);
  }

  /**
   * Create a catalog with a known name from a hadoop configuration.
   */
  public NessieCatalog(String name, Configuration config, String ref, String url) {
    this.config = config;
    this.name = name;
    String path = url == null ? config.get(CONF_NESSIE_URL) : url;
    String username = config.get(CONF_NESSIE_USERNAME);
    String password = config.get(CONF_NESSIE_PASSWORD);
    String authTypeStr = config.get(CONF_NESSIE_AUTH_TYPE, NESSIE_AUTH_TYPE_DEFAULT);
    AuthType authType = AuthType.valueOf(authTypeStr);
    this.client = new NessieClient(authType, path, username, password);

    warehouseLocation = getWarehouseLocation();

    final String requestedRef = ref != null ? ref : config.get(CONF_NESSIE_REF);
    this.reference = get(requestedRef);
  }

  private String getWarehouseLocation() {
    String nessieWarehouseDir = config.get("nessie.warehouse.dir");
    if (nessieWarehouseDir != null) {
      return nessieWarehouseDir;
    }
    String hiveWarehouseDir = config.get("hive.metastore.warehouse.dir");
    if (hiveWarehouseDir != null) {
      return hiveWarehouseDir;
    }
    String defaultFS = config.get("fs.defaultFS");
    if (defaultFS != null) {
      return defaultFS + "/" + ICEBERG_HADOOP_WAREHOUSE_BASE;
    }
    throw new IllegalStateException("Don't know where to put the nessie iceberg data. " +
        "Please set one of the following:\n" +
        "nessie.warehouse.dir\n" +
        "hive.metastore.warehouse.dir\n" +
        "fs.defaultFS.");
  }

  private UpdateableReference get(String requestedRef) {
    try {
      Reference ref = requestedRef == null ? client.getTreeApi().getDefaultBranch()
          : client.getTreeApi().getReferenceByName(requestedRef);
      return new UpdateableReference(ref, client.getTreeApi());
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format("Nessie ref '%s' provided via %s does not exist. " +
          "This ref must exist before creating a NessieCatalog.", requestedRef, CONF_NESSIE_REF), ex);
      }

      throw new IllegalArgumentException(String.format("Nessie does not have an existing default branch." +
        "Either configure an alternative ref via %s or create the default branch on the server.",
          CONF_NESSIE_REF), ex);
    }
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public String name() {
    return name;
  }

  private static ContentsKey toKey(TableIdentifier tableIdentifier) {
    List<String> identifiers = new ArrayList<>();
    if (tableIdentifier.hasNamespace()) {
      identifiers.addAll(Arrays.asList(tableIdentifier.namespace().levels()));
    }
    identifiers.add(tableIdentifier.name());

    ContentsKey key = new ContentsKey(identifiers);
    return key;
  }

  private IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      Contents table = client.getContentsApi().getContents(toKey(tableIdentifier), reference.getHash());
      if (table instanceof IcebergTable) {
        return (IcebergTable) table;
      }
    } catch (NessieNotFoundException e) {
      // ignore
    }
    return null;
  }


  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    ParsedTableIdentifier pti = ParsedTableIdentifier.getParsedTableIdentifier(tableIdentifier, new HashMap<>());
    UpdateableReference newReference = this.reference;
    if (pti.getReference() != null) {
      newReference = get(pti.getReference());
    }
    return new NessieTableOperations(config,
                                     toKey(pti.getTableIdentifier()),
                                     newReference,
                                     client);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    if (table.hasNamespace()) {
      return SLASH.join(warehouseLocation, table.namespace().toString(), table.name());
    }
    return SLASH.join(warehouseLocation, table.name());
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    try {
      return client.getTreeApi()
          .getEntries(reference.getHash())
          .getEntries()
          .stream()
          .filter(namespacePredicate(namespace))
          .map(NessieCatalog::toIdentifier)
          .collect(Collectors.toList());
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException("Unable to list tables due to missing ref.", ex);
    }
  }

  private static Predicate<EntriesResponse.Entry> namespacePredicate(Namespace ns) {
    // TODO: filter to just iceberg tables.
    if (ns == null) {
      return e -> true;
    }

    final List<String> namespace = Arrays.asList(ns.levels());
    Predicate<EntriesResponse.Entry> predicate = e -> {
      List<String> names = e.getName().getElements();

      if (names.size() <= namespace.size()) {
        return false;
      }

      return namespace.equals(names.subList(0, namespace.size()));
    };
    return predicate;
  }

  private static TableIdentifier toIdentifier(EntriesResponse.Entry entry) {
    List<String> elements = entry.getName().getElements();
    return TableIdentifier.of(elements.toArray(new String[elements.size()]));
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    reference.checkMutable();

    IcebergTable existingTable = table(identifier);
    if (existingTable == null) {
      return false;
    }
    TableOperations ops = newTableOps(identifier);
    TableMetadata lastMetadata;
    if (purge && ops.current() != null) {
      lastMetadata = ops.current();
    } else {
      lastMetadata = null;
    }

    try {
      client.getContentsApi().deleteContents(toKey(identifier), reference.getAsBranch().getName(), reference.getHash(),
          "no message");
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to drop table as ref is no longer valid.", e);
    } catch (NessieConflictException e) {
      throw new RuntimeException("Failed to drop table as table state needs to be refreshed.");
    }

    // TODO: purge should be blocked since nessie will clean through other means.
    if (purge && lastMetadata != null) {
      BaseMetastoreCatalog.dropTableData(ops.io(), lastMetadata);
    }
    // TODO: fix this so we don't depend on it in tests.
    refresh();
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier toOriginal) {
    reference.checkMutable();

    TableIdentifier to = removeCatalogName(toOriginal);

    IcebergTable existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("table %s doesn't exists", from.name());
    }
    IcebergTable existingToTable = table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("table %s already exists", to.name());
    }

    MultiContents contents = ImmutableMultiContents.builder()
        .addOperations(ImmutablePut.builder().key(toKey(to)).contents(existingFromTable).build())
        .addOperations(ImmutableDelete.builder().key(toKey(from)).build())
        .build();

    try {
      client.getTreeApi().commitMultipleOperations(reference.getAsBranch().getName(), reference.getHash(),
          "iceberg rename table", contents);
      // TODO: fix this so we don't depend on it in tests.
      refresh();
    } catch (Exception e) {
      throw new CommitFailedException(e, "failed");
    }
  }

  private TableIdentifier removeCatalogName(TableIdentifier to) {

    String[] levels = to.namespace().levels();
    // check if the identifier includes the catalog name and remove it
    if (levels.length >= 2 && name().equalsIgnoreCase(to.namespace().level(0))) {
      Namespace trimmedNamespace = Namespace.of(Arrays.copyOfRange(levels, 1, levels.length));
      return TableIdentifier.of(trimmedNamespace, to.name());
    }

    // return the original unmodified
    return to;
  }

  public TreeApi getTreeApi() {
    return client.getTreeApi();
  }

  public void refresh() {
    reference.refresh();
  }

  public String getHash() {
    return reference.getHash();
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    // no-op
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    try {
      List<Namespace> namespaces = allNamespaces(namespace).stream().distinct().collect(Collectors.toList());
      if (namespaces.isEmpty()) {
        throw new NoSuchNamespaceException("No namespace %s", namespace);
      }
      return namespaces;
    } catch (NessieNotFoundException e) {
      throw new NoSuchNamespaceException(e, "namespace %s not found", namespace);
    }
  }

  private List<Namespace> allNamespaces(Namespace namespace) throws NessieNotFoundException {
    EntriesResponse entries = client.getTreeApi().getEntries(reference.getName());
    return entries.getEntries()
        .stream()
        .map(NessieCatalog::toIdentifier)
        .map(TableIdentifier::namespace)
        .filter(Objects::nonNull)
        .filter(n -> isEqualsOrGreaterThan(namespace, n))
        .collect(Collectors.toList());
  }

  private static boolean isEqualsOrGreaterThan(Namespace target, Namespace current) {
    if (target.levels().length > current.levels().length) {
      return false;
    }
    if (target.levels().length == current.levels().length) {
      return Objects.deepEquals(target, current);
    }
    Namespace subCurrent = Namespace.of(Arrays.copyOf(current.levels(), target.levels().length));
    return Objects.deepEquals(target, subCurrent);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    listNamespaces(namespace);
    return ImmutableMap.of();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    try {
      List<Namespace> namespaces = allNamespaces(namespace);
      if (namespaces.isEmpty()) {
        return false;
      }
      if (namespaces.size() > 1) {
        throw new NamespaceNotEmptyException("Namespace %s still has %d entries", namespace, namespaces.size());
      }
      return true;
    } catch (NessieNotFoundException e) {
      return false;
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot set properties " + namespace + " : setProperties is not supported");

  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot remove properties " + namespace + " : removeProperties is not supported");

  }
}
