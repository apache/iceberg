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
import com.dremio.nessie.error.NessieConflictException;
import com.dremio.nessie.error.NessieNotFoundException;
import com.dremio.nessie.model.Contents;
import com.dremio.nessie.model.ContentsKey;
import com.dremio.nessie.model.EntriesResponse;
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableDelete;
import com.dremio.nessie.model.ImmutableOperations;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.Operations;
import com.dremio.nessie.model.Reference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configurable;
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
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;

/**
 * Nessie implementation of Iceberg Catalog.
 *
 * <p>
 *   A note on namespaces: Nessie namespaces are implicit and do not need to be explicitly created or deleted.
 *   The create and delete namespace methods are no-ops for the NessieCatalog. One can still list namespaces that have
 *   objects stored in them to assist with namespace-centric catalog exploration.
 * </p>
 */
public class NessieCatalog extends BaseMetastoreCatalog implements AutoCloseable, SupportsNamespaces, Configurable {

  private static final Joiner SLASH = Joiner.on("/");
  public static final String NESSIE_WAREHOUSE_DIR = "nessie.warehouse.dir";
  private NessieClient client;
  private String warehouseLocation;
  private Configuration config;
  private UpdateableReference reference;
  private String name;

  /**
   * Try to avoid passing parameters via hadoop config. Dynamic catalog expects Map instead
   *
   * todo replace with #1640 style constructor
   */
  public NessieCatalog() {
  }

  /**
   * Create a catalog with a known name from a hadoop configuration.
   */
  public NessieCatalog(String name, Configuration config, String ref, String url, String warehouseLocation) {
    this.config = config;
    this.name = name == null ? "nessie" : name;
    init(ref, url, warehouseLocation);
  }

  private void init(String ref, String url, String inputWarehouseLocation) {
    this.client = NessieClient.withConfig(s -> {
      if (s.equals(NessieClient.CONF_NESSIE_URL)) {
        return url == null ? config.get(s) : url;
      }
      return config.get(s);
    });

    this.warehouseLocation = inputWarehouseLocation == null ? getWarehouseLocation(config) : inputWarehouseLocation;

    final String requestedRef = ref != null ? ref : config.get(NessieClient.CONF_NESSIE_REF);
    this.reference = get(requestedRef);
  }

  private static String getWarehouseLocation(Configuration config) {
    String nessieWarehouseDir = config.get(NESSIE_WAREHOUSE_DIR);
    if (nessieWarehouseDir != null) {
      return nessieWarehouseDir;
    }
    throw new IllegalStateException("Don't know where to put the nessie iceberg data. Please set nessie.warehouse.dir");
  }

  private UpdateableReference get(String requestedRef) {
    try {
      Reference ref = requestedRef == null ? client.getTreeApi().getDefaultBranch()
          : client.getTreeApi().getReferenceByName(requestedRef);
      return new UpdateableReference(ref, client.getTreeApi());
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format("Nessie ref '%s' provided via %s does not exist. " +
          "This ref must exist before creating a NessieCatalog.", requestedRef, NessieClient.CONF_NESSIE_REF), ex);
      }

      throw new IllegalArgumentException(String.format("Nessie does not have an existing default branch." +
        "Either configure an alternative ref via %s or create the default branch on the server.",
          NessieClient.CONF_NESSIE_REF), ex);
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
    return tableStream(namespace).collect(Collectors.toList());
  }

  private Stream<TableIdentifier> tableStream(Namespace namespace) {
    try {
      return client.getTreeApi()
          .getEntries(reference.getHash())
          .getEntries()
          .stream()
          .filter(namespacePredicate(namespace))
          .map(NessieCatalog::toIdentifier);
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(ex, "Unable to list tables due to missing ref. %s", reference.getName());
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

    Operations contents = ImmutableOperations.builder()
        .addOperations(ImmutablePut.builder().key(toKey(to)).contents(existingFromTable).build(),
            ImmutableDelete.builder().key(toKey(from)).build())
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

  public static Builder builder(Configuration conf) {
    return new Builder(conf);
  }

  /**
   * creating namespaces in nessie is implicit, therefore this is a no-op. Metadata is ignored.
   *
   * @param namespace a multi-part namespace
   * @param metadata a string Map of properties for the given namespace
   */
  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return tableStream(namespace)
        .map(TableIdentifier::namespace)
        .filter(n -> !n.isEmpty())
        .distinct()
        .collect(Collectors.toList());
  }

  /**
   * namespace metadata is not supported in Nessie and we return an empty map.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return an empty map
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return ImmutableMap.of();
  }

  /**
   * Namespaces in Nessie are implicit and deleting them results in a no-op.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return always false.
   */
  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return false;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot set namespace properties " + namespace + " : setProperties is not supported");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) throws NoSuchNamespaceException {
    throw new UnsupportedOperationException(
        "Cannot remove properties " + namespace + " : removeProperties is not supported");
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  public void initialize(String inputName, Map<String, String> options) {
    this.name = inputName;
    init(options.getOrDefault(NessieClient.CONF_NESSIE_REF, config.get(NessieClient.CONF_NESSIE_REF)),
         options.getOrDefault(NessieClient.CONF_NESSIE_URL, config.get(NessieClient.CONF_NESSIE_URL)),
         options.getOrDefault(NESSIE_WAREHOUSE_DIR, config.get(NESSIE_WAREHOUSE_DIR)));
  }

  public static class Builder {
    private final Configuration conf;
    private String url;
    private String ref;
    private String warehouseLocation;
    private String name;

    public Builder(Configuration conf) {
      this.conf = conf;
    }

    public Builder setUrl(String url) {
      this.url = url;
      return this;
    }

    public Builder setRef(String ref) {
      this.ref = ref;
      return this;
    }

    public Builder setWarehouseLocation(String warehouseLocation) {
      this.warehouseLocation = warehouseLocation;
      return this;
    }

    public Builder setName(String name) {
      this.name = name;
      return this;
    }

    public NessieCatalog build() {
      return new NessieCatalog(name, conf, ref, url, warehouseLocation);
    }
  }
}
