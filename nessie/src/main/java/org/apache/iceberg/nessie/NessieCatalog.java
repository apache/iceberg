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
import com.dremio.nessie.model.IcebergTable;
import com.dremio.nessie.model.ImmutableDelete;
import com.dremio.nessie.model.ImmutableOperations;
import com.dremio.nessie.model.ImmutablePut;
import com.dremio.nessie.model.Operations;
import com.dremio.nessie.model.Reference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
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
  private FileIO fileIO;

  /**
   * Try to avoid passing parameters via hadoop config. Dynamic catalog expects Map instead
   */
  public NessieCatalog() {
  }

  @Override
  public void initialize(String inputName, Map<String, String> options) {
    String fileIOImpl = options.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ? new HadoopFileIO(config) : CatalogUtil.loadFileIO(fileIOImpl, options, config);
    this.name = inputName == null ? "nessie" : inputName;
    this.client = NessieClient.withConfig(s -> options.getOrDefault(s, config.get(s)));

    this.warehouseLocation = options.getOrDefault(NESSIE_WAREHOUSE_DIR, config.get(NESSIE_WAREHOUSE_DIR));
    if (warehouseLocation == null) {
      throw new IllegalStateException("Parameter nessie.warehouse.dir not set, nessie can't store data.");
    }
    final String requestedRef = options.getOrDefault(NessieClient.CONF_NESSIE_REF,
        config.get(NessieClient.CONF_NESSIE_REF));
    this.reference = loadReference(requestedRef);
  }

  @Override
  public void close() {
    client.close();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    TableReference pti = TableReference.parse(tableIdentifier);
    UpdateableReference newReference = this.reference;
    if (pti.getReference() != null) {
      newReference = loadReference(pti.getReference());
    }
    return new NessieTableOperations(NessieUtil.toKey(pti.getTableIdentifier()), newReference, client, fileIO);
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

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    reference.checkMutable();

    IcebergTable existingTable = table(identifier);
    if (existingTable == null) {
      return false;
    }

    // We try to drop the table. Simple retry after ref update.
    int count = 0;
    while (count < 5) {
      count++;
      try {
        dropTableInner(identifier);
        break;
      } catch (NessieConflictException e) {
        // pass for retry
      } catch (NessieNotFoundException e) {
        throw new RuntimeException("Cannot drop table: ref is no longer valid.", e);
      }
    }
    if (count >= 5) {
      throw new RuntimeException("Cannot drop table: failed after retry (update hash and retry)");
    }
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier toOriginal) {
    reference.checkMutable();

    TableIdentifier to = NessieUtil.removeCatalogName(toOriginal, name());

    IcebergTable existingFromTable = table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("table %s doesn't exists", from.name());
    }
    IcebergTable existingToTable = table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("table %s already exists", to.name());
    }

    Operations contents = ImmutableOperations.builder()
        .addOperations(ImmutablePut.builder().key(NessieUtil.toKey(to)).contents(existingFromTable).build(),
            ImmutableDelete.builder().key(NessieUtil.toKey(from)).build())
        .build();

    try {
      client.getTreeApi().commitMultipleOperations(reference.getAsBranch().getName(), reference.getHash(),
          "iceberg rename table", contents);
      // TODO: fix this so we don't depend on it in tests.
      refresh();
    } catch (NessieConflictException e) {
      throw new CommitFailedException(e, "failed");
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to drop table as ref is no longer valid.", e);
    }
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

  public TreeApi getTreeApi() {
    return client.getTreeApi();
  }

  public void refresh() throws NessieNotFoundException {
    reference.refresh();
  }

  public String currentHash() {
    return reference.getHash();
  }

  private IcebergTable table(TableIdentifier tableIdentifier) {
    try {
      Contents table = client.getContentsApi().getContents(NessieUtil.toKey(tableIdentifier), reference.getHash());
      if (table instanceof IcebergTable) {
        return (IcebergTable) table;
      }
    } catch (NessieNotFoundException e) {
      // ignore
    }
    return null;
  }

  private UpdateableReference loadReference(String requestedRef) {
    try {
      Reference ref = requestedRef == null ? client.getTreeApi().getDefaultBranch()
          : client.getTreeApi().getReferenceByName(requestedRef);
      return new UpdateableReference(ref, client.getTreeApi());
    } catch (NessieNotFoundException ex) {
      if (requestedRef != null) {
        throw new IllegalArgumentException(String.format("Nessie ref '%s' does not exist. " +
            "This ref must exist before creating a NessieCatalog.", requestedRef), ex);
      }

      throw new IllegalArgumentException(String.format("Nessie does not have an existing default branch." +
              "Either configure an alternative ref via %s or create the default branch on the server.",
          NessieClient.CONF_NESSIE_REF), ex);
    }
  }


  public void dropTableInner(TableIdentifier identifier) throws NessieConflictException, NessieNotFoundException {
    try {
      client.getContentsApi().deleteContents(NessieUtil.toKey(identifier),
          reference.getAsBranch().getName(),
          reference.getHash(),
          String.format("delete table %s", identifier));

    } finally {
      // TODO: fix this so we don't depend on it in tests. and move refresh into catch clause.
      refresh();
    }
  }

  private Stream<TableIdentifier> tableStream(Namespace namespace) {
    try {
      return client.getTreeApi()
          .getEntries(reference.getHash())
          .getEntries()
          .stream()
          .filter(NessieUtil.namespacePredicate(namespace))
          .map(NessieUtil::toIdentifier);
    } catch (NessieNotFoundException ex) {
      throw new NoSuchNamespaceException(ex, "Unable to list tables due to missing ref. %s", reference.getName());
    }
  }
}
