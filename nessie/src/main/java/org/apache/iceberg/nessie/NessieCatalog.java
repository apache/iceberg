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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.util.Tasks;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.CommitMultipleOperationsBuilder;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.BaseNessieClientServerException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNamespaceAlreadyExistsException;
import org.projectnessie.error.NessieNamespaceNotEmptyException;
import org.projectnessie.error.NessieNamespaceNotFoundException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GetNamespacesResponse;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie implementation of Iceberg Catalog.
 *
 * <p>
 * A note on namespaces: Nessie namespaces are implicit and do not need to be explicitly created or deleted.
 * The create and delete namespace methods are no-ops for the NessieCatalog. One can still list namespaces that have
 * objects stored in them to assist with namespace-centric catalog exploration.
 * </p>
 */
public class NessieCatalog extends BaseMetastoreCatalog implements AutoCloseable, SupportsNamespaces, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(NessieCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private NessieIcebergClient client;
  private String warehouseLocation;
  private Configuration config;
  private String name;
  private FileIO fileIO;
  private Map<String, String> catalogOptions;

  public NessieCatalog() {
  }

  @Override
  public void initialize(String inputName, Map<String, String> options) {
    this.catalogOptions = ImmutableMap.copyOf(options);
    String fileIOImpl = options.get(CatalogProperties.FILE_IO_IMPL);
    this.fileIO = fileIOImpl == null ? new HadoopFileIO(config) : CatalogUtil.loadFileIO(fileIOImpl, options, config);
    this.name = inputName == null ? "nessie" : inputName;
    // remove nessie prefix
    final Function<String, String> removePrefix = x -> x.replace(NessieUtil.NESSIE_CONFIG_PREFIX, "");

    this.warehouseLocation = options.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation == null) {
      // Explicitly log a warning, otherwise the thrown exception can get list in the "silent-ish catch"
      // in o.a.i.spark.Spark3Util.catalogAndIdentifier(o.a.s.sql.SparkSession, List<String>,
      //     o.a.s.sql.connector.catalog.CatalogPlugin)
      // in the code block
      //    Pair<CatalogPlugin, Identifier> catalogIdentifier = SparkUtil.catalogAndIdentifier(nameParts,
      //        catalogName ->  {
      //          try {
      //            return catalogManager.catalog(catalogName);
      //          } catch (Exception e) {
      //            return null;
      //          }
      //        },
      //        Identifier::of,
      //        defaultCatalog,
      //        currentNamespace
      //    );
      LOG.warn("Catalog creation for inputName={} and options {} failed, because parameter " +
          "'warehouse' is not set, Nessie can't store data.", inputName, options);
      throw new IllegalStateException("Parameter 'warehouse' not set, Nessie can't store data.");
    }
    final String requestedRef = options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF));
    NessieApiV1 api = createNessieClientBuilder(options.get(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL))
        .fromConfig(x -> options.get(removePrefix.apply(x)))
        .build(NessieApiV1.class);
    this.client = new NessieIcebergClient(api, requestedRef, null);
  }

  private static NessieClientBuilder<?> createNessieClientBuilder(String customBuilder) {
    NessieClientBuilder<?> clientBuilder;
    if (customBuilder != null) {
      try {
        clientBuilder = DynMethods.builder("builder").impl(customBuilder).build().asStatic().invoke();
      } catch (Exception e) {
        throw new RuntimeException(String.format("Failed to use custom NessieClientBuilder '%s'.", customBuilder), e);
      }
    } else {
      clientBuilder = HttpClientBuilder.builder();
    }
    return clientBuilder;
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
    TableReference tr = TableReference.parse(tableIdentifier.name());
    Preconditions.checkArgument(!tr.hasTimestamp(), "Invalid table name: # is only allowed for hashes (reference by " +
        "timestamp is not supported)");
    NessieIcebergClient newClient = client;
    if (tr.getReference() != null) {
      newClient = new NessieIcebergClient(client.getApi(), tr.getReference(), tr.getHash());
    }
    return new NessieTableOperations(
        ContentKey.of(org.projectnessie.model.Namespace.of(tableIdentifier.namespace().levels()), tr.getName()),
        newClient,
        fileIO,
        catalogOptions);
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
    return client.tableStream(namespace).collect(Collectors.toList());
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    client.getRef().checkMutable();

    IcebergTable existingTable = client.table(identifier);
    if (existingTable == null) {
      return false;
    }

    if (purge) {
      LOG.info("Purging data for table {} was set to true but is ignored", identifier.toString());
    }

    CommitMultipleOperationsBuilder commitBuilderBase = client.getApi().commitMultipleOperations()
        .commitMeta(NessieUtil.buildCommitMetadata(String.format("Iceberg delete table %s", identifier),
            catalogOptions))
        .operation(Operation.Delete.of(NessieUtil.toKey(identifier)));

    // We try to drop the table. Simple retry after ref update.
    boolean threw = true;
    try {
      Tasks.foreach(commitBuilderBase)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> client.refresh())
          .run(commitBuilder -> {
            Branch branch = commitBuilder
                .branch(client.getRef().getAsBranch())
                .commit();
            client.getRef().updateReference(branch);
          }, BaseNessieClientServerException.class);
      threw = false;
    } catch (NessieConflictException e) {
      LOG.error("Cannot drop table: failed after retry (update ref '{}' and retry)", client.getRef().getName(), e);
    } catch (NessieNotFoundException e) {
      LOG.error("Cannot drop table: ref '{}' is no longer valid.", client.getRef().getName(), e);
    } catch (BaseNessieClientServerException e) {
      LOG.error("Cannot drop table: unknown error", e);
    }
    return !threw;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier toOriginal) {
    client.getRef().checkMutable();

    TableIdentifier to = NessieUtil.removeCatalogName(toOriginal, name());

    IcebergTable existingFromTable = client.table(from);
    if (existingFromTable == null) {
      throw new NoSuchTableException("table '%s' doesn't exists", from.name());
    }
    IcebergTable existingToTable = client.table(to);
    if (existingToTable != null) {
      throw new AlreadyExistsException("table '%s' already exists", to.name());
    }

    CommitMultipleOperationsBuilder operations = client.getApi().commitMultipleOperations()
        .commitMeta(NessieUtil.buildCommitMetadata(String.format("Iceberg rename table from '%s' to '%s'",
            from, to), catalogOptions))
        .operation(Operation.Put.of(NessieUtil.toKey(to), existingFromTable, existingFromTable))
        .operation(Operation.Delete.of(NessieUtil.toKey(from)));

    try {
      Tasks.foreach(operations)
          .retry(5)
          .stopRetryOn(NessieNotFoundException.class)
          .throwFailureWhenFinished()
          .onFailure((o, exception) -> client.refresh())
          .run(ops -> {
            Branch branch = ops
                .branch(client.getRef().getAsBranch())
                .commit();
            client.getRef().updateReference(branch);
          }, BaseNessieClientServerException.class);
    } catch (NessieNotFoundException e) {
      // important note: the NotFoundException refers to the ref only. If a table was not found it would imply that the
      // another commit has deleted the table from underneath us. This would arise as a Conflict exception as opposed to
      // a not found exception. This is analogous to a merge conflict in git when a table has been changed by one user
      // and removed by another.
      throw new RuntimeException(String.format("Cannot rename table '%s' to '%s': " +
          "ref '%s' no longer exists.", from.name(), to.name(), client.getRef().getName()), e);
    } catch (BaseNessieClientServerException e) {
      throw new CommitFailedException(e, "Cannot rename table '%s' to '%s': " +
          "the current reference is not up to date.", from.name(), to.name());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      throw new CommitStateUnknownException(ex);
    }
    // Intentionally just "throw through" Nessie's HttpClientException here and do not "special case"
    // just the "timeout" variant to propagate all kinds of network errors (e.g. connection reset).
    // Network code implementation details and all kinds of network devices can induce unexpected
    // behavior. So better be safe than sorry.
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    try {
      client.getRef().checkMutable();
      client.getApi().createNamespace()
          .reference(client.getRef().getReference())
          .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
          .create();
      client.refresh();
    } catch (NessieNamespaceAlreadyExistsException e) {
      throw new AlreadyExistsException(e, "Namespace '%s' already exists.", namespace);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(String.format("Cannot create Namespace '%s': " +
          "ref '%s' is no longer valid.", namespace, client.getRef().getName()), e);
    }
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    try {
      GetNamespacesResponse response = client.getApi().getMultipleNamespaces()
          .reference(client.getRef().getReference())
          .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
          .get();
      return response.getNamespaces().stream()
          .map(ns -> Namespace.of(ns.getElements().toArray(new String[0])))
          .collect(Collectors.toList());
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(
          String.format("Cannot list Namespaces starting from '%s': " +
              "ref '%s' is no longer valid.", namespace, client.getRef().getName()), e);
    }
  }

  /**
   * Load the given namespace but return an empty map because namespace properties are currently not supported.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return an empty map
   * @throws NoSuchNamespaceException If the namespace does not exist
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    try {
      client.getApi().getNamespace()
          .reference(client.getRef().getReference())
          .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
          .get();
    } catch (NessieNamespaceNotFoundException e) {
      throw new NoSuchNamespaceException(e, "Namespace '%s' does not exist.", namespace);
    } catch (NessieReferenceNotFoundException e) {
      throw new RuntimeException(String.format("Cannot load Namespace '%s': " +
          "ref '%s' is no longer valid.", namespace, client.getRef().getName()), e);
    }
    return ImmutableMap.of();
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    try {
      client.getRef().checkMutable();
      client.getApi().deleteNamespace()
          .reference(client.getRef().getReference())
          .namespace(org.projectnessie.model.Namespace.of(namespace.levels()))
          .delete();
      client.refresh();
      return true;
    } catch (NessieNamespaceNotFoundException e) {
      return false;
    } catch (NessieNotFoundException e) {
      LOG.error("Cannot drop Namespace '{}': ref '{}' is no longer valid.", namespace, client.getRef().getName(), e);
      return false;
    } catch (NessieNamespaceNotEmptyException e) {
      throw new NamespaceNotEmptyException(e, "Namespace '%s' is not empty. One or more tables exist.", namespace);
    }
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    throw new UnsupportedOperationException(
        "Cannot set properties for namespace '" + namespace +
            "': setProperties is not supported by the NessieCatalog");
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    throw new UnsupportedOperationException(
        "Cannot remove properties for namespace '" + namespace +
            "': removeProperties is not supported by the NessieCatalog");
  }

  @Override
  public void setConf(Configuration conf) {
    this.config = conf;
  }

  @Override
  public Configuration getConf() {
    return config;
  }

  public String currentHash() {
    return client.getRef().getHash();
  }

  @VisibleForTesting
  String currentRefName() {
    return client.getRef().getName();
  }

  @VisibleForTesting
  FileIO fileIO() {
    return fileIO;
  }
}
