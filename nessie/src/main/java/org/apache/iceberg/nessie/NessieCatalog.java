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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.projectnessie.client.NessieClientBuilder;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.api.NessieApiV2;
import org.projectnessie.client.http.HttpClientBuilder;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.TableReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie implementation of Iceberg Catalog.
 *
 * <p>A note on namespaces: Nessie namespaces are implicit and do not need to be explicitly created
 * or deleted. The create and delete namespace methods are no-ops for the NessieCatalog. One can
 * still list namespaces that have objects stored in them to assist with namespace-centric catalog
 * exploration.
 */
public class NessieCatalog extends BaseMetastoreCatalog
    implements AutoCloseable, SupportsNamespaces, Configurable<Object> {

  private static final Logger LOG = LoggerFactory.getLogger(NessieCatalog.class);
  private static final Joiner SLASH = Joiner.on("/");
  private static final String NAMESPACE_LOCATION_PROPS = "location";
  private NessieIcebergClient client;
  private String warehouseLocation;
  private Object config;
  private String name;
  private FileIO fileIO;
  private Map<String, String> catalogOptions;
  private CloseableGroup closeableGroup;

  public NessieCatalog() {}

  @SuppressWarnings("checkstyle:HiddenField")
  @Override
  public void initialize(String name, Map<String, String> options) {
    Map<String, String> catalogOptions = ImmutableMap.copyOf(options);
    String fileIOImpl =
        options.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, "org.apache.iceberg.hadoop.HadoopFileIO");
    // remove nessie prefix
    final Function<String, String> removePrefix =
        x -> x.replace(NessieUtil.NESSIE_CONFIG_PREFIX, "");
    final String requestedRef =
        options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF));
    String requestedHash =
        options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF_HASH));

    NessieClientBuilder<?> nessieClientBuilder =
        createNessieClientBuilder(
                options.get(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL))
            .fromConfig(x -> options.get(removePrefix.apply(x)));
    // default version is set to v1.
    final String apiVersion =
        options.getOrDefault(removePrefix.apply(NessieUtil.CLIENT_API_VERSION), "1");
    NessieApiV1 api;
    switch (apiVersion) {
      case "1":
        api = nessieClientBuilder.build(NessieApiV1.class);
        break;
      case "2":
        api = nessieClientBuilder.build(NessieApiV2.class);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported %s: %s. Can only be 1 or 2",
                removePrefix.apply(NessieUtil.CLIENT_API_VERSION), apiVersion));
    }

    initialize(
        name,
        new NessieIcebergClient(api, requestedRef, requestedHash, catalogOptions),
        CatalogUtil.loadFileIO(fileIOImpl, options, config),
        catalogOptions);
  }

  /**
   * An alternative way to initialize the catalog using a pre-configured {@link NessieIcebergClient}
   * and {@link FileIO} instance.
   *
   * @param name The name of the catalog, defaults to "nessie" if <code>null</code>
   * @param client The pre-configured {@link NessieIcebergClient} instance to use
   * @param fileIO The {@link FileIO} instance to use
   * @param catalogOptions The catalog options to use
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public void initialize(
      String name, NessieIcebergClient client, FileIO fileIO, Map<String, String> catalogOptions) {
    this.name = name == null ? "nessie" : name;
    this.client = Preconditions.checkNotNull(client, "client must be non-null");
    this.fileIO = Preconditions.checkNotNull(fileIO, "fileIO must be non-null");
    this.catalogOptions =
        Preconditions.checkNotNull(catalogOptions, "catalogOptions must be non-null");
    this.warehouseLocation = validateWarehouseLocation(name, catalogOptions);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(client);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.setSuppressCloseFailure(true);
  }

  @SuppressWarnings("checkstyle:HiddenField")
  private String validateWarehouseLocation(String name, Map<String, String> catalogOptions) {
    String warehouseLocation = catalogOptions.get(CatalogProperties.WAREHOUSE_LOCATION);
    if (warehouseLocation == null) {
      // Explicitly log a warning, otherwise the thrown exception can get list in the "silent-ish
      // catch"
      // in o.a.i.spark.Spark3Util.catalogAndIdentifier(o.a.s.sql.SparkSession, List<String>,
      //     o.a.s.sql.connector.catalog.CatalogPlugin)
      // in the code block
      //    Pair<CatalogPlugin, Identifier> catalogIdentifier =
      // SparkUtil.catalogAndIdentifier(nameParts,
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
      LOG.warn(
          "Catalog creation for inputName={} and options {} failed, because parameter "
              + "'warehouse' is not set, Nessie can't store data.",
          name,
          catalogOptions);
      throw new IllegalStateException("Parameter 'warehouse' not set, Nessie can't store data.");
    }
    return warehouseLocation;
  }

  private static NessieClientBuilder<?> createNessieClientBuilder(String customBuilder) {
    NessieClientBuilder<?> clientBuilder;
    if (customBuilder != null) {
      try {
        clientBuilder =
            DynMethods.builder("builder").impl(customBuilder).build().asStatic().invoke();
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to use custom NessieClientBuilder '%s'.", customBuilder), e);
      }
    } else {
      clientBuilder = HttpClientBuilder.builder();
    }
    return clientBuilder;
  }

  @Override
  public void close() throws IOException {
    if (null != closeableGroup) {
      closeableGroup.close();
    }
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  protected TableOperations newTableOps(TableIdentifier tableIdentifier) {
    TableReference tr = parseTableReference(tableIdentifier);
    return new NessieTableOperations(
        ContentKey.of(
            org.projectnessie.model.Namespace.of(tableIdentifier.namespace().levels()),
            tr.getName()),
        client.withReference(tr.getReference(), tr.getHash()),
        fileIO,
        catalogOptions);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier table) {
    String location;
    if (table.hasNamespace()) {
      String baseLocation = SLASH.join(warehouseLocation, table.namespace().toString());
      try {
        baseLocation =
            loadNamespaceMetadata(table.namespace())
                .getOrDefault(NAMESPACE_LOCATION_PROPS, baseLocation);
      } catch (NoSuchNamespaceException e) {
        // do nothing we want the same behavior that if the location is not defined
      }
      location = SLASH.join(baseLocation, table.name());
    } else {
      location = SLASH.join(warehouseLocation, table.name());
    }
    // Different tables with same table name can exist across references in Nessie.
    // To avoid sharing same table path between two tables with same name, use uuid in the table
    // path.
    return location + "_" + UUID.randomUUID();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return client.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    TableReference tableReference = parseTableReference(identifier);
    return client
        .withReference(tableReference.getReference(), tableReference.getHash())
        .dropTable(identifierWithoutTableReference(identifier, tableReference), purge);
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    TableReference fromTableReference = parseTableReference(from);
    TableReference toTableReference = parseTableReference(to);
    String fromReference =
        fromTableReference.hasReference()
            ? fromTableReference.getReference()
            : client.getRef().getName();
    String toReference =
        toTableReference.hasReference()
            ? toTableReference.getReference()
            : client.getRef().getName();
    Preconditions.checkArgument(
        fromReference.equalsIgnoreCase(toReference),
        "from: %s and to: %s reference name must be same",
        fromReference,
        toReference);

    client
        .withReference(fromTableReference.getReference(), fromTableReference.getHash())
        .renameTable(
            identifierWithoutTableReference(from, fromTableReference),
            NessieUtil.removeCatalogName(
                identifierWithoutTableReference(to, toTableReference), name()));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    client.createNamespace(namespace, metadata);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return client.listNamespaces(namespace);
  }

  /**
   * Load the given namespace and return its properties.
   *
   * @param namespace a namespace. {@link Namespace}
   * @return a string map of properties for the given namespace
   * @throws NoSuchNamespaceException If the namespace does not exist
   */
  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return client.loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    return client.dropNamespace(namespace);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties) {
    return client.setProperties(namespace, properties);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties) {
    return client.removeProperties(namespace, properties);
  }

  @Override
  public void setConf(Object conf) {
    this.config = conf;
  }

  @VisibleForTesting
  String currentHash() {
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

  private TableReference parseTableReference(TableIdentifier tableIdentifier) {
    TableReference tr = TableReference.parse(tableIdentifier.name());
    Preconditions.checkArgument(
        !tr.hasTimestamp(),
        "Invalid table name: # is only allowed for hashes (reference by "
            + "timestamp is not supported)");
    return tr;
  }

  private TableIdentifier identifierWithoutTableReference(
      TableIdentifier identifier, TableReference tableReference) {
    if (tableReference.hasReference()) {
      return TableIdentifier.of(identifier.namespace(), tableReference.getName());
    }
    return identifier;
  }

  @Override
  protected Map<String, String> properties() {
    return catalogOptions == null ? ImmutableMap.of() : catalogOptions;
  }
}
