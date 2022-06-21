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
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.view.MetastoreViewCatalog;
import org.apache.iceberg.view.MetastoreViewOperations;
import org.projectnessie.client.NessieConfigConstants;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.TableReference;

public class NessieViewCatalog extends MetastoreViewCatalog implements AutoCloseable {

  private static final Joiner SLASH = Joiner.on("/");
  private NessieIcebergClient client;
  private String warehouseLocation;
  private final Configuration config;
  private String name;
  private FileIO fileIO;
  private Map<String, String> catalogOptions;
  private CloseableGroup closeableGroup;

  public NessieViewCatalog(Configuration conf) {
    super(conf);
    this.config = conf;
  }

  @SuppressWarnings("checkstyle:HiddenField")
  @Override
  public void initialize(String name, Map<String, String> options) {
    Map<String, String> catalogOptions = ImmutableMap.copyOf(options);
    String fileIOImpl = options.get(CatalogProperties.FILE_IO_IMPL);
    // remove nessie prefix
    final Function<String, String> removePrefix = x -> x.replace(NessieUtil.NESSIE_CONFIG_PREFIX, "");
    final String requestedRef = options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF));
    String requestedHash = options.get(removePrefix.apply(NessieConfigConstants.CONF_NESSIE_REF_HASH));
    NessieApiV1 api =
        NessieUtil.createNessieClientBuilder(
                options.get(NessieConfigConstants.CONF_NESSIE_CLIENT_BUILDER_IMPL))
            .fromConfig(x -> options.get(removePrefix.apply(x)))
            .build(NessieApiV1.class);

    initialize(name,
        new NessieIcebergClient(api, requestedRef, requestedHash, catalogOptions),
        fileIOImpl == null ? new HadoopFileIO(config) : CatalogUtil.loadFileIO(fileIOImpl, options, config),
        catalogOptions);
  }

  /**
   * An alternative way to initialize the catalog using a pre-configured {@link NessieIcebergClient} and {@link FileIO}
   * instance.
   * @param name The name of the catalog, defaults to "nessie" if <code>null</code>
   * @param client The pre-configured {@link NessieIcebergClient} instance to use
   * @param fileIO The {@link FileIO} instance to use
   * @param catalogOptions The catalog options to use
   */
  @SuppressWarnings("checkstyle:HiddenField")
  public void initialize(String name, NessieIcebergClient client, FileIO fileIO, Map<String, String> catalogOptions) {
    this.name = name == null ? "nessie" : name;
    this.client = Preconditions.checkNotNull(client, "client must be non-null");
    this.fileIO = Preconditions.checkNotNull(fileIO, "fileIO must be non-null");
    this.catalogOptions = Preconditions.checkNotNull(catalogOptions, "catalogOptions must be non-null");
    this.warehouseLocation = NessieUtil.validateWarehouseLocation(name, catalogOptions);
    this.closeableGroup = new CloseableGroup();
    closeableGroup.addCloseable(client);
    closeableGroup.addCloseable(fileIO);
    closeableGroup.setSuppressCloseFailure(true);
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier view) {
    if (view.hasNamespace()) {
      return SLASH.join(warehouseLocation, view.namespace().toString(), view.name());
    }
    return SLASH.join(warehouseLocation, view.name());
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return client.listViews(namespace);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return client.dropView(identifier, false);
  }

  @Override
  public boolean dropView(TableIdentifier identifier, boolean purge) {
    return client.dropView(identifier, purge);
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    client.renameView(from, NessieUtil.removeCatalogName(to, name()));
  }

  @Override
  protected MetastoreViewOperations newViewOps(TableIdentifier viewName) {
    TableReference tr = TableReference.parse(viewName.name());
    Preconditions.checkArgument(!tr.hasTimestamp(), "Invalid view name: # is only allowed for hashes (reference by " +
        "timestamp is not supported)");
    return new NessieViewOperations(
        ContentKey.of(org.projectnessie.model.Namespace.of(viewName.namespace().levels()), tr.getName()),
        client.withReference(tr.getReference(), tr.getHash()),
        fileIO,
        catalogOptions);
  }

  @Override
  public void close() throws Exception {
    if (null != closeableGroup) {
      closeableGroup.close();
    }
  }
}
