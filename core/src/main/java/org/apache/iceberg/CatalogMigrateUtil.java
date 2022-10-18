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
package org.apache.iceberg;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.util.Tasks;
import org.apache.iceberg.util.ThreadPools;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogMigrateUtil {
  private static final Logger LOG = LoggerFactory.getLogger(CatalogMigrateUtil.class);

  private CatalogMigrateUtil() {}

  /**
   * Migrates tables from one catalog(source catalog) to another catalog(target catalog). After
   * successful migration, deletes the table entry from source catalog(not applicable for
   * HadoopCatalog).
   *
   * <p>Supports bulk migrations with a multi-thread execution.
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * migration.
   *
   * @param tableIdentifiers a list of {@link TableIdentifier} for the tables required to be
   *     migrated. If not specified, all the tables would be migrated
   * @param sourceCatalog Source {@link Catalog} from which the tables are chosen
   * @param targetCatalog Target {@link Catalog} to which the tables need to be migrated
   * @param maxThreadPoolSize Size of the thread pool used for migrate tables (If set to 0, no
   *     thread pool is used)
   * @return Collection of table identifiers for successfully migrated tables
   */
  public static Collection<TableIdentifier> migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      int maxThreadPoolSize) {
    return migrateTables(tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize, true);
  }

  /**
   * Register tables from one catalog(source catalog) to another catalog(target catalog). User has
   * to take care of deleting the tables from source catalog after registration.
   *
   * <p>Supports bulk registration with a multi-thread execution.
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * registration.
   *
   * @param tableIdentifiers a list of {@link TableIdentifier} for the tables required to be
   *     registered. If not specified, all the tables would be registered
   * @param sourceCatalog Source {@link Catalog} from which the tables are chosen
   * @param targetCatalog Target {@link Catalog} to which the tables need to be registered
   * @param maxThreadPoolSize Size of the thread pool used for registering tables (If set to 0, no
   *     thread pool is used)
   * @return Collection of table identifiers for successfully registered tables
   */
  public static Collection<TableIdentifier> registerTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      int maxThreadPoolSize) {
    return migrateTables(tableIdentifiers, sourceCatalog, targetCatalog, maxThreadPoolSize, false);
  }

  private static Collection<TableIdentifier> migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      int maxThreadPoolSize,
      boolean deleteEntriesFromSourceCatalog) {
    validate(sourceCatalog, targetCatalog, maxThreadPoolSize);

    List<TableIdentifier> identifiers;
    if (tableIdentifiers == null || tableIdentifiers.isEmpty()) {
      // fetch all the table identifiers from all the namespaces.
      List<Namespace> namespaces =
          (sourceCatalog instanceof SupportsNamespaces)
              ? ((SupportsNamespaces) sourceCatalog).listNamespaces()
              : ImmutableList.of(Namespace.empty());
      identifiers =
          namespaces.stream()
              .flatMap(namespace -> sourceCatalog.listTables(namespace).stream())
              .collect(Collectors.toList());
    } else {
      identifiers = tableIdentifiers;
    }

    ExecutorService executorService = null;
    if (maxThreadPoolSize > 0) {
      executorService = ThreadPools.newWorkerPool("migrate-tables", maxThreadPoolSize);
    }

    try {
      Collection<TableIdentifier> migratedTableIdentifiers = new ConcurrentLinkedQueue<>();
      Tasks.foreach(identifiers.stream().filter(Objects::nonNull))
          .retry(3)
          .stopRetryOn(NoSuchTableException.class, NoSuchNamespaceException.class)
          .suppressFailureWhenFinished()
          .executeWith(executorService)
          .onFailure(
              (tableIdentifier, exc) ->
                  LOG.warn("Unable to migrate table {}", tableIdentifier, exc))
          .run(
              tableIdentifier -> {
                migrate(
                    tableIdentifier, sourceCatalog, targetCatalog, deleteEntriesFromSourceCatalog);
                migratedTableIdentifiers.add(tableIdentifier);
              });
      return migratedTableIdentifiers;
    } finally {
      if (executorService != null) {
        executorService.shutdown();
      }
    }
  }

  private static void validate(
      Catalog sourceCatalog, Catalog targetCatalog, int maxThreadPoolSize) {
    Preconditions.checkArgument(
        maxThreadPoolSize >= 0,
        "maxThreadPoolSize should have value >= 0,  value: " + maxThreadPoolSize);
    Preconditions.checkArgument(sourceCatalog != null, "Invalid source catalog: null");
    Preconditions.checkArgument(targetCatalog != null, "Invalid target catalog: null");
    Preconditions.checkArgument(
        !targetCatalog.equals(sourceCatalog), "target catalog is same as source catalog");
  }

  private static void migrate(
      TableIdentifier tableIdentifier,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      boolean deleteEntriesFromSourceCatalog) {
    // register the table to the target catalog
    TableOperations ops = ((BaseTable) sourceCatalog.loadTable(tableIdentifier)).operations();
    targetCatalog.registerTable(tableIdentifier, ops.current().metadataFileLocation());

    if (deleteEntriesFromSourceCatalog && !(sourceCatalog instanceof HadoopCatalog)) {
      // HadoopCatalog dropTable will delete the table files completely even when purge is false.
      // So, skip dropTable for HadoopCatalog.
      sourceCatalog.dropTable(tableIdentifier, false);
    }
  }
}
