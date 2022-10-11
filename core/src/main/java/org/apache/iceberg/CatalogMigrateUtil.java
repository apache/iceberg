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
   * Migrates tables from one catalog(source catalog) to another catalog(target catalog).
   *
   * <p>Supports bulk migrations with a multi-thread execution. Once the migration is success, table
   * would be dropped from the source catalog.
   *
   * <p>Users must make sure that no in-progress commits on the tables of source catalog during
   * migration.
   *
   * @param tableIdentifiers a list of {@link TableIdentifier} for the tables required to be
   *     migrated. If not specified, all the tables would be migrated.
   * @param sourceCatalog Source {@link Catalog} from which the tables are chosen
   * @param targetCatalog Target {@link Catalog} to which the tables need to be migrated
   * @param maxConcurrentMigrations Size of the thread pool used for migrate tables (If set to 0, no
   *     thread pool is used)
   * @param deleteEntriesFromSourceCatalog If set to true, after successful migration, delete the
   *     table entry from source catalog. This field is not applicable for HadoopCatalog.
   * @return Collection of table identifiers for successfully migrated tables
   */
  public static Collection<TableIdentifier> migrateTables(
      List<TableIdentifier> tableIdentifiers,
      Catalog sourceCatalog,
      Catalog targetCatalog,
      int maxConcurrentMigrations,
      boolean deleteEntriesFromSourceCatalog) {
    validate(sourceCatalog, targetCatalog, maxConcurrentMigrations);

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
    if (maxConcurrentMigrations > 0) {
      executorService = ThreadPools.newWorkerPool("migrate-tables", maxConcurrentMigrations);
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
                    sourceCatalog, targetCatalog, tableIdentifier, deleteEntriesFromSourceCatalog);
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
      Catalog sourceCatalog, Catalog targetCatalog, int maxConcurrentMigrations) {
    Preconditions.checkArgument(
        maxConcurrentMigrations >= 0,
        "maxConcurrentMigrations should have value >= 0,  value: " + maxConcurrentMigrations);
    Preconditions.checkArgument(sourceCatalog != null, "Invalid source catalog: null");
    Preconditions.checkArgument(targetCatalog != null, "Invalid target catalog: null");
    Preconditions.checkArgument(
        !targetCatalog.equals(sourceCatalog), "target catalog is same as source catalog");
  }

  private static void migrate(
      Catalog sourceCatalog,
      Catalog targetCatalog,
      TableIdentifier tableIdentifier,
      boolean deleteEntriesFromSourceCatalog) {
    // register the table to the target catalog
    TableOperations ops =
        ((HasTableOperations) sourceCatalog.loadTable(tableIdentifier)).operations();
    targetCatalog.registerTable(tableIdentifier, ops.current().metadataFileLocation());

    if (deleteEntriesFromSourceCatalog && !(sourceCatalog instanceof HadoopCatalog)) {
      // HadoopCatalog dropTable will delete the table files completely even when purge is false.
      // So, skip dropTable for HadoopCatalog.
      sourceCatalog.dropTable(tableIdentifier, false);
    }
  }
}
