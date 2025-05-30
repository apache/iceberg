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
package org.apache.iceberg.flink.sink.dynamic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.UpdatePartitionSpec;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates and maintains a view on Iceberg tables. Updates the Iceberg tables in case of schema,
 * branch, or partition changes.
 */
@Internal
class TableUpdater {

  private static final Logger LOG = LoggerFactory.getLogger(TableUpdater.class);
  private final TableMetadataCache cache;
  private final Catalog catalog;

  TableUpdater(TableMetadataCache cache, Catalog catalog) {
    this.cache = cache;
    this.catalog = catalog;
  }

  /**
   * Creates or updates a table to make sure that the given branch, schema, spec exists.
   *
   * @return a {@link Tuple3} of the new {@link Schema}, the status of the schema compared to the
   *     requested one, and the new {@link PartitionSpec#specId()}.
   */
  Tuple3<Schema, CompareSchemasVisitor.Result, PartitionSpec> update(
      TableIdentifier tableIdentifier, String branch, Schema schema, PartitionSpec spec) {
    findOrCreateTable(tableIdentifier, schema, spec);
    findOrCreateBranch(tableIdentifier, branch);
    Tuple2<Schema, CompareSchemasVisitor.Result> newSchema =
        findOrCreateSchema(tableIdentifier, schema);
    PartitionSpec newSpec = findOrCreateSpec(tableIdentifier, spec);
    return Tuple3.of(newSchema.f0, newSchema.f1, newSpec);
  }

  private void findOrCreateTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    Tuple2<Boolean, Exception> exists = cache.exists(identifier);
    if (Boolean.FALSE.equals(exists.f0)) {
      if (exists.f1 instanceof NoSuchNamespaceException) {
        SupportsNamespaces catalogWithNameSpace = (SupportsNamespaces) catalog;
        LOG.info("Namespace {} not found during table search. Creating namespace", identifier);
        try {
          catalogWithNameSpace.createNamespace(identifier.namespace());
        } catch (AlreadyExistsException e) {
          LOG.debug("Namespace {} created concurrently", identifier.namespace(), e);
        }

        createTable(identifier, schema, spec);
      } else {
        LOG.info("Table {} not found during table search. Creating table.", identifier);
        createTable(identifier, schema, spec);
      }
    }
  }

  private void createTable(TableIdentifier identifier, Schema schema, PartitionSpec spec) {
    try {
      Table table = catalog.createTable(identifier, schema, spec);
      cache.update(identifier, table);
    } catch (AlreadyExistsException e) {
      LOG.info("Table {} created concurrently. Skipping creation.", identifier, e);
    }
  }

  private void findOrCreateBranch(TableIdentifier identifier, String branch) {
    String fromCache = cache.branch(identifier, branch);
    if (fromCache == null) {
      try {
        // TODO: Which snapshot should be used to create the branch?
        catalog.loadTable(identifier).manageSnapshots().createBranch(branch).commit();
        LOG.info("Branch {} for {} created", branch, identifier);
      } catch (Exception e) {
        LOG.info(
            "Failed to create branch {} for {}. Maybe created concurrently?",
            branch,
            identifier,
            e);
      }
    }
  }

  private Tuple2<Schema, CompareSchemasVisitor.Result> findOrCreateSchema(
      TableIdentifier identifier, Schema schema) {
    Tuple2<Schema, CompareSchemasVisitor.Result> fromCache = cache.schema(identifier, schema);
    if (fromCache.f1 != CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED) {
      return fromCache;
    } else {
      Table table = catalog.loadTable(identifier);
      Schema tableSchema = table.schema();
      CompareSchemasVisitor.Result result = CompareSchemasVisitor.visit(schema, tableSchema, true);
      switch (result) {
        case SAME:
        case DATA_ADAPTION_NEEDED:
          cache.update(identifier, table);
          return Tuple2.of(tableSchema, result);
        case SCHEMA_UPDATE_NEEDED:
          LOG.info(
              "Triggering schema update for table {} {} to {}", identifier, tableSchema, schema);
          UpdateSchema updateApi = table.updateSchema();
          EvolveSchemaVisitor.visit(updateApi, tableSchema, schema);

          try {
            updateApi.commit();
            cache.invalidate(identifier);
            Tuple2<Schema, CompareSchemasVisitor.Result> comparisonAfterMigration =
                cache.schema(identifier, schema);
            Schema newSchema = comparisonAfterMigration.f0;
            LOG.info("Table {} schema updated from {} to {}", identifier, tableSchema, newSchema);
            return comparisonAfterMigration;
          } catch (CommitFailedException e) {
            LOG.info(
                "Schema update failed for {} from {} to {}", identifier, tableSchema, schema, e);
            Tuple2<Schema, CompareSchemasVisitor.Result> newSchema =
                cache.schema(identifier, schema);
            if (newSchema.f1 != CompareSchemasVisitor.Result.SCHEMA_UPDATE_NEEDED) {
              LOG.info("Table {} schema updated concurrently to {}", identifier, schema);
              return newSchema;
            } else {
              throw e;
            }
          }
        default:
          throw new IllegalArgumentException("Unknown comparison result");
      }
    }
  }

  private PartitionSpec findOrCreateSpec(TableIdentifier identifier, PartitionSpec targetSpec) {
    PartitionSpec currentSpec = cache.spec(identifier, targetSpec);
    if (currentSpec != null) {
      return currentSpec;
    }

    Table table = catalog.loadTable(identifier);
    currentSpec = table.spec();

    PartitionSpecEvolution.PartitionSpecChanges result =
        PartitionSpecEvolution.evolve(currentSpec, targetSpec);
    if (result.isEmpty()) {
      LOG.info("Returning equivalent existing spec {} for {}", currentSpec, targetSpec);
      return currentSpec;
    }

    LOG.info(
        "Spec for table {} has been altered. Updating from {} to {}",
        identifier,
        currentSpec,
        targetSpec);
    UpdatePartitionSpec updater = table.updateSpec();
    result.termsToRemove().forEach(updater::removeField);
    result.termsToAdd().forEach(updater::addField);

    try {
      updater.commit();
    } catch (CommitFailedException e) {
      cache.invalidate(identifier);
      PartitionSpec newSpec = cache.spec(identifier, targetSpec);
      result = PartitionSpecEvolution.evolve(targetSpec, newSpec);
      if (result.isEmpty()) {
        LOG.debug("Table {} partition spec updated concurrently to {}", identifier, newSpec);
        return newSpec;
      } else {
        LOG.error(
            "Partition spec update failed for {} from {} to {}",
            identifier,
            currentSpec,
            targetSpec,
            e);
        throw e;
      }
    }

    cache.invalidate(identifier);
    return cache.spec(identifier, targetSpec);
  }
}
