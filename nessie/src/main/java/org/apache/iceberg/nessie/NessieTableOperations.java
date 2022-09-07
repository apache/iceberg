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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.GenericMetadata;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Nessie implementation of Iceberg TableOperations. */
public class NessieTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(NessieTableOperations.class);

  /**
   * Name of the `{@link TableMetadata} property that holds the Nessie commit-ID from which the
   * metadata has been loaded.
   */
  public static final String NESSIE_COMMIT_ID_PROPERTY = "nessie.commit.id";

  private final NessieIcebergClient client;
  private final ContentKey key;
  private IcebergTable table;
  private final FileIO fileIO;
  private final Map<String, String> catalogOptions;

  /** Create a nessie table operations given a table identifier. */
  NessieTableOperations(
      ContentKey key,
      NessieIcebergClient client,
      FileIO fileIO,
      Map<String, String> catalogOptions) {
    this.key = key;
    this.client = client;
    this.fileIO = fileIO;
    this.catalogOptions = catalogOptions;
  }

  @Override
  protected String tableName() {
    return key.toString();
  }

  private TableMetadata loadTableMetadata(String metadataLocation, Reference reference) {
    // Update the TableMetadata with the Content of NessieTableState.
    TableMetadata deserialized =
        NessieUtil.tableMetadataFromIcebergTable(io(), table, metadataLocation);
    Map<String, String> newProperties = Maps.newHashMap(deserialized.properties());
    newProperties.put(NESSIE_COMMIT_ID_PROPERTY, reference.getHash());
    // To prevent accidental deletion of files that are still referenced by other branches/tags,
    // setting GC_ENABLED to false. So that all Iceberg's gc operations like expire_snapshots,
    // remove_orphan_files, drop_table with purge will fail with an error.
    // Nessie CLI will provide a reference aware GC functionality for the expired/unreferenced
    // files.
    newProperties.put(TableProperties.GC_ENABLED, "false");
    TableMetadata.Builder builder =
        TableMetadata.buildFrom(deserialized)
            .setPreviousFileLocation(null)
            .setCurrentSchema(table.getSchemaId())
            .setDefaultSortOrder(table.getSortOrderId())
            .setDefaultPartitionSpec(table.getSpecId())
            .withMetadataLocation(metadataLocation)
            .setProperties(newProperties);
    if (table.getSnapshotId() != -1) {
      builder.setBranchSnapshot(table.getSnapshotId(), SnapshotRef.MAIN_BRANCH);
    }
    LOG.info(
        "loadTableMetadata for '{}' from location '{}' at '{}'", key, metadataLocation, reference);

    return builder.discardChanges().build();
  }

  @Override
  protected void doRefresh() {
    try {
      client.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(
          String.format(
              "Failed to refresh as ref '%s' " + "is no longer valid.", client.getRef().getName()),
          e);
    }
    String metadataLocation = null;
    Reference reference = client.getRef().getReference();
    try {
      Content content = client.getApi().getContent().key(key).reference(reference).get().get(key);
      LOG.debug("Content '{}' at '{}': {}", key, reference, content);
      if (content == null) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchTableException("No such table '%s' in '%s'", key, reference);
        }
      } else {
        this.table =
            content
                .unwrap(IcebergTable.class)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            String.format(
                                "Cannot refresh iceberg table: "
                                    + "Nessie points to a non-Iceberg object for path: %s.",
                                key)));
        metadataLocation = table.getMetadataLocation();
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such table '%s'", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, null, 2, l -> loadTableMetadata(l, reference));
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    UpdateableReference updateableReference = client.getRef();

    updateableReference.checkMutable();

    Branch current = updateableReference.getAsBranch();
    Branch expectedHead = current;
    if (base != null) {
      String metadataCommitId = base.property(NESSIE_COMMIT_ID_PROPERTY, expectedHead.getHash());
      if (metadataCommitId != null) {
        expectedHead = Branch.of(expectedHead.getName(), metadataCommitId);
      }
    }

    String newMetadataLocation =
        (base == null) && (metadata.metadataFileLocation() != null)
            ? metadata.metadataFileLocation()
            : writeNewMetadata(metadata, currentVersion() + 1);

    boolean delete = true;
    try {
      ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder();
      if (table != null) {
        newTableBuilder.id(table.getId());
      }
      Snapshot snapshot = metadata.currentSnapshot();
      long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;

      JsonNode newMetadata = NessieUtil.tableMetadataAsJsonNode(metadata);
      IcebergTable newTable =
          newTableBuilder
              .snapshotId(snapshotId)
              .schemaId(metadata.currentSchemaId())
              .specId(metadata.defaultSpecId())
              .sortOrderId(metadata.defaultSortOrderId())
              .metadataLocation(newMetadataLocation)
              .metadata(
                  GenericMetadata.of("org.apache:iceberg:" + metadata.formatVersion(), newMetadata))
              .build();

      LOG.debug(
          "Committing '{}' against '{}', current is '{}': {}",
          key,
          expectedHead,
          current.getHash(),
          newTable);
      ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
      builder.message(buildCommitMsg(base, metadata));
      if (isSnapshotOperation(base, metadata)) {
        builder.putProperties("iceberg.operation", snapshot.operation());
      }
      Branch branch =
          client
              .getApi()
              .commitMultipleOperations()
              .operation(Operation.Put.of(key, newTable, table))
              .commitMeta(NessieUtil.catalogOptions(builder, catalogOptions).build())
              .branch(expectedHead)
              .commit();
      LOG.info(
          "Committed '{}' against '{}', expected commit-id was '{}'",
          key,
          branch,
          expectedHead.getHash());
      updateableReference.updateReference(branch);

      delete = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(
          ex,
          "Cannot commit: Reference hash is out of date. "
              + "Update the reference '%s' and try again",
          updateableReference.getName());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      delete = false;
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(
          String.format(
              "Cannot commit: Reference '%s' no longer exists", updateableReference.getName()),
          ex);
    } finally {
      if (delete) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private boolean isSnapshotOperation(TableMetadata base, TableMetadata metadata) {
    Snapshot snapshot = metadata.currentSnapshot();
    return snapshot != null
        && (base == null
            || base.currentSnapshot() == null
            || snapshot.snapshotId() != base.currentSnapshot().snapshotId());
  }

  private String buildCommitMsg(TableMetadata base, TableMetadata metadata) {
    if (isSnapshotOperation(base, metadata)) {
      return String.format(
          "Iceberg %s against %s", metadata.currentSnapshot().operation(), tableName());
    } else if (base != null && metadata.currentSchemaId() != base.currentSchemaId()) {
      return String.format("Iceberg schema change against %s", tableName());
    }
    return String.format("Iceberg commit against %s", tableName());
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
