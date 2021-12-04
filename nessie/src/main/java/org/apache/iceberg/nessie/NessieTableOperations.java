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

import java.util.Map;
import java.util.function.Predicate;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergTable;
import org.projectnessie.model.Operation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Nessie implementation of Iceberg TableOperations.
 */
public class NessieTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(NessieTableOperations.class);

  private final NessieApiV1 api;
  private final ContentKey key;
  private final UpdateableReference reference;
  private IcebergTable table;
  private final FileIO fileIO;
  private final Map<String, String> catalogOptions;

  /**
   * Create a nessie table operations given a table identifier.
   */
  NessieTableOperations(
      ContentKey key,
      UpdateableReference reference,
      NessieApiV1 api,
      FileIO fileIO,
      Map<String, String> catalogOptions) {
    this.key = key;
    this.reference = reference;
    this.api = api;
    this.fileIO = fileIO;
    this.catalogOptions = catalogOptions;
  }

  @Override
  protected String tableName() {
    return key.toString();
  }

  @Override
  protected void refreshFromMetadataLocation(String newLocation, Predicate<Exception> shouldRetry, int numRetries) {
    super.refreshFromMetadataLocation(newLocation, shouldRetry, numRetries, this::loadTableMetadata);
  }

  private TableMetadata loadTableMetadata(String metadataLocation) {
    // Update the TableMetadata with the Content of NessieTableState.
    return TableMetadata.buildFrom(TableMetadataParser.read(io(), metadataLocation))
        .setCurrentSnapshot(table.getSnapshotId())
        .setCurrentSchema(table.getSchemaId())
        .setDefaultSortOrder(table.getSortOrderId())
        .setDefaultPartitionSpec(table.getSpecId())
        .discardChanges()
        .build();
  }

  @Override
  protected void doRefresh() {
    try {
      reference.refresh(api);
    } catch (NessieNotFoundException e) {
      throw new RuntimeException("Failed to refresh as ref is no longer valid.", e);
    }
    String metadataLocation = null;
    try {
      Content content = api.getContent().key(key).reference(reference.getReference()).get()
          .get(key);
      LOG.debug("Content '{}' at '{}': {}", key, reference.getReference(), content);
      if (content == null) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchTableException("No such table %s in %s", key, reference.getReference());
        }
      } else {
        this.table = content.unwrap(IcebergTable.class)
            .orElseThrow(() ->
                new IllegalStateException("Cannot refresh iceberg table: " +
                    String.format("Nessie points to a non-Iceberg object for path: %s.", key)));
        metadataLocation = table.getMetadataLocation();
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such table %s", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, 2);
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    reference.checkMutable();

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean delete = true;
    try {
      ImmutableIcebergTable.Builder newTableBuilder = ImmutableIcebergTable.builder();
      if (table != null) {
        newTableBuilder.id(table.getId());
      }
      Snapshot snapshot = metadata.currentSnapshot();
      long snapshotId = snapshot != null ? snapshot.snapshotId() : -1L;
      IcebergTable newTable = newTableBuilder
          .snapshotId(snapshotId)
          .schemaId(metadata.currentSchemaId())
          .specId(metadata.defaultSpecId())
          .sortOrderId(metadata.defaultSortOrderId())
          .metadataLocation(newMetadataLocation)
          .build();

      LOG.debug("Committing '{}' against '{}': {}", key, reference.getReference(), newTable);
      ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
      builder.message(buildCommitMsg(base, metadata));
      if (isSnapshotOperation(base, metadata)) {
        builder.putProperties("iceberg.operation", snapshot.operation());
      }
      Branch branch = api.commitMultipleOperations()
          .operation(Operation.Put.of(key, newTable, table))
          .commitMeta(NessieUtil.catalogOptions(builder, catalogOptions).build())
          .branch(reference.getAsBranch())
          .commit();
      reference.updateReference(branch);

      delete = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(ex, "Commit failed: Reference hash is out of date. " +
          "Update the reference %s and try again", reference.getName());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      delete = false;
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(
          String.format("Commit failed: Reference %s no longer exist", reference.getName()), ex);
    } finally {
      if (delete) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private boolean isSnapshotOperation(TableMetadata base, TableMetadata metadata) {
    Snapshot snapshot = metadata.currentSnapshot();
    return snapshot != null && (base == null || base.currentSnapshot() == null ||
        snapshot.snapshotId() != base.currentSnapshot().snapshotId());
  }

  private String buildCommitMsg(TableMetadata base, TableMetadata metadata) {
    if (isSnapshotOperation(base, metadata)) {
      return String.format("Iceberg %s against %s", metadata.currentSnapshot().operation(), tableName());
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
