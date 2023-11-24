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

import java.util.EnumSet;
import java.util.Map;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.error.NessieReferenceConflictException;
import org.projectnessie.model.Conflict;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
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

  public static final String NESSIE_GC_NO_WARNING_PROPERTY = "nessie.gc.no-warning";

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
    refreshFromMetadataLocation(
        metadataLocation,
        null,
        2,
        location ->
            NessieUtil.updateTableMetadataWithNessieSpecificProperties(
                TableMetadataParser.read(fileIO, location),
                location,
                table,
                key.toString(),
                reference));
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    String newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);

    String refName = client.refName();
    boolean failure = false;
    try {
      String contentId = table == null ? null : table.getId();
      client.commitTable(base, metadata, newMetadataLocation, contentId, key);
    } catch (NessieConflictException ex) {
      failure = true;
      if (ex instanceof NessieReferenceConflictException) {
        // Throws a specialized exception, if possible
        maybeThrowSpecializedException((NessieReferenceConflictException) ex);
      }
      throw new CommitFailedException(
          ex,
          "Cannot commit: Reference hash is out of date. "
              + "Update the reference '%s' and try again",
          refName);
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      failure = true;
      throw new RuntimeException(
          String.format("Cannot commit: Reference '%s' no longer exists", refName), ex);
    } finally {
      if (failure) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private static void maybeThrowSpecializedException(NessieReferenceConflictException ex) {
    NessieUtil.extractSingleConflict(
            ex,
            EnumSet.of(
                Conflict.ConflictType.NAMESPACE_ABSENT,
                Conflict.ConflictType.NAMESPACE_NOT_EMPTY,
                Conflict.ConflictType.KEY_DOES_NOT_EXIST,
                Conflict.ConflictType.KEY_EXISTS))
        .ifPresent(
            conflict -> {
              switch (conflict.conflictType()) {
                case NAMESPACE_ABSENT:
                  throw new NoSuchNamespaceException(
                      ex, "Namespace does not exist: %s", conflict.key());
                case NAMESPACE_NOT_EMPTY:
                  throw new NamespaceNotEmptyException(
                      ex, "Namespace not empty: %s", conflict.key());
                case KEY_DOES_NOT_EXIST:
                  throw new NoSuchTableException(
                      ex, "Table or view does not exist: %s", conflict.key());
                case KEY_EXISTS:
                  throw new AlreadyExistsException(
                      ex, "Table or view already exists: %s", conflict.key());
                default:
                  // Explicit fall-through
                  break;
              }
            });
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
