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

import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieBadRequestException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.IcebergView;
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

  /** Create a nessie table operations given a table identifier. */
  NessieTableOperations(ContentKey key, NessieIcebergClient client, FileIO fileIO) {
    this.key = key;
    this.client = client;
    this.fileIO = fileIO;
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
              "Failed to refresh as ref '%s' is no longer valid.", client.getRef().getName()),
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
                    () -> {
                      if (content instanceof IcebergView) {
                        return new AlreadyExistsException(
                            "View with same name already exists: %s", key);
                      } else {
                        return new AlreadyExistsException(
                            "Cannot refresh Iceberg table: "
                                + "Nessie points to a non-Iceberg object for path: %s.",
                            key);
                      }
                    });
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

    boolean failure = false;
    try {
      String contentId = table == null ? null : table.getId();
      client.commitTable(base, metadata, newMetadataLocation, contentId, key);
    } catch (NessieConflictException | NessieNotFoundException | HttpClientException ex) {
      if (ex instanceof NessieConflictException || ex instanceof NessieNotFoundException) {
        failure = true;
      }
      NessieUtil.handleExceptionsForCommits(ex, client.refName(), Content.Type.ICEBERG_TABLE);
    } catch (NessieBadRequestException ex) {
      failure = true;
      throw NessieUtil.handleBadRequestForCommit(client, key, Content.Type.ICEBERG_TABLE)
          .orElse(ex);
    } finally {
      if (failure) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
