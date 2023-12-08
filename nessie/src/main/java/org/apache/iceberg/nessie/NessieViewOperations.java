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

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.view.BaseViewOperations;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
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

public class NessieViewOperations extends BaseViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(NessieViewOperations.class);

  private final NessieIcebergClient client;
  private final ContentKey key;
  private final FileIO fileIO;
  private IcebergView icebergView;

  NessieViewOperations(ContentKey key, NessieIcebergClient client, FileIO fileIO) {
    this.key = key;
    this.client = client;
    this.fileIO = fileIO;
  }

  @Override
  public void doRefresh() {
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
          throw new NoSuchViewException("View does not exist: %s in %s", key, reference);
        }
      } else {
        this.icebergView =
            content
                .unwrap(IcebergView.class)
                .orElseThrow(
                    () -> {
                      if (content instanceof IcebergTable) {
                        return new AlreadyExistsException(
                            "Table with same name already exists: %s in %s", key, reference);
                      } else {
                        return new AlreadyExistsException(
                            "Cannot refresh Iceberg view: Nessie points to a non-Iceberg object for path: %s in %s",
                            key, reference);
                      }
                    });
        metadataLocation = icebergView.getMetadataLocation();
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchViewException("View does not exist: %s in %s", key, reference);
      }
    }
    refreshFromMetadataLocation(
        metadataLocation,
        null,
        2,
        location ->
            NessieUtil.loadViewMetadata(
                ViewMetadataParser.read(io().newInputFile(location)), location, reference));
  }

  @Override
  public void doCommit(ViewMetadata base, ViewMetadata metadata) {
    String newMetadataLocation = writeNewMetadataIfRequired(metadata);

    boolean failure = false;
    try {
      String contentId = icebergView == null ? null : icebergView.getId();
      client.commitView(base, metadata, newMetadataLocation, contentId, key);
    } catch (NessieConflictException | NessieNotFoundException | HttpClientException ex) {
      if (ex instanceof NessieConflictException || ex instanceof NessieNotFoundException) {
        failure = true;
      }
      NessieUtil.handleExceptionsForCommits(ex, client.refName(), Content.Type.ICEBERG_VIEW);
    } catch (NessieBadRequestException ex) {
      failure = true;
      throw NessieUtil.handleBadRequestForCommit(client, key, Content.Type.ICEBERG_VIEW).orElse(ex);
    } finally {
      if (failure) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  @Override
  protected String viewName() {
    return key.toString();
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
