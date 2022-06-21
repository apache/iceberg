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
import java.util.Optional;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.view.BaseViewVersion;
import org.apache.iceberg.view.MetastoreViewOperations;
import org.apache.iceberg.view.ViewHistoryEntry;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.apache.iceberg.view.ViewVersion;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergView;
import org.projectnessie.model.ImmutableCommitMeta;
import org.projectnessie.model.ImmutableIcebergView;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Reference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NessieViewOperations extends MetastoreViewOperations {

  private static final Logger LOG = LoggerFactory.getLogger(NessieViewOperations.class);

  /**
   * Name of the `{@link TableMetadata} property that holds the Nessie commit-ID from which the
   * metadata has been loaded.
   */
  public static final String NESSIE_COMMIT_ID_PROPERTY = "nessie.commit.id";

  private final NessieIcebergClient client;
  private final ContentKey key;
  private final FileIO fileIO;
  private final Map<String, String> catalogOptions;
  private IcebergView icebergView;

  NessieViewOperations(
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
  public ViewMetadata refresh() {
    try {
      client.refresh();
    } catch (NessieNotFoundException e) {
      throw new RuntimeException(String.format("Failed to refresh as ref '%s' " +
          "is no longer valid.", client.getRef().getName()), e);
    }
    String metadataLocation = null;
    Reference reference = client.getRef().getReference();
    try {
      Content content =
          client
              .getApi()
              .getContent()
              .key(key)
              .reference(reference)
              .get()
              .get(key);
      LOG.debug("Content '{}' at '{}': {}", key, reference, content);
      if (content == null) {
        if (currentMetadataLocation() != null) {
          throw new NoSuchTableException("No such view %s in %s", key, reference);
        }
      } else {
        this.icebergView =
            content
                .unwrap(IcebergView.class)
                .orElseThrow(
                    () ->
                        new IllegalStateException(String.format(
                            "Cannot refresh iceberg view: Nessie points to a non-Iceberg object for path: %s.", key)));
        metadataLocation = icebergView.getMetadataLocation();
      }
    } catch (NessieNotFoundException ex) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(ex, "No such view %s", key);
      }
    }
    refreshFromMetadataLocation(metadataLocation, null, 2, l -> loadViewMetadata(l, reference));
    return current();
  }

  private ViewMetadata loadViewMetadata(String metadataLocation, Reference reference) {
    ViewMetadata metadata =
        ViewMetadataParser.read(io().newInputFile(metadataLocation));
    Optional<ViewVersion> viewVersion =
        metadata.versions().stream()
            .filter(version -> version.versionId() == icebergView.getVersionId())
            .findFirst();
    if (viewVersion.isPresent()) {
      ViewVersion version = viewVersion.get();
      BaseViewVersion baseViewVersion =
          BaseViewVersion.builder()
              .versionId(version.versionId())
              .parentId(version.parentId())
              .timestampMillis(version.timestampMillis())
              .summary(version.summary())
              .representations(version.representations())
              .build();

      List<ViewVersion> versions = getVersionsUntil(metadata, icebergView.getVersionId());
      List<ViewHistoryEntry> history = getHistoryEntriesUntil(metadata, icebergView.getVersionId());
      metadata =
          ViewMetadata.builder()
              .location(metadata.location())
              .addVersion(baseViewVersion)
              .properties(metadata.properties())
              .versions(versions)
              .history(history)
              .currentVersionId(baseViewVersion.versionId())
              .build();
    }

    return metadata;
  }

  private List<ViewHistoryEntry> getHistoryEntriesUntil(ViewMetadata metadata, int versionId) {
    List<ViewHistoryEntry> history = Lists.newArrayList();
    for (ViewHistoryEntry entry : metadata.history()) {
      if (entry.versionId() == versionId) {
        break;
      }
      history.add(entry);
    }
    return history;
  }

  private List<ViewVersion> getVersionsUntil(ViewMetadata metadata, int versionId) {
    List<ViewVersion> versions = Lists.newArrayList();
    for (ViewVersion version : metadata.versions()) {
      if (version.versionId() == versionId) {
        break;
      }
      versions.add(version);
    }
    return versions;
  }

  @Override
  public void commit(ViewMetadata base, ViewMetadata metadata, Map<String, String> properties) {
    UpdateableReference updateableReference = client.getRef();

    updateableReference.checkMutable();

    Branch current = updateableReference.getAsBranch();
    Branch expectedHead = current;
    // TODO
//    if (base != null) {
//      String metadataCommitId = base.property(NESSIE_COMMIT_ID_PROPERTY, expectedHead.getHash());
//      if (metadataCommitId != null) {
//        expectedHead = Branch.of(expectedHead.getName(), metadataCommitId);
//      }
//    }

    String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);

    boolean delete = true;
    try {
      ImmutableIcebergView.Builder viewBuilder = ImmutableIcebergView.builder();
      if (icebergView != null) {
        viewBuilder.id(icebergView.getId());
      }

      // TODO: read schema/dialect/sql from view defintion
      IcebergView newView =
          viewBuilder
              .metadataLocation(newMetadataLocation)
              .versionId(metadata.currentVersionId())
              .schemaId(23)
              .dialect("TODO")
              //.sqlText(metadata.currentVersion().representations().get(0.definition().sql())
              .build();


      LOG.debug("Committing '{}' against '{}', current is '{}': {}", key, expectedHead,
          current.getHash(), newView);
      ImmutableCommitMeta.Builder builder = ImmutableCommitMeta.builder();
      builder.message(buildCommitMsg(base, metadata));
      Branch branch = client.getApi().commitMultipleOperations()
          .operation(Operation.Put.of(key, newView, icebergView))
          .commitMeta(NessieUtil.catalogOptions(builder, catalogOptions).build())
          .branch(expectedHead)
          .commit();
      LOG.info("Committed '{}' against '{}', expected commit-id was '{}'", key, branch,
          expectedHead.getHash());
      updateableReference.updateReference(branch);

      delete = false;
    } catch (NessieConflictException ex) {
      throw new CommitFailedException(ex, "Cannot commit: Reference hash is out of date. " +
          "Update the reference '%s' and try again", updateableReference.getName());
    } catch (HttpClientException ex) {
      // Intentionally catch all nessie-client-exceptions here and not just the "timeout" variant
      // to catch all kinds of network errors (e.g. connection reset). Network code implementation
      // details and all kinds of network devices can induce unexpected behavior. So better be
      // safe than sorry.
      delete = false;
      throw new CommitStateUnknownException(ex);
    } catch (NessieNotFoundException ex) {
      throw new RuntimeException(
          String.format("Cannot commit: Reference '%s' no longer exists",
              updateableReference.getName()), ex);
    } finally {
      if (delete) {
        io().deleteFile(newMetadataLocation);
      }
    }
  }

  private String buildCommitMsg(ViewMetadata base, ViewMetadata metadata) {
    if (base != null && metadata.currentVersionId() != base.currentVersionId()) {
      return "Iceberg schema change against view ";
    }
    return "Iceberg commit against view %s";
  }

  @Override
  public FileIO io() {
    return fileIO;
  }
}
