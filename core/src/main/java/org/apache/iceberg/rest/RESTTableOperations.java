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
package org.apache.iceberg.rest;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RESTTableOperations implements TableOperations {
  private static final Logger LOG = LoggerFactory.getLogger(RESTTableOperations.class);

  private static final String METADATA_FOLDER_NAME = "metadata";

  enum UpdateType {
    CREATE,
    REPLACE,
    SIMPLE
  }

  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final FileIO io;
  private final List<MetadataUpdate> createChanges;
  private final TableMetadata replaceBase;
  private UpdateType updateType;
  private TableMetadata current;

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      TableMetadata current) {
    this(client, path, headers, io, UpdateType.SIMPLE, Lists.newArrayList(), current);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current) {
    this.client = client;
    this.path = path;
    this.headers = headers;
    this.io = io;
    this.updateType = updateType;
    this.createChanges = createChanges;
    this.replaceBase = current;
    if (updateType == UpdateType.CREATE) {
      this.current = null;
    } else {
      this.current = current;
    }
  }

  @Override
  public TableMetadata current() {
    return current;
  }

  @Override
  public TableMetadata refresh() {
    return updateCurrentMetadata(
        client.get(path, LoadTableResponse.class, headers, ErrorHandlers.tableErrorHandler()));
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Consumer<ErrorResponse> errorHandler;
    List<UpdateRequirement> requirements;
    List<MetadataUpdate> updates;
    switch (updateType) {
      case CREATE:
        Preconditions.checkState(
            base == null, "Invalid base metadata for create transaction, expected null: %s", base);
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
        requirements = UpdateRequirements.forCreateTable(updates);
        errorHandler = ErrorHandlers.tableErrorHandler(); // throws NoSuchTableException
        break;

      case REPLACE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(metadata.changes())
                .build();
        // use the original replace base metadata because the transaction will refresh
        requirements = UpdateRequirements.forReplaceTable(replaceBase, updates);
        errorHandler = ErrorHandlers.tableCommitHandler();
        break;

      case SIMPLE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates = metadata.changes();
        requirements = UpdateRequirements.forUpdateTable(base, updates);
        errorHandler = ErrorHandlers.tableCommitHandler();
        break;

      default:
        throw new UnsupportedOperationException(
            String.format("Update type %s is not supported", updateType));
    }

    UpdateTableRequest request = new UpdateTableRequest(requirements, updates);

    // the error handler will throw necessary exceptions like CommitFailedException and
    // UnknownCommitStateException
    // TODO: ensure that the HTTP client lib passes HTTP client errors to the error handler
    LoadTableResponse response =
        client.post(path, request, LoadTableResponse.class, headers, errorHandler);

    // all future commits should be simple commits
    this.updateType = UpdateType.SIMPLE;

    deleteRemovedMetadataFiles(base, metadata);
    updateCurrentMetadata(response);
  }

  @Override
  public FileIO io() {
    return io;
  }

  private TableMetadata updateCurrentMetadata(LoadTableResponse response) {
    // LoadTableResponse is used to deserialize the response, but config is not allowed by the REST
    // spec so it can be
    // safely ignored. there is no requirement to update config on refresh or commit.
    if (current == null
        || !Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = response.tableMetadata();
    }

    return current;
  }

  private static String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties().get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", LocationUtil.stripTrailingSlash(metadataLocation), filename);
    } else {
      return String.format("%s/%s/%s", metadata.location(), METADATA_FOLDER_NAME, filename);
    }
  }

  @Override
  public String metadataFileLocation(String filename) {
    return metadataFileLocation(current(), filename);
  }

  @Override
  public LocationProvider locationProvider() {
    return LocationProviders.locationsFor(current().location(), current().properties());
  }

  @Override
  public TableOperations temp(TableMetadata uncommittedMetadata) {
    return new TableOperations() {
      @Override
      public TableMetadata current() {
        return uncommittedMetadata;
      }

      @Override
      public TableMetadata refresh() {
        throw new UnsupportedOperationException(
            "Cannot call refresh on temporary table operations");
      }

      @Override
      public void commit(TableMetadata base, TableMetadata metadata) {
        throw new UnsupportedOperationException("Cannot call commit on temporary table operations");
      }

      @Override
      public String metadataFileLocation(String fileName) {
        return RESTTableOperations.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        return RESTTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return RESTTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return RESTTableOperations.this.newSnapshotId();
      }
    };
  }

  /**
   * Deletes the oldest metadata files if {@link
   * TableProperties#METADATA_DELETE_AFTER_COMMIT_ENABLED} is true.
   *
   * @param base table metadata on which previous versions were based
   * @param metadata new table metadata with updated previous versions
   */
  private void deleteRemovedMetadataFiles(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    boolean deleteAfterCommit =
            metadata.propertyAsBoolean(
                    TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED,
                    TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED_DEFAULT);

    if (deleteAfterCommit) {
      Set<TableMetadata.MetadataLogEntry> removedPreviousMetadataFiles =
              Sets.newHashSet(base.previousFiles());
      // TableMetadata#addPreviousFile builds up the metadata log and uses
      // TableProperties.METADATA_PREVIOUS_VERSIONS_MAX to determine how many files should stay in
      // the log, thus we don't include metadata.previousFiles() for deletion - everything else can
      // be removed
      removedPreviousMetadataFiles.removeAll(metadata.previousFiles());
      Tasks.foreach(removedPreviousMetadataFiles)
              .noRetry()
              .suppressFailureWhenFinished()
              .onFailure(
                      (previousMetadataFile, exc) ->
                              LOG.warn(
                                      "Delete failed for previous metadata file: {}", previousMetadataFile, exc))
              .run(previousMetadataFile -> io().deleteFile(previousMetadataFile.file()));
    }
  }
}
