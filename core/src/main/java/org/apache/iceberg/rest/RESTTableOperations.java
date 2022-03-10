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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Objects;
import org.apache.commons.compress.utils.Lists;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;

class RESTTableOperations implements TableOperations {
  private static final String METADATA_FOLDER_NAME = "metadata";
  private static final ObjectMapper MAPPER = new ObjectMapper(new JsonFactory());
  static {
    RESTSerializers.registerAll(MAPPER);
  }

  enum UpdateType {
    CREATE,
    REPLACE,
    SIMPLE
  }

  private final RESTClient client;
  private final String path;
  private final FileIO io;
  private final List<MetadataUpdate> createChanges;
  private final TableMetadata replaceBase;
  private UpdateType updateType;
  private TableMetadata current;

  RESTTableOperations(RESTClient client, String path, FileIO io, TableMetadata current) {
    this(client, path, io, UpdateType.SIMPLE, Lists.newArrayList(), current);
  }

  RESTTableOperations(RESTClient client, String path, FileIO io, UpdateType updateType,
                      List<MetadataUpdate> createChanges, TableMetadata current) {
    this.client = client;
    this.path = path;
    this.io = io;
    this.updateType = updateType;
    this.createChanges = createChanges;
    this.replaceBase = current;
    this.current = current;
  }

  @Override
  public TableMetadata current() {
    // TODO: may be able to get rid of this by updating refresh() and passing null for create metadata
    return updateType == UpdateType.CREATE ? null : current;
  }

  @Override
  public TableMetadata refresh() {
    // TODO: what if config changes?
    LoadTableResponse response = client.get(path, LoadTableResponse.class, ErrorHandlers.tableErrorHandler());
    if (!Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = response.tableMetadata();
    }

    return current;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    UpdateTableRequest.Builder requestBuilder;
    List<MetadataUpdate> baseChanges;
    switch (updateType) {
      case CREATE:
        Preconditions.checkState(base == null, "Invalid base metadata for create transaction, expected null: %s", base);
        requestBuilder = UpdateTableRequest.builderForCreate();
        baseChanges = createChanges;
        break;

      case REPLACE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        // use the original replace base metadata because the transaction will refresh
        requestBuilder = UpdateTableRequest.builderForReplace(replaceBase);
        baseChanges = createChanges;
        break;

      case SIMPLE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        requestBuilder = UpdateTableRequest.builderFor(base);
        baseChanges = ImmutableList.of();
        break;

      default:
        throw new UnsupportedOperationException(String.format("Update type %s is not supported", updateType));
    }

    baseChanges.forEach(requestBuilder::update);
    metadata.changes().forEach(requestBuilder::update);
    UpdateTableRequest request = requestBuilder.build();

    // the error handler will throw necessary exceptions like CommitFailedException and UnknownCommitStateException
    // TODO: ensure that the HTTP client lib passes HTTP client errors to the error handler
    LoadTableResponse response = client.post(
        path, request, LoadTableResponse.class, ErrorHandlers.tableCommitHandler());

    // all future commits should be simple commits
    this.updateType = UpdateType.SIMPLE;

    this.current = response.tableMetadata();
  }

  @Override
  public FileIO io() {
    return io;
  }

  private static String metadataFileLocation(TableMetadata metadata, String filename) {
    String metadataLocation = metadata.properties()
        .get(TableProperties.WRITE_METADATA_LOCATION);

    if (metadataLocation != null) {
      return String.format("%s/%s", metadataLocation, filename);
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
        throw new UnsupportedOperationException("Cannot call refresh on temporary table operations");
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
        return LocationProviders.locationsFor(uncommittedMetadata.location(), uncommittedMetadata.properties());
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
}
