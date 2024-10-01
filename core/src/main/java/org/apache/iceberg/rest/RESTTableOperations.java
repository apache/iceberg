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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.hadoop.Configurable;
import org.apache.iceberg.hadoop.HadoopConfigurable;
import org.apache.iceberg.io.CloseableGroup;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.LocationUtil;

class RESTTableOperations implements TableOperations, Closeable {
  private static final String METADATA_FOLDER_NAME = "metadata";

  enum UpdateType {
    CREATE,
    REPLACE,
    SIMPLE
  }

  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> headers;
  private final List<MetadataUpdate> createChanges;
  private final TableMetadata replaceBase;
  private final Set<Endpoint> endpoints;
  private final CloseableGroup closeableGroup;
  private UpdateType updateType;
  private TableMetadata current;
  private FileIO io;

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    this(client, path, headers, io, UpdateType.SIMPLE, Lists.newArrayList(), current, endpoints);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> endpoints) {
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
    this.endpoints = endpoints;
    this.closeableGroup = new CloseableGroup();
  }

  @Override
  public TableMetadata current() {
    return current;
  }

  @Override
  public TableMetadata refresh() {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_TABLE);
    LoadTableResponse loadTableResponse =
            client.get(path, LoadTableResponse.class, headers, ErrorHandlers.tableErrorHandler());
    updateCurrentMetadata(loadTableResponse);
    updateIO(loadTableResponse);
    return this.current;
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);
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

    updateCurrentMetadata(response);
  }

  @Override
  public FileIO io() {
    return io;
  }

  private void updateCurrentMetadata(LoadTableResponse response) {
    this.current = response.tableMetadata();
  }

  private void updateIO(LoadTableResponse response) {
    Map<String, String> mergedConfig = RESTUtil.merge(this.io.properties(), response.config());
    String ioImpl =mergedConfig.getOrDefault(
            CatalogProperties.FILE_IO_IMPL, CatalogProperties.DEFAULT_REST_FILE_IO_IMPL);
    Object hadoopConf = null;
    if (this.io instanceof HadoopConfigurable) {
      hadoopConf = ((HadoopConfigurable) this.io).getConf();
    }

    closeableGroup.addCloseable(this.io);
    this.io = CatalogUtil.loadFileIO(ioImpl, mergedConfig, hadoopConf);
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

  public void close() throws IOException {
    if (null != closeableGroup) {
      closeableGroup.close();
    }
  }
}
