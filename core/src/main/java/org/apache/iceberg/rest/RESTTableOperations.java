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
import java.util.stream.Collectors;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.MetadataUpdate;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateRequirement;
import org.apache.iceberg.UpdateRequirements;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.rest.requests.UpdateTableRequest;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.util.LocationUtil;
import org.apache.iceberg.util.PropertyUtil;

class RESTTableOperations implements TableOperations {
  private static final String METADATA_FOLDER_NAME = "metadata";

  enum UpdateType {
    CREATE,
    REPLACE,
    SIMPLE
  }

  private final RESTClient client;
  private final String path;
  private final Supplier<Map<String, String>> readHeaders;
  private final Supplier<Map<String, String>> mutationHeaders;
  private final FileIO io;
  private final KeyManagementClient keyManagementClient;
  private final List<MetadataUpdate> createChanges;
  private final TableMetadata replaceBase;
  private final Set<Endpoint> endpoints;
  private UpdateType updateType;
  private TableMetadata current;
  private EncryptionManager encryptionManager;
  private EncryptingFileIO encryptingFileIO;
  private TableOperations rawFileIOTrackingOps;
  private String tableKeyId;
  private int encryptionDekLength;
  private List<EncryptedKey> encryptedKeys = Lists.newArrayList();

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    this(
        client,
        path,
        headers,
        headers,
        io,
        UpdateType.SIMPLE,
        Lists.newArrayList(),
        current,
        endpoints,
        null);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> headers,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints,
      KeyManagementClient keyManagementClient) {
    this(
        client,
        path,
        headers,
        headers,
        io,
        UpdateType.SIMPLE,
        Lists.newArrayList(),
        current,
        endpoints,
        keyManagementClient);
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
    this(client, path, headers, headers, io, updateType, createChanges, current, endpoints, null);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaders,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    this(
        client,
        path,
        readHeaders,
        mutationHeaders,
        io,
        UpdateType.SIMPLE,
        Lists.newArrayList(),
        current,
        endpoints,
        null);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaders,
      FileIO io,
      TableMetadata current,
      Set<Endpoint> endpoints,
      KeyManagementClient keyManagementClient) {
    this(
        client,
        path,
        readHeaders,
        mutationHeaders,
        io,
        UpdateType.SIMPLE,
        Lists.newArrayList(),
        current,
        endpoints,
        keyManagementClient);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaders,
      FileIO io,
      UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> endpoints) {
    this(
        client,
        path,
        readHeaders,
        mutationHeaders,
        io,
        updateType,
        createChanges,
        current,
        endpoints,
        null);
  }

  RESTTableOperations(
      RESTClient client,
      String path,
      Supplier<Map<String, String>> readHeaders,
      Supplier<Map<String, String>> mutationHeaders,
      FileIO io,
      UpdateType updateType,
      List<MetadataUpdate> createChanges,
      TableMetadata current,
      Set<Endpoint> endpoints,
      KeyManagementClient keyManagementClient) {
    this.client = client;
    this.path = path;
    this.readHeaders = readHeaders;
    this.mutationHeaders = mutationHeaders;
    this.io = io;
    this.keyManagementClient = keyManagementClient;
    this.updateType = updateType;
    this.createChanges = createChanges;
    this.replaceBase = current;
    if (updateType == UpdateType.CREATE) {
      this.current = null;
    } else {
      this.current = current;
    }
    this.endpoints = endpoints;
  }

  @Override
  public TableMetadata current() {
    return current;
  }

  @Override
  public TableMetadata refresh() {
    Endpoint.check(endpoints, Endpoint.V1_LOAD_TABLE);
    return updateCurrentMetadata(
        client.get(path, LoadTableResponse.class, readHeaders, ErrorHandlers.tableErrorHandler()));
  }

  @Override
  public void commit(TableMetadata base, TableMetadata metadata) {
    Endpoint.check(endpoints, Endpoint.V1_UPDATE_TABLE);
    Consumer<ErrorResponse> errorHandler;
    List<UpdateRequirement> requirements;
    List<MetadataUpdate> updates;
    validateEncryptionProperties(base, metadata);
    TableMetadata tableMetadata = withEncryptionKeys(metadata);

    switch (updateType) {
      case CREATE:
        Preconditions.checkState(
            base == null, "Invalid base metadata for create transaction, expected null: %s", base);
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(tableMetadata.changes())
                .build();
        requirements = UpdateRequirements.forCreateTable(updates);
        errorHandler = ErrorHandlers.createTableErrorHandler();
        break;

      case REPLACE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates =
            ImmutableList.<MetadataUpdate>builder()
                .addAll(createChanges)
                .addAll(tableMetadata.changes())
                .build();
        // use the original replace base metadata because the transaction will refresh
        requirements = UpdateRequirements.forReplaceTable(replaceBase, updates);
        errorHandler = ErrorHandlers.tableCommitHandler();
        break;

      case SIMPLE:
        Preconditions.checkState(base != null, "Invalid base metadata: null");
        updates = tableMetadata.changes();
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
    LoadTableResponse response;
    try {
      response = client.post(path, request, LoadTableResponse.class, mutationHeaders, errorHandler);
    } catch (CommitStateUnknownException e) {
      // Lightweight reconciliation for snapshot-add-only updates on transient unknown commit state
      if (updateType == UpdateType.SIMPLE && reconcileOnSimpleUpdate(updates, e)) {
        return;
      }

      throw e;
    }

    // all future commits should be simple commits
    this.updateType = UpdateType.SIMPLE;

    updateCurrentMetadata(response);
  }

  /**
   * Attempt best-effort reconciliation for SIMPLE updates that only add a snapshot.
   *
   * <p>Returns true if the expected snapshot is observed in the refreshed table state. Returns
   * false if the expected snapshot cannot be determined, is not present after refresh, or if the
   * refresh fails. In case of refresh failure, the failure is recorded as suppressed on the
   * provided {@code original} exception to aid diagnostics.
   */
  private boolean reconcileOnSimpleUpdate(
      List<MetadataUpdate> updates, CommitStateUnknownException original) {
    Long expectedSnapshotId = expectedSnapshotIdIfSnapshotAddOnly(updates);
    if (expectedSnapshotId == null) {
      return false;
    }

    try {
      TableMetadata refreshed = refresh();
      return refreshed != null && refreshed.snapshot(expectedSnapshotId) != null;
    } catch (RuntimeException reconEx) {
      original.addSuppressed(reconEx);
      return false;
    }
  }

  @Override
  public FileIO io() {
    return io(current());
  }

  private FileIO rawFileIO() {
    return io;
  }

  TableOperations rawFileIOTrackingOps() {
    if (rawFileIOTrackingOps == null) {
      rawFileIOTrackingOps =
          new TableOperations() {
            @Override
            public TableMetadata current() {
              return RESTTableOperations.this.current();
            }

            @Override
            public TableMetadata refresh() {
              return RESTTableOperations.this.refresh();
            }

            @Override
            public void commit(TableMetadata base, TableMetadata metadata) {
              RESTTableOperations.this.commit(base, metadata);
            }

            @Override
            public FileIO io() {
              return RESTTableOperations.this.rawFileIO();
            }

            @Override
            public String metadataFileLocation(String fileName) {
              return RESTTableOperations.this.metadataFileLocation(fileName);
            }

            @Override
            public LocationProvider locationProvider() {
              return RESTTableOperations.this.locationProvider();
            }
          };
    }

    return rawFileIOTrackingOps;
  }

  private FileIO io(TableMetadata metadata) {
    if (!isEncrypted(metadata)) {
      return io;
    }

    if (encryptingFileIO == null) {
      encryptingFileIO = EncryptingFileIO.combine(io, encryption(metadata));
    }

    return encryptingFileIO;
  }

  @Override
  public EncryptionManager encryption() {
    return encryption(current());
  }

  private EncryptionManager encryption(TableMetadata metadata) {
    if (metadata != null) {
      encryptionPropsFromMetadata(metadata.properties());
      if (tableKeyId != null && encryptedKeys.isEmpty()) {
        encryptedKeys = Lists.newArrayList(metadata.encryptionKeys());
      }
    }

    if (tableKeyId != null) {
      Preconditions.checkArgument(
          keyManagementClient != null,
          "Cannot create encryption manager, missing catalog property %s or %s",
          CatalogProperties.ENCRYPTION_KMS_TYPE,
          CatalogProperties.ENCRYPTION_KMS_IMPL);

      if (encryptionManager == null) {
        Map<String, String> encryptionProperties =
            ImmutableMap.of(
                TableProperties.ENCRYPTION_TABLE_KEY,
                tableKeyId,
                TableProperties.ENCRYPTION_DEK_LENGTH,
                String.valueOf(encryptionDekLength));

        encryptionManager =
            EncryptionUtil.createEncryptionManager(
                encryptedKeys, encryptionProperties, keyManagementClient);
      }

      return encryptionManager;
    }

    return PlaintextEncryptionManager.instance();
  }

  private TableMetadata withEncryptionKeys(TableMetadata metadata) {
    EncryptionManager encrManager = encryption(metadata);
    if (encrManager instanceof StandardEncryptionManager) {
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
      for (Map.Entry<String, EncryptedKey> entry :
          EncryptionUtil.encryptionKeys(encrManager).entrySet()) {
        builder.addEncryptionKey(entry.getValue());
      }

      return builder.build();
    }

    return metadata;
  }

  private void encryptionPropsFromMetadata(Map<String, String> tableProperties) {
    if (tableKeyId == null) {
      tableKeyId = tableProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    }

    if (tableKeyId != null && encryptionDekLength <= 0) {
      encryptionDekLength =
          PropertyUtil.propertyAsInt(
              tableProperties,
              TableProperties.ENCRYPTION_DEK_LENGTH,
              TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
    }
  }

  private static boolean isEncrypted(TableMetadata metadata) {
    return metadata != null
        && metadata.properties().containsKey(TableProperties.ENCRYPTION_TABLE_KEY);
  }

  private static void validateEncryptionProperties(TableMetadata base, TableMetadata metadata) {
    if (base != null) {
      Set<String> removedProps =
          base.properties().keySet().stream()
              .filter(key -> !metadata.properties().containsKey(key))
              .collect(Collectors.toSet());

      Preconditions.checkArgument(
          !removedProps.contains(TableProperties.ENCRYPTION_TABLE_KEY),
          "Cannot remove key ID from an encrypted table");

      Preconditions.checkArgument(
          Objects.equals(
              base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY),
              metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY)),
          "Cannot modify key ID of an encrypted table");
    }
  }

  private void resetEncryption() {
    this.encryptionManager = null;
    this.encryptingFileIO = null;
    this.tableKeyId = null;
    this.encryptionDekLength = 0;
    this.encryptedKeys = Lists.newArrayList();
  }

  private static Long expectedSnapshotIdIfSnapshotAddOnly(List<MetadataUpdate> updates) {
    Long addedSnapshotId = null;
    Long mainRefSnapshotId = null;

    for (MetadataUpdate update : updates) {
      if (update instanceof MetadataUpdate.AddSnapshot) {
        if (addedSnapshotId != null) {
          return null; // multiple snapshot adds -> not safe
        }
        addedSnapshotId = ((MetadataUpdate.AddSnapshot) update).snapshot().snapshotId();
      } else if (update instanceof MetadataUpdate.SetSnapshotRef) {
        MetadataUpdate.SetSnapshotRef setRef = (MetadataUpdate.SetSnapshotRef) update;
        if (!SnapshotRef.MAIN_BRANCH.equals(setRef.name())) {
          return null; // only allow main ref update
        }
        mainRefSnapshotId = setRef.snapshotId();
      } else {
        // any other update type makes this not a pure snapshot-add
        return null;
      }
    }

    if (addedSnapshotId == null) {
      return null;
    }

    if (mainRefSnapshotId != null && !addedSnapshotId.equals(mainRefSnapshotId)) {
      // Only handle "append to main" here. In this request, main is being set to a snapshot ID
      // that is different from the snapshot we just added (e.g., rollback or move main elsewhere).
      // In that case, finding the added snapshot in history doesn't tell us whether main moved to
      // it, so skip reconciliation.
      return null;
    }

    return addedSnapshotId;
  }

  private TableMetadata updateCurrentMetadata(LoadTableResponse response) {
    // LoadTableResponse is used to deserialize the response, but config is not allowed by the REST
    // spec so it can be
    // safely ignored. there is no requirement to update config on refresh or commit.
    if (current == null
        || !Objects.equals(current.metadataFileLocation(), response.metadataLocation())) {
      this.current = checkUUID(current, response.tableMetadata());
      resetEncryption();
    }

    return current;
  }

  private static TableMetadata checkUUID(TableMetadata currentMetadata, TableMetadata newMetadata) {
    String newUUID = newMetadata.uuid();
    if (currentMetadata != null && currentMetadata.uuid() != null && newUUID != null) {
      Preconditions.checkState(
          newUUID.equals(currentMetadata.uuid()),
          "Table UUID does not match: current=%s != refreshed=%s",
          currentMetadata.uuid(),
          newUUID);
    }

    return newMetadata;
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
        return RESTTableOperations.this.io(uncommittedMetadata);
      }

      @Override
      public EncryptionManager encryption() {
        return RESTTableOperations.this.encryption(uncommittedMetadata);
      }

      @Override
      public long newSnapshotId() {
        return RESTTableOperations.this.newSnapshotId();
      }
    };
  }
}
