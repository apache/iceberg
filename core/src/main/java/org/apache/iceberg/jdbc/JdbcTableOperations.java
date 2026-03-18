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
package org.apache.iceberg.jdbc;

import java.sql.DataTruncation;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLWarning;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class JdbcTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(JdbcTableOperations.class);
  private final String catalogName;
  private final TableIdentifier tableIdentifier;
  private final FileIO fileIO;
  private final JdbcClientPool connections;
  private final Map<String, String> catalogProperties;
  private final JdbcUtil.SchemaVersion schemaVersion;
  private final KeyManagementClient keyManagementClient;

  private EncryptionManager encryptionManager;
  private EncryptingFileIO encryptingFileIO;
  private String tableKeyId;
  private int encryptionDekLength;
  private List<EncryptedKey> encryptedKeys = List.of();

  protected JdbcTableOperations(
      JdbcClientPool dbConnPool,
      FileIO fileIO,
      String catalogName,
      TableIdentifier tableIdentifier,
      Map<String, String> catalogProperties,
      JdbcUtil.SchemaVersion schemaVersion,
      KeyManagementClient keyManagementClient) {
    this.catalogName = catalogName;
    this.tableIdentifier = tableIdentifier;
    this.fileIO = fileIO;
    this.connections = dbConnPool;
    this.catalogProperties = catalogProperties;
    this.schemaVersion = schemaVersion;
    this.keyManagementClient = keyManagementClient;
  }

  @Override
  public FileIO io() {
    if (tableKeyId == null) {
      return fileIO;
    }

    if (encryptingFileIO == null) {
      encryptingFileIO = EncryptingFileIO.combine(fileIO, encryption());
    }

    return encryptingFileIO;
  }

  @Override
  public EncryptionManager encryption() {
    if (encryptionManager != null) {
      return encryptionManager;
    }

    if (tableKeyId != null) {
      if (keyManagementClient == null) {
        throw new RuntimeException(
            "Can't create encryption manager, because key management client is not set");
      }

      Map<String, String> encryptionProperties = Maps.newHashMap();
      encryptionProperties.put(TableProperties.ENCRYPTION_TABLE_KEY, tableKeyId);
      encryptionProperties.put(
          TableProperties.ENCRYPTION_DEK_LENGTH, String.valueOf(encryptionDekLength));

      encryptionManager =
          EncryptionUtil.createEncryptionManager(
              encryptedKeys, encryptionProperties, keyManagementClient);
    } else {
      return PlaintextEncryptionManager.instance();
    }

    return encryptionManager;
  }

  @Override
  public void doRefresh() {
    Map<String, String> table;

    try {
      table = JdbcUtil.loadTable(schemaVersion, connections, catalogName, tableIdentifier);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during refresh");
    } catch (SQLException e) {
      // SQL exception happened when getting table from catalog
      throw new UncheckedSQLException(
          e, "Failed to get table %s from catalog %s", tableIdentifier, catalogName);
    }

    if (table.isEmpty()) {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Failed to load table %s from catalog %s: dropped by another process",
            tableIdentifier, catalogName);
      } else {
        this.disableRefresh();
        return;
      }
    }

    String newMetadataLocation = table.get(METADATA_LOCATION_PROP);
    Preconditions.checkState(
        newMetadataLocation != null,
        "Invalid table %s: metadata location is null",
        tableIdentifier);
    refreshFromMetadataLocation(newMetadataLocation);

    // TODO: Store a metadata hash in iceberg_tables and verify it here, like Hive does with HMS,
    //  to protect against tampering with the unencrypted metadata.json file in untrusted storage.
    TableMetadata metadata = current();
    if (metadata != null) {
      String tableKeyIdFromMetadata =
          metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
      if (tableKeyIdFromMetadata != null) {
        tableKeyId = tableKeyIdFromMetadata;
        encryptionDekLength =
            PropertyUtil.propertyAsInt(
                metadata.properties(),
                TableProperties.ENCRYPTION_DEK_LENGTH,
                TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);

        encryptedKeys =
            Optional.ofNullable(metadata.encryptionKeys())
                .map(Lists::newLinkedList)
                .orElseGet(Lists::newLinkedList);

        if (encryptionManager != null) {
          Set<String> keyIdsFromMetadata =
              encryptedKeys.stream().map(EncryptedKey::keyId).collect(Collectors.toSet());

          for (EncryptedKey keyFromEM : EncryptionUtil.encryptionKeys(encryptionManager).values()) {
            if (!keyIdsFromMetadata.contains(keyFromEM.keyId())) {
              encryptedKeys.add(keyFromEM);
            }
          }
        }

        // Force re-creation of encryption manager with updated keys
        encryptingFileIO = null;
        encryptionManager = null;
      }
    }
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  @Override
  public void doCommit(TableMetadata base, TableMetadata metadata) {
    boolean newTable = base == null;
    encryptionPropsFromMetadata(metadata.properties());

    final TableMetadata tableMetadata;
    EncryptionManager encrManager = encryption();
    if (encrManager instanceof StandardEncryptionManager) {
      TableMetadata.Builder builder = TableMetadata.buildFrom(metadata);
      for (Map.Entry<String, EncryptedKey> entry :
          EncryptionUtil.encryptionKeys(encrManager).entrySet()) {
        builder.addEncryptionKey(entry.getValue());
      }

      tableMetadata = builder.build();
    } else {
      tableMetadata = metadata;
    }

    if (base != null) {
      Set<String> removedProps =
          base.properties().keySet().stream()
              .filter(key -> !metadata.properties().containsKey(key))
              .collect(Collectors.toSet());

      if (removedProps.contains(TableProperties.ENCRYPTION_TABLE_KEY)) {
        throw new IllegalArgumentException("Cannot remove key in encrypted table");
      }

      if (!Objects.equals(
          base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY),
          metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY))) {
        throw new IllegalArgumentException("Cannot modify key in encrypted table");
      }
    }

    String newMetadataLocation = writeNewMetadataIfRequired(newTable, tableMetadata);
    try {
      Map<String, String> table =
          JdbcUtil.loadTable(schemaVersion, connections, catalogName, tableIdentifier);

      if (base != null) {
        validateMetadataLocation(table, base);
        String oldMetadataLocation = base.metadataFileLocation();
        // Start atomic update
        LOG.debug("Committing existing table: {}", tableName());
        updateTable(newMetadataLocation, oldMetadataLocation);
      } else {
        // table not exists create it
        LOG.debug("Committing new table: {}", tableName());
        createTable(newMetadataLocation);
      }

    } catch (SQLTimeoutException e) {
      throw new UncheckedSQLException(e, "Database Connection timeout");
    } catch (SQLTransientConnectionException | SQLNonTransientConnectionException e) {
      throw new UncheckedSQLException(e, "Database Connection failed");
    } catch (DataTruncation e) {
      throw new UncheckedSQLException(e, "Database data truncation error");
    } catch (SQLWarning e) {
      throw new UncheckedSQLException(e, "Database warning");
    } catch (SQLException e) {
      if (JdbcUtil.isConstraintViolation(e)) {
        if (currentMetadataLocation() == null) {
          throw new AlreadyExistsException(e, "Table already exists: %s", tableIdentifier);
        } else {
          throw new UncheckedSQLException(e, "Table already exists: %s", tableIdentifier);
        }
      }

      throw new UncheckedSQLException(e, "Unknown failure");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new UncheckedInterruptedException(e, "Interrupted during commit");
    }
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
        return JdbcTableOperations.this.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        JdbcTableOperations.this.encryptionPropsFromMetadata(uncommittedMetadata.properties());
        return JdbcTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return JdbcTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return JdbcTableOperations.this.newSnapshotId();
      }
    };
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

  private void updateTable(String newMetadataLocation, String oldMetadataLocation)
      throws SQLException, InterruptedException {
    int updatedRecords =
        JdbcUtil.updateTable(
            schemaVersion,
            connections,
            catalogName,
            tableIdentifier,
            newMetadataLocation,
            oldMetadataLocation);

    if (updatedRecords == 1) {
      LOG.debug("Successfully committed to existing table: {}", tableIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to update table %s from catalog %s", tableIdentifier, catalogName);
    }
  }

  private void createTable(String newMetadataLocation) throws SQLException, InterruptedException {
    Namespace namespace = tableIdentifier.namespace();
    if (PropertyUtil.propertyAsBoolean(catalogProperties, JdbcUtil.STRICT_MODE_PROPERTY, false)
        && !JdbcUtil.namespaceExists(catalogName, connections, namespace)) {
      throw new NoSuchNamespaceException(
          "Cannot create table %s in catalog %s. Namespace %s does not exist",
          tableIdentifier, catalogName, namespace);
    }

    if (schemaVersion == JdbcUtil.SchemaVersion.V1
        && JdbcUtil.viewExists(catalogName, connections, tableIdentifier)) {
      throw new AlreadyExistsException("View with same name already exists: %s", tableIdentifier);
    }

    if (JdbcUtil.tableExists(schemaVersion, catalogName, connections, tableIdentifier)) {
      throw new AlreadyExistsException("Table already exists: %s", tableIdentifier);
    }

    int insertRecord =
        JdbcUtil.doCommitCreateTable(
            schemaVersion,
            connections,
            catalogName,
            namespace,
            tableIdentifier,
            newMetadataLocation);

    if (insertRecord == 1) {
      LOG.debug("Successfully committed to new table: {}", tableIdentifier);
    } else {
      throw new CommitFailedException(
          "Failed to create table %s in catalog %s", tableIdentifier, catalogName);
    }
  }

  private void validateMetadataLocation(Map<String, String> table, TableMetadata base) {
    String catalogMetadataLocation = table.get(METADATA_LOCATION_PROP);
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;

    if (!Objects.equals(baseMetadataLocation, catalogMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s: metadata location %s has changed from %s",
          tableIdentifier, baseMetadataLocation, catalogMetadataLocation);
    }
  }

  @Override
  protected String tableName() {
    return tableIdentifier.toString();
  }
}
