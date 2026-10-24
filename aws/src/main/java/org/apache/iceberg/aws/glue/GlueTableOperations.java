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
package org.apache.iceberg.aws.glue;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.LocationProviders;
import org.apache.iceberg.LockManager;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.aws.util.RetryDetector;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.encryption.EncryptedKey;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionUtil;
import org.apache.iceberg.encryption.KeyManagementClient;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.base.Strings;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

class GlueTableOperations extends BaseMetastoreTableOperations {

  private static final Logger LOG = LoggerFactory.getLogger(GlueTableOperations.class);

  // same as org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE
  // more details: https://docs.aws.amazon.com/glue/latest/webapi/API_TableInput.html
  private static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

  private static final String MISSING_KMS_CLIENT_MESSAGE =
      "Cannot create encryption manager without a key management client. "
          + "Consider setting the '%s' catalog property";

  private final GlueClient glue;
  private final AwsProperties awsProperties;
  private final String databaseName;
  private final String tableName;
  private final String fullTableName;
  private final String commitLockEntityId;
  private final Map<String, String> tableCatalogProperties;
  private final Object hadoopConf;
  private final LockManager lockManager;
  private final KeyManagementClient keyManagementClient;
  private FileIO fileIO;

  private EncryptionManager encryptionManager;
  private EncryptingFileIO encryptingFileIO;
  private String tableKeyId;
  private int encryptionDekLength;

  private List<EncryptedKey> encryptedKeys = List.of();

  // Attempt to set versionId if available on the path
  private static final DynMethods.UnboundMethod SET_VERSION_ID =
      DynMethods.builder("versionId")
          .hiddenImpl(
              "software.amazon.awssdk.services.glue.model.UpdateTableRequest$Builder", String.class)
          .orNoop()
          .build();

  GlueTableOperations(
      GlueClient glue,
      LockManager lockManager,
      String catalogName,
      AwsProperties awsProperties,
      Map<String, String> tableCatalogProperties,
      Object hadoopConf,
      TableIdentifier tableIdentifier,
      KeyManagementClient keyManagementClient) {
    this.glue = glue;
    this.awsProperties = awsProperties;
    this.databaseName =
        IcebergToGlueConverter.getDatabaseName(
            tableIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.tableName =
        IcebergToGlueConverter.getTableName(
            tableIdentifier, awsProperties.glueCatalogSkipNameValidation());
    this.fullTableName = String.format("%s.%s.%s", catalogName, databaseName, tableName);
    this.commitLockEntityId = String.format("%s.%s", databaseName, tableName);
    this.tableCatalogProperties = tableCatalogProperties;
    this.hadoopConf = hadoopConf;
    this.lockManager = lockManager;
    this.keyManagementClient = keyManagementClient;
  }

  @Override
  public FileIO io() {
    if (fileIO == null) {
      fileIO = initializeFileIO(this.tableCatalogProperties, this.hadoopConf);
    }

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

    if (tableKeyId == null) {
      return PlaintextEncryptionManager.instance();
    }

    Preconditions.checkArgument(
        keyManagementClient != null,
        MISSING_KMS_CLIENT_MESSAGE,
        CatalogProperties.ENCRYPTION_KMS_IMPL);

    Map<String, String> encryptionProperties =
        ImmutableMap.of(
            TableProperties.ENCRYPTION_TABLE_KEY,
            tableKeyId,
            TableProperties.ENCRYPTION_DEK_LENGTH,
            String.valueOf(encryptionDekLength));

    encryptionManager =
        EncryptionUtil.createEncryptionManager(
            encryptedKeys, encryptionProperties, keyManagementClient);

    return encryptionManager;
  }

  @Override
  protected String tableName() {
    return fullTableName;
  }

  @Override
  protected void doRefresh() {
    String metadataLocation = null;
    String tableKeyIdFromGlue = null;
    String dekLengthFromGlue = null;
    String metadataHashFromGlue = null;
    Table table = getGlueTable();
    if (table != null) {
      checkIfTableIsIceberg(table, tableName());
      metadataLocation = table.parameters().get(METADATA_LOCATION_PROP);
      /* Table key ID must be retrieved from a catalog service, and not from untrusted storage
      (e.g. metadata json file) that can be tampered with. For example, an attacker can remove
      the table key parameter (along with existing snapshots) in the file, making the writers
      produce unencrypted files. Table key ID is taken directly from the Glue catalog */
      tableKeyIdFromGlue = table.parameters().get(TableProperties.ENCRYPTION_TABLE_KEY);
      dekLengthFromGlue = table.parameters().get(TableProperties.ENCRYPTION_DEK_LENGTH);
      metadataHashFromGlue = table.parameters().get(METADATA_HASH_PROP);
    } else {
      if (currentMetadataLocation() != null) {
        throw new NoSuchTableException(
            "Cannot find Glue table %s after refresh, "
                + "maybe another process deleted it or revoked your access permission",
            tableName());
      }
    }

    refreshFromMetadataLocation(metadataLocation);

    if (tableKeyIdFromGlue != null) {
      checkIntegrityForEncryption(tableKeyIdFromGlue, dekLengthFromGlue, metadataHashFromGlue);

      this.tableKeyId = tableKeyIdFromGlue;
      this.encryptionDekLength =
          (dekLengthFromGlue != null)
              ? Integer.parseInt(dekLengthFromGlue)
              : TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT;

      this.encryptedKeys =
          Optional.ofNullable(current().encryptionKeys())
              .map(Lists::newLinkedList)
              .orElseGet(Lists::newLinkedList);

      // Active transactions may still need keys that are not in committed metadata.
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
      this.encryptingFileIO = null;
      this.encryptionManager = null;
    }
  }

  @Override
  protected void doCommit(TableMetadata base, TableMetadata metadata) {
    checkEncryptionKeyUnchanged(base, metadata);
    encryptionPropsFromMetadata(metadata.properties());
    // fail fast on a misconfigured catalog, rather than in the middle of the commit
    Preconditions.checkArgument(
        tableKeyId == null || keyManagementClient != null,
        MISSING_KMS_CLIENT_MESSAGE,
        CatalogProperties.ENCRYPTION_KMS_IMPL);

    CommitStatus commitStatus = CommitStatus.FAILURE;
    RetryDetector retryDetector = new RetryDetector();

    String newMetadataLocation = null;
    boolean glueTempTableCreated = false;
    try {
      glueTempTableCreated = createGlueTempTableIfNecessary(base, metadata.location());

      boolean newTable = base == null;
      newMetadataLocation = writeNewMetadataIfRequired(newTable, metadata);
      lock(newMetadataLocation);
      Table glueTable = getGlueTable();
      checkMetadataLocation(glueTable, base);
      Map<String, String> properties = prepareProperties(glueTable, newMetadataLocation, metadata);
      persistGlueTable(glueTable, properties, metadata, retryDetector);
      commitStatus = CommitStatus.SUCCESS;
    } catch (CommitFailedException e) {
      throw e;
    } catch (RuntimeException persistFailure) {
      boolean isAwsServiceException = persistFailure instanceof AwsServiceException;

      // If we got an exception we weren't expecting, or we got an AWS service exception
      // but retries were performed, attempt to reconcile the actual commit status.
      if (!isAwsServiceException || retryDetector.retried()) {
        LOG.warn(
            "Received unexpected failure when committing to {}, validating if commit ended up succeeding.",
            fullTableName,
            persistFailure);
        commitStatus = checkCommitStatus(newMetadataLocation, metadata);
      }

      // If we got an AWS exception we would usually handle, but find we
      // succeeded on a retry that threw an exception, skip the exception.
      if (commitStatus != CommitStatus.SUCCESS && isAwsServiceException) {
        handleAWSExceptions((AwsServiceException) persistFailure);
      }

      switch (commitStatus) {
        case SUCCESS:
          break;
        case FAILURE:
          throw new CommitFailedException(
              persistFailure, "Cannot commit %s due to unexpected exception", tableName());
        case UNKNOWN:
          throw new CommitStateUnknownException(persistFailure);
      }
    } finally {
      cleanupMetadataAndUnlock(commitStatus, newMetadataLocation);
      cleanupGlueTempTableIfNecessary(glueTempTableCreated, commitStatus);
    }
  }

  /**
   * Validate the Glue table is Iceberg table by checking its parameters. If the table properties
   * check does not pass, for Iceberg it is equivalent to not having a table in the catalog. We
   * throw a {@link NoSuchIcebergTableException} in that case.
   *
   * @param table glue table
   * @param fullName full table name for logging
   * @throws NoSuchIcebergTableException if the table is not an Iceberg table
   */
  static void checkIfTableIsIceberg(Table table, String fullName) {
    String tableType = table.parameters().get(TABLE_TYPE_PROP);
    NoSuchIcebergTableException.check(
        tableType != null && tableType.equalsIgnoreCase(ICEBERG_TABLE_TYPE_VALUE),
        "Input Glue table is not an iceberg table: %s (type=%s)",
        fullName,
        tableType);
  }

  protected static FileIO initializeFileIO(Map<String, String> properties, Object hadoopConf) {
    String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
    if (fileIOImpl == null) {
      FileIO io = new S3FileIO();
      io.initialize(properties);
      return io;
    } else {
      return CatalogUtil.loadFileIO(fileIOImpl, properties, hadoopConf);
    }
  }

  private boolean createGlueTempTableIfNecessary(TableMetadata base, String metadataLocation) {
    if (awsProperties.glueLakeFormationEnabled() && base == null) {
      // LakeFormation credential require TableArn as input，so creating a dummy table
      // beforehand for create table scenario
      glue.createTable(
          CreateTableRequest.builder()
              .databaseName(databaseName)
              .tableInput(
                  TableInput.builder()
                      .parameters(ImmutableMap.of(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE))
                      .name(tableName)
                      .storageDescriptor(
                          StorageDescriptor.builder().location(metadataLocation).build())
                      .build())
              .build());
      return true;
    }

    return false;
  }

  private void cleanupGlueTempTableIfNecessary(
      boolean glueTempTableCreated, CommitStatus commitStatus) {
    if (glueTempTableCreated && commitStatus != CommitStatus.SUCCESS) {
      glue.deleteTable(
          DeleteTableRequest.builder().databaseName(databaseName).name(tableName).build());
    }
  }

  private void lock(String newMetadataLocation) {
    if (lockManager != null && !lockManager.acquire(commitLockEntityId, newMetadataLocation)) {
      throw new IllegalStateException(
          String.format(
              "Fail to acquire lock %s to commit new metadata at %s",
              commitLockEntityId, newMetadataLocation));
    }
  }

  private void checkMetadataLocation(Table glueTable, TableMetadata base) {
    String glueMetadataLocation =
        glueTable != null ? glueTable.parameters().get(METADATA_LOCATION_PROP) : null;
    String baseMetadataLocation = base != null ? base.metadataFileLocation() : null;
    if (!Objects.equals(baseMetadataLocation, glueMetadataLocation)) {
      throw new CommitFailedException(
          "Cannot commit %s because base metadata location '%s' is not same as the current Glue location '%s'",
          tableName(), baseMetadataLocation, glueMetadataLocation);
    }
  }

  private Table getGlueTable() {
    try {
      GetTableResponse response =
          glue.getTable(
              GetTableRequest.builder()
                  .catalogId(awsProperties.glueCatalogId())
                  .databaseName(databaseName)
                  .name(tableName)
                  .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    }
  }

  private Map<String, String> prepareProperties(
      Table glueTable, String newMetadataLocation, TableMetadata metadata) {
    Map<String, String> properties =
        glueTable != null ? Maps.newHashMap(glueTable.parameters()) : Maps.newHashMap();
    properties.put(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(Locale.ROOT));
    properties.put(METADATA_LOCATION_PROP, newMetadataLocation);
    if (currentMetadataLocation() != null && !currentMetadataLocation().isEmpty()) {
      properties.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation());
    }

    if (tableKeyId != null) {
      /* Encryption parameters are kept in the Glue catalog, and not only in the metadata json file,
      because the latter can be tampered with in an untrusted storage. The metadata hash allows to
      detect such tampering upon refresh. */
      properties.put(TableProperties.ENCRYPTION_TABLE_KEY, tableKeyId);
      properties.put(TableProperties.ENCRYPTION_DEK_LENGTH, String.valueOf(encryptionDekLength));
      properties.put(METADATA_HASH_PROP, EncryptionUtil.metadataHash(metadata));
    }

    return properties;
  }

  @VisibleForTesting
  void persistGlueTable(
      Table glueTable,
      Map<String, String> parameters,
      TableMetadata metadata,
      RetryDetector retryDetector) {
    if (glueTable != null) {
      LOG.debug("Committing existing Glue table: {}", tableName());
      UpdateTableRequest.Builder updateTableRequest =
          UpdateTableRequest.builder()
              .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(databaseName)
              .skipArchive(awsProperties.glueCatalogSkipArchive())
              .tableInput(
                  TableInput.builder()
                      .applyMutation(
                          builder ->
                              IcebergToGlueConverter.setTableInputInformation(
                                  builder, metadata, glueTable))
                      .name(tableName)
                      .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                      .parameters(parameters)
                      .build());
      // Use Optimistic locking with table version id while updating table
      if (!SET_VERSION_ID.isNoop() && lockManager == null) {
        SET_VERSION_ID.invoke(updateTableRequest, glueTable.versionId());
      }

      glue.updateTable(updateTableRequest.build());
    } else {
      LOG.debug("Committing new Glue table: {}", tableName());
      glue.createTable(
          CreateTableRequest.builder()
              .overrideConfiguration(c -> c.addMetricPublisher(retryDetector))
              .catalogId(awsProperties.glueCatalogId())
              .databaseName(databaseName)
              .tableInput(
                  TableInput.builder()
                      .applyMutation(
                          builder ->
                              IcebergToGlueConverter.setTableInputInformation(builder, metadata))
                      .name(tableName)
                      .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                      .parameters(parameters)
                      .build())
              .build());
    }
  }

  private void handleAWSExceptions(AwsServiceException persistFailure) {
    if (persistFailure instanceof ConcurrentModificationException) {
      throw new CommitFailedException(
          persistFailure, "Cannot commit %s because Glue detected concurrent update", tableName());
    } else if (persistFailure
        instanceof software.amazon.awssdk.services.glue.model.AlreadyExistsException) {
      throw new AlreadyExistsException(
          persistFailure,
          "Cannot commit %s because its Glue table already exists when trying to create one",
          tableName());
    } else if (persistFailure instanceof EntityNotFoundException) {
      throw new NotFoundException(
          persistFailure,
          "Cannot commit %s because Glue cannot find the requested entity",
          tableName());
    } else if (persistFailure instanceof AccessDeniedException) {
      throw new ForbiddenException(
          persistFailure,
          "Cannot commit %s because Glue cannot access the requested resources",
          tableName());
    } else if (persistFailure
        instanceof software.amazon.awssdk.services.glue.model.ValidationException) {
      throw new ValidationException(
          persistFailure,
          "Cannot commit %s because Glue encountered a validation exception "
              + "while accessing requested resources",
          tableName());
    } else {
      int statusCode = persistFailure.statusCode();
      if (statusCode < 500 || statusCode >= 600) {
        throw persistFailure;
      }
    }
  }

  @VisibleForTesting
  void cleanupMetadataAndUnlock(CommitStatus commitStatus, String metadataLocation) {
    try {
      if (commitStatus == CommitStatus.FAILURE
          && metadataLocation != null
          && !metadataLocation.isEmpty()) {
        // if anything went wrong, clean up the uncommitted metadata file
        io().deleteFile(metadataLocation);
      }
    } catch (RuntimeException e) {
      LOG.error("Failed to cleanup metadata file at {}", metadataLocation, e);
    } finally {
      if (lockManager != null) {
        lockManager.release(commitLockEntityId, metadataLocation);
      }
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
        return GlueTableOperations.this.metadataFileLocation(uncommittedMetadata, fileName);
      }

      @Override
      public LocationProvider locationProvider() {
        return LocationProviders.locationsFor(
            uncommittedMetadata.location(), uncommittedMetadata.properties());
      }

      @Override
      public FileIO io() {
        GlueTableOperations.this.encryptionPropsFromMetadata(uncommittedMetadata.properties());
        return GlueTableOperations.this.io();
      }

      @Override
      public EncryptionManager encryption() {
        return GlueTableOperations.this.encryption();
      }

      @Override
      public long newSnapshotId() {
        return GlueTableOperations.this.newSnapshotId();
      }
    };
  }

  private void encryptionPropsFromMetadata(Map<String, String> tableProperties) {
    if (tableKeyId == null) {
      this.tableKeyId = tableProperties.get(TableProperties.ENCRYPTION_TABLE_KEY);
    }

    if (tableKeyId != null && encryptionDekLength <= 0) {
      this.encryptionDekLength =
          PropertyUtil.propertyAsInt(
              tableProperties,
              TableProperties.ENCRYPTION_DEK_LENGTH,
              TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT);
    }
  }

  /** The key ID of an encrypted table must stay the same for the whole table lifetime. */
  private static void checkEncryptionKeyUnchanged(TableMetadata base, TableMetadata metadata) {
    if (base == null) {
      return;
    }

    String baseKeyId = base.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);
    String newKeyId = metadata.properties().get(TableProperties.ENCRYPTION_TABLE_KEY);

    Preconditions.checkArgument(
        baseKeyId == null || newKeyId != null, "Cannot remove key ID from an encrypted table");
    Preconditions.checkArgument(
        Objects.equals(baseKeyId, newKeyId), "Cannot modify key ID of an encrypted table");
  }

  private void checkIntegrityForEncryption(
      String encryptionKeyIdFromGlue, String dekLengthFromGlue, String metadataHashFromGlue) {
    TableMetadata metadata = current();
    if (!Strings.isNullOrEmpty(metadataHashFromGlue)) {
      EncryptionUtil.verifyMetadataHash(metadata, metadataHashFromGlue);
      return;
    }

    LOG.warn(
        "Full metadata integrity check skipped because no metadata hash was recorded in Glue for table {}."
            + " Falling back to encryption property based check.",
        tableName());

    Map<String, String> propertiesFromMetadata = metadata.properties();

    String encryptionKeyIdFromMetadata =
        propertiesFromMetadata.get(TableProperties.ENCRYPTION_TABLE_KEY);
    if (!Objects.equals(encryptionKeyIdFromGlue, encryptionKeyIdFromMetadata)) {
      throw new RuntimeException(
          String.format(
              "Metadata file might have been modified. Encryption key id %s differs from Glue value %s",
              encryptionKeyIdFromMetadata, encryptionKeyIdFromGlue));
    }

    String dekLengthFromMetadata =
        propertiesFromMetadata.get(TableProperties.ENCRYPTION_DEK_LENGTH);
    if (!Objects.equals(dekLengthFromGlue, dekLengthFromMetadata)) {
      throw new RuntimeException(
          String.format(
              "Metadata file might have been modified. DEK length %s differs from Glue value %s",
              dekLengthFromMetadata, dekLengthFromGlue));
    }
  }

  @VisibleForTesting
  Map<String, String> tableCatalogProperties() {
    return tableCatalogProperties;
  }
}
