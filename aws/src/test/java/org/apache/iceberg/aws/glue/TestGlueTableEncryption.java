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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.encryption.StandardEncryptionManager;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

public class TestGlueTableEncryption {

  private static final String CATALOG_NAME = "glue";
  private static final String WAREHOUSE_PATH = "s3://bucket/warehouse";
  private static final String DB_NAME = "db";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, "table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));
  private static final Map<String, String> ENCRYPTED_TABLE_PROPERTIES =
      ImmutableMap.of(
          TableProperties.FORMAT_VERSION,
          "3",
          TableProperties.ENCRYPTION_TABLE_KEY,
          UnitestKMS.MASTER_KEY_NAME1);

  // the single table held by the mocked Glue service
  private final AtomicReference<Table> glueTable = new AtomicReference<>();

  private GlueClient glue;
  private GlueCatalog catalog;

  @BeforeEach
  public void before() {
    glueTable.set(null);
    glue = mockGlueClient();
    catalog =
        catalogWith(
            ImmutableMap.of(
                CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getCanonicalName()));
  }

  @AfterEach
  public void after() throws Exception {
    catalog.close();
  }

  @Test
  public void testEncryptionParametersArePersistedInGlue() {
    catalog.createTable(
        TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), ENCRYPTED_TABLE_PROPERTIES);

    assertThat(glueTable.get().parameters())
        .containsEntry(TableProperties.ENCRYPTION_TABLE_KEY, UnitestKMS.MASTER_KEY_NAME1)
        .containsEntry(
            TableProperties.ENCRYPTION_DEK_LENGTH,
            String.valueOf(TableProperties.ENCRYPTION_DEK_LENGTH_DEFAULT))
        .containsKey(BaseMetastoreTableOperations.METADATA_HASH_PROP);
  }

  @Test
  public void testLoadedTableIsEncrypted() {
    catalog.createTable(
        TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), ENCRYPTED_TABLE_PROPERTIES);

    assertThat(encryptionOf(catalog.loadTable(TABLE_IDENTIFIER)))
        .isInstanceOf(StandardEncryptionManager.class);
  }

  @Test
  public void testUnencryptedTableHasNoEncryptionParameters() {
    catalog.createTable(TABLE_IDENTIFIER, SCHEMA);

    assertThat(glueTable.get().parameters())
        .doesNotContainKey(TableProperties.ENCRYPTION_TABLE_KEY)
        .doesNotContainKey(TableProperties.ENCRYPTION_DEK_LENGTH)
        .doesNotContainKey(BaseMetastoreTableOperations.METADATA_HASH_PROP);

    assertThat(encryptionOf(catalog.loadTable(TABLE_IDENTIFIER)))
        .isInstanceOf(PlaintextEncryptionManager.class);
  }

  @Test
  public void testDetectsTamperedMetadataFile() {
    catalog.createTable(
        TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), ENCRYPTED_TABLE_PROPERTIES);

    // the key ID is read from Glue, so tampering has to happen in the metadata file in storage
    InMemoryFileIO io = new InMemoryFileIO();
    String metadataLocation =
        glueTable.get().parameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    TableMetadata tampered =
        TableMetadata.buildFrom(TableMetadataParser.read(io, metadataLocation))
            .setProperties(ImmutableMap.of("tampered", "true"))
            .build();
    io.addFile(
        metadataLocation, TableMetadataParser.toJson(tampered).getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> catalog.loadTable(TABLE_IDENTIFIER))
        .isInstanceOf(RuntimeException.class)
        .hasMessageContaining("might have been modified");
  }

  @Test
  public void testCannotRemoveKeyId() {
    org.apache.iceberg.Table table =
        catalog.createTable(
            TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), ENCRYPTED_TABLE_PROPERTIES);
    UpdateProperties update = table.updateProperties().remove(TableProperties.ENCRYPTION_TABLE_KEY);

    assertThatThrownBy(update::commit)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot remove key ID from an encrypted table");
  }

  @Test
  public void testCannotModifyKeyId() {
    org.apache.iceberg.Table table =
        catalog.createTable(
            TABLE_IDENTIFIER, SCHEMA, PartitionSpec.unpartitioned(), ENCRYPTED_TABLE_PROPERTIES);
    UpdateProperties update =
        table
            .updateProperties()
            .set(TableProperties.ENCRYPTION_TABLE_KEY, UnitestKMS.MASTER_KEY_NAME2);

    assertThatThrownBy(update::commit)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot modify key ID of an encrypted table");
  }

  @Test
  public void testMissingKeyManagementClient() throws Exception {
    try (GlueCatalog catalogWithoutKms = catalogWith(ImmutableMap.of())) {
      assertThatThrownBy(
              () ->
                  catalogWithoutKms.createTable(
                      TABLE_IDENTIFIER,
                      SCHEMA,
                      PartitionSpec.unpartitioned(),
                      ENCRYPTED_TABLE_PROPERTIES))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("Cannot create encryption manager without a key management client");
    }
  }

  private GlueCatalog catalogWith(Map<String, String> extraProperties) {
    Map<String, String> catalogProperties = Maps.newHashMap(extraProperties);
    catalogProperties.put(CatalogProperties.FILE_IO_IMPL, InMemoryFileIO.class.getCanonicalName());

    GlueCatalog glueCatalog = new GlueCatalog();
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        catalogProperties);
    return glueCatalog;
  }

  private static EncryptionManager encryptionOf(org.apache.iceberg.Table table) {
    return ((HasTableOperations) table).operations().encryption();
  }

  private GlueClient mockGlueClient() {
    GlueClient client = Mockito.mock(GlueClient.class);

    Mockito.doAnswer(
            invocation ->
                GetDatabaseResponse.builder()
                    .database(Database.builder().name(DB_NAME).build())
                    .build())
        .when(client)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    Mockito.doAnswer(
            invocation -> {
              Table table = glueTable.get();
              if (table == null) {
                throw EntityNotFoundException.builder().message("Table not found").build();
              }

              return GetTableResponse.builder().table(table).build();
            })
        .when(client)
        .getTable(Mockito.any(GetTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              CreateTableRequest request = invocation.getArgument(0);
              glueTable.set(toGlueTable(request.databaseName(), request.tableInput()));
              return CreateTableResponse.builder().build();
            })
        .when(client)
        .createTable(Mockito.any(CreateTableRequest.class));

    Mockito.doAnswer(
            invocation -> {
              UpdateTableRequest request = invocation.getArgument(0);
              glueTable.set(toGlueTable(request.databaseName(), request.tableInput()));
              return UpdateTableResponse.builder().build();
            })
        .when(client)
        .updateTable(Mockito.any(UpdateTableRequest.class));

    return client;
  }

  private static Table toGlueTable(String databaseName, TableInput input) {
    return Table.builder()
        .databaseName(databaseName)
        .name(input.name())
        .tableType(input.tableType())
        .parameters(input.parameters())
        .storageDescriptor(input.storageDescriptor())
        .build();
  }
}
