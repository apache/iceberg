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

import static org.apache.iceberg.aws.AwsProperties.GLUE_CATALOG_ID;
import static org.apache.iceberg.aws.AwsProperties.GLUE_CATALOG_SKIP_NAME_VALIDATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.apache.iceberg.GlueTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.aws.AwsProperties;
import org.apache.iceberg.aws.s3.S3FileIOProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.FederatedDatabase;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.glue.GlueCatalogExtensions;

public class TestGlueExtensionsCatalog {

  private static final String WAREHOUSE_PATH = "s3://bucket";
  private static final String CATALOG_NAME = "glue";
  private GlueClient glue;
  private GlueCatalog glueCatalog;

  @BeforeEach
  public void before() {
    glue = Mockito.mock(GlueClient.class);
    glueCatalog = new GlueCatalog();
    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(),
        new S3FileIOProperties(),
        glue,
        LockManagers.defaultLockManager(),
        ImmutableMap.of());
  }

  @Test
  public void testLoadRedshiftTable() {
    TableIdentifier identifier = TableIdentifier.of("db", "tableName");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    Map<String, String> properties =
        ImmutableMap.of(
            GLUE_CATALOG_SKIP_NAME_VALIDATION, "true",
            GLUE_CATALOG_ID, "123456789012/rs_ns/rs_db");
    Mockito.doReturn(glueTable).when(glueCatalogExtensions).loadTable(identifier);
    Mockito.doReturn(true)
        .when(glueCatalogExtensions)
        .useExtensionsForGlueTable(Mockito.any(Table.class));
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .name("tableName")
                        .storageDescriptor(
                            StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    org.apache.iceberg.Table table = glueCatalog.loadTable(identifier);
    assertThat(table).isInstanceOf(GlueTable.class);
  }

  @Test
  public void testCreateRedshiftTable() {
    TableIdentifier identifier = TableIdentifier.of("db", "tableName");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    Schema schema = new Schema(Types.NestedField.required(1, "id", Types.StringType.get()));
    Map<String, String> properties =
        ImmutableMap.of(
            GLUE_CATALOG_SKIP_NAME_VALIDATION, "true",
            GLUE_CATALOG_ID, "123456789012/rs_ns/rs_db");
    Map<String, String> tableProperties = ImmutableMap.of("aws.write.format", "rms");
    Catalog.TableBuilder builder = Mockito.mock(Catalog.TableBuilder.class);
    Mockito.doReturn(builder).when(builder).withProperties(Mockito.any());
    Mockito.doReturn(builder).when(builder).withLocation(Mockito.any());
    Mockito.doReturn(builder).when(builder).withPartitionSpec(Mockito.any());
    Mockito.doReturn(builder).when(builder).withSortOrder(Mockito.any());
    Mockito.doReturn(builder).when(glueCatalogExtensions).buildTable(identifier, schema);
    Mockito.doReturn(glueTable).when(builder).create();
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .name("tableName")
                        .storageDescriptor(
                            StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    org.apache.iceberg.Table table =
        glueCatalog.createTable(identifier, schema, PartitionSpec.unpartitioned(), tableProperties);
    assertThat(table).isInstanceOf(GlueTable.class);
  }

  @Test
  public void testDropRedshiftTable() {
    TableIdentifier identifier = TableIdentifier.of("db", "tableName");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    Map<String, String> properties =
        ImmutableMap.of(
            GLUE_CATALOG_SKIP_NAME_VALIDATION, "true",
            GLUE_CATALOG_ID, "123456789012:rs_ns/rs_db");
    Mockito.doReturn(glueTable).when(glueCatalogExtensions).loadTable(identifier);
    Mockito.doReturn(true).when(glueCatalogExtensions).dropTable(identifier, true);
    Mockito.doReturn(true)
        .when(glueCatalogExtensions)
        .useExtensionsForGlueTable(Mockito.any(Table.class));
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .name("tableName")
                        .storageDescriptor(
                            StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    glueCatalog.dropTable(identifier);
    Mockito.verify(glueCatalogExtensions).dropTable(identifier, true);
  }

  @Test
  public void testRenameRedshiftTable() {
    TableIdentifier identifier = TableIdentifier.of("db", "tableName");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    GlueTable glueTable = Mockito.mock(GlueTable.class);
    Map<String, String> properties =
        ImmutableMap.of(
            GLUE_CATALOG_SKIP_NAME_VALIDATION, "true",
            GLUE_CATALOG_ID, "123456789012/rs_ns/rs_db");
    Mockito.doReturn(glueTable).when(glueCatalogExtensions).loadTable(identifier);
    Mockito.doReturn(true).when(glueCatalogExtensions).dropTable(identifier, true);
    Mockito.doReturn(true)
        .when(glueCatalogExtensions)
        .useExtensionsForGlueTable(Mockito.any(Table.class));
    Mockito.doReturn(
            GetTableResponse.builder()
                .table(
                    Table.builder()
                        .name("tableName")
                        .storageDescriptor(
                            StorageDescriptor.builder().inputFormat("RedshiftFormat").build())
                        .build())
                .build())
        .when(glue)
        .getTable(Mockito.any(GetTableRequest.class));

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    glueCatalog.dropTable(identifier);
    Mockito.verify(glueCatalogExtensions).dropTable(identifier, true);
  }

  @Test
  public void testCreateRedshiftNamespace() {
    Namespace namespace = Namespace.of("ns");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    Map<String, String> properties = ImmutableMap.of("type", "redshift");
    Mockito.doNothing().when(glueCatalogExtensions).createNamespace(namespace, properties);
    Mockito.doReturn(glueCatalogExtensions).when(glueCatalogExtensions).namespaces();

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    glueCatalog.createNamespace(namespace, ImmutableMap.of("type", "redshift"));
  }

  @Test
  public void testDropRedshiftNamespace() {
    Namespace namespace = Namespace.of("ns");
    GlueCatalogExtensions glueCatalogExtensions = Mockito.mock(GlueCatalogExtensions.class);
    Map<String, String> properties = ImmutableMap.of("type", "redshift");
    Database database =
        Database.builder()
            .name("ns")
            .federatedDatabase(FederatedDatabase.builder().connectionName("aws:redshift").build())
            .build();
    Mockito.doReturn(true).when(glueCatalogExtensions).dropNamespace(namespace);
    Mockito.doReturn(true).when(glueCatalogExtensions).useExtensionsForGlueDatabase(database);
    Mockito.doReturn(glueCatalogExtensions).when(glueCatalogExtensions).namespaces();
    Mockito.doReturn(GetDatabaseResponse.builder().database(database).build())
        .when(glue)
        .getDatabase(Mockito.any(GetDatabaseRequest.class));

    glueCatalog.initialize(
        CATALOG_NAME,
        WAREHOUSE_PATH,
        new AwsProperties(properties),
        new S3FileIOProperties(properties),
        glue,
        LockManagers.defaultLockManager(),
        properties);
    glueCatalog.setExtensions(glueCatalogExtensions);
    glueCatalog.dropNamespace(namespace);
  }
}
