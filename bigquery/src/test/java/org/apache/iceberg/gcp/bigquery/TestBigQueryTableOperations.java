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
package org.apache.iceberg.gcp.bigquery;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.PROJECT_ID;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.StorageDescriptor;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableReference;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

public class TestBigQueryTableOperations {

  @TempDir private File tempFolder;
  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String NS = "db";
  private static final String TABLE = "tbl";
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of(NS, TABLE);

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(GCP_PROJECT).setDatasetId(NS).setTableId(TABLE);

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  private final BigQueryMetastoreClient client = mock(BigQueryMetastoreClient.class);

  private BigQueryMetastoreCatalog catalog;
  private BigQueryTableOperations tableOps;

  @BeforeEach
  public void before() {
    this.catalog = new BigQueryMetastoreCatalog();
    this.catalog.setConf(new Configuration());
    String warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    catalog.initialize(
        "CATALOG_ID",
        ImmutableMap.of(
            PROJECT_ID,
            GCP_PROJECT,
            CatalogProperties.WAREHOUSE_LOCATION,
            warehouseLocation,
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.hadoop.HadoopFileIO"),
        GCP_PROJECT,
        GCP_REGION,
        client);
    this.tableOps = (BigQueryTableOperations) catalog.newTableOps(IDENTIFIER);
  }

  @Test
  public void fetchLatestMetadataFromBigQuery() throws Exception {
    Table createdTable = createTestTable();
    reset(client);
    when(client.load(TABLE_REFERENCE)).thenReturn(createdTable);

    tableOps.refresh();
    assertThat(
            createdTable
                .getExternalCatalogTableOptions()
                .getParameters()
                .getOrDefault(METADATA_LOCATION_PROP, ""))
        .isEqualTo(tableOps.currentMetadataLocation());

    reset(client);
    when(client.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Refresh fails when table is not found but metadata already presents.
    assertThatThrownBy(() -> tableOps.refresh())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("error message getTable");
  }

  @Test
  public void loadNonIcebergTableFails() {
    when(client.load(TABLE_REFERENCE)).thenReturn(new Table().setTableReference(TABLE_REFERENCE));

    assertThatThrownBy(() -> tableOps.refresh())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("metadata location not found");
  }

  @Test
  public void loadNoOpWhenMetadataAndTableNotFound() {
    when(client.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Table not found won't cause errors when the metadata is null.
    assertThat(tableOps.currentMetadataLocation()).isNull();
    assertThatNoException().isThrownBy(() -> tableOps.refresh());
  }

  @Test
  public void loadTableNameAsExpected() {
    assertThat(tableOps.tableName()).isEqualTo("db.tbl");
  }

  @Test
  public void useEtagForUpdateTable() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(client);
    when(client.load(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = catalog.loadTable(IDENTIFIER);

    when(client.update(any(), any())).thenReturn(tableWithEtag);
    loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ArgumentCaptor<TableReference> tableReferenceArgumentCaptor =
        ArgumentCaptor.forClass(TableReference.class);
    ArgumentCaptor<Table> tableArgumentCaptor = ArgumentCaptor.forClass(Table.class);
    verify(client, times(1))
        .update(tableReferenceArgumentCaptor.capture(), tableArgumentCaptor.capture());
    assertThat(tableReferenceArgumentCaptor.getValue()).isEqualTo(TABLE_REFERENCE);
    assertThat(tableArgumentCaptor.getValue().getEtag()).isEqualTo("etag");
  }

  @Test
  public void failWhenEtagMismatch() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(client);
    when(client.load(TABLE_REFERENCE)).thenReturn(tableWithEtag);

    org.apache.iceberg.Table loadedTable = catalog.loadTable(IDENTIFIER);

    when(client.update(any(), any()))
        .thenThrow(new ValidationException("error message etag mismatch"));
    assertThatThrownBy(
            () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Updating table failed due to conflict updates (etag mismatch). Retry the update");
  }

  @Test
  public void failWhenMetadataLocationDiff() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    Table tableWithNewMetadata =
        new Table()
            .setEtag("etag")
            .setExternalCatalogTableOptions(
                new ExternalCatalogTableOptions()
                    .setParameters(ImmutableMap.of(METADATA_LOCATION_PROP, "a/new/location")));

    reset(client);
    // Two invocations, for loadTable and commit.
    when(client.load(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithNewMetadata);

    org.apache.iceberg.Table loadedTable = catalog.loadTable(IDENTIFIER);

    when(client.update(any(), any())).thenReturn(tableWithEtag);
    assertThatThrownBy(
            () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("is not same as the current table metadata location");
  }

  @Test
  public void createTableCommitSucceeds() throws Exception {
    var testTable = createTestTable();
    TableReference expectedTableReference =
        new TableReference().setProjectId(GCP_PROJECT).setDatasetId(NS).setTableId(TABLE);
    ArgumentCaptor<Table> createdTableCaptor = ArgumentCaptor.forClass(Table.class);

    when(client.create(createdTableCaptor.capture())).thenReturn(testTable);
    when(client.load(new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(NS)))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setDefaultStorageLocationUri("build/db_folder")));

    Schema schema = SCHEMA;
    catalog.createTable(IDENTIFIER, schema, PartitionSpec.unpartitioned());

    Table capturedTable = createdTableCaptor.getValue();
    assertThat(capturedTable.getTableReference()).isEqualTo(expectedTableReference);
    assertThat(
            capturedTable
                .getExternalCatalogTableOptions()
                .getParameters()
                .get(METADATA_LOCATION_PROP))
        .isNotNull();
    assertThat(
            capturedTable.getExternalCatalogTableOptions().getStorageDescriptor().getLocationUri())
        .startsWith("build/db_folder/");

    reset(client);
    when(client.load(expectedTableReference)).thenReturn(testTable);
    org.apache.iceberg.Table loadedTable = catalog.loadTable(IDENTIFIER);
    assertThat(loadedTable).isNotNull();
    assertThat(loadedTable.name()).isEqualTo(catalog.name() + "." + IDENTIFIER);
    assertThat(loadedTable.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  /** Creates a test table to have Iceberg metadata files in place. */
  private Table createTestTable() throws Exception {
    when(client.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    return createTestTable(tempFolder, catalog, TABLE_REFERENCE);
  }

  public static Table createTestTable(
      File tempFolder,
      BigQueryMetastoreCatalog bigQueryMetastoreCatalog,
      TableReference tableReference)
      throws IOException {
    Schema schema = SCHEMA;
    TableIdentifier tableIdentifier =
        TableIdentifier.of(tableReference.getDatasetId(), tableReference.getTableId());
    String tableDir = tempFolder.toPath().resolve(tableReference.getTableId()).toString();

    bigQueryMetastoreCatalog
        .buildTable(tableIdentifier, schema)
        .withLocation(tableDir)
        .createTransaction()
        .commitTransaction();

    Optional<String> metadataLocation = metadataFilePath(tableDir);
    assertThat(metadataLocation).isPresent();
    return new Table()
        .setTableReference(tableReference)
        .setExternalCatalogTableOptions(
            new ExternalCatalogTableOptions()
                .setStorageDescriptor(new StorageDescriptor().setLocationUri(tableDir))
                .setParameters(
                    Collections.singletonMap(METADATA_LOCATION_PROP, metadataLocation.get())));
  }

  private static Optional<String> metadataFilePath(String tableDir) throws IOException {
    for (File file :
        FileUtils.listFiles(new File(tableDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
      if (file.getCanonicalPath().endsWith(".json")) {
        return Optional.of(file.getCanonicalPath());
      }
    }

    return Optional.empty();
  }
}
