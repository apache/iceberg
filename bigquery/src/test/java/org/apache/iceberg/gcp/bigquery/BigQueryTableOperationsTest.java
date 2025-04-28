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

import static org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog.PROJECT_ID;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
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

public class BigQueryTableOperationsTest {

  @TempDir private File tempFolder;
  public static final String METADATA_LOCATION_PROP = "metadata_location";
  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String DATASET_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final TableIdentifier SPARK_TABLE_ID = TableIdentifier.of(DATASET_ID, TABLE_ID);

  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID).setTableId(TABLE_ID);

  private final BigQueryMetastoreClient bigQueryMetaStoreClient =
      mock(BigQueryMetastoreClient.class);

  private BigQueryMetastoreCatalog bigQueryMetastoreCatalog;
  private BigQueryTableOperations tableOps;

  @BeforeEach
  public void before() {
    this.bigQueryMetastoreCatalog = new BigQueryMetastoreCatalog();
    this.bigQueryMetastoreCatalog.setConf(new Configuration());
    String warehouseLocation = tempFolder.toPath().resolve("hive-warehouse").toString();

    bigQueryMetastoreCatalog.initialize(
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
        bigQueryMetaStoreClient);
    this.tableOps = (BigQueryTableOperations) bigQueryMetastoreCatalog.newTableOps(SPARK_TABLE_ID);
  }

  @Test
  public void testFetchLatestMetadataFromBigQuery() throws Exception {
    Table createdTable = createTestTable();
    reset(bigQueryMetaStoreClient);
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE)).thenReturn(createdTable);

    tableOps.refresh();
    assertThat(
            createdTable
                .getExternalCatalogTableOptions()
                .getParameters()
                .getOrDefault(METADATA_LOCATION_PROP, ""))
        .isEqualTo(tableOps.currentMetadataLocation());

    reset(bigQueryMetaStoreClient);
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Refresh fails when table is not found but metadata already presents.
    assertThatThrownBy(() -> tableOps.refresh())
        .isInstanceOf(NoSuchTableException.class)
        .hasMessageContaining("error message getTable");
  }

  @Test
  public void testFailForNonIcebergTable() {
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE))
        .thenReturn(new Table().setTableReference(TABLE_REFERENCE));

    assertThatThrownBy(() -> tableOps.refresh())
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("metadata location not found");
  }

  @Test
  public void testNoOpWhenMetadataAndTableNotFound() {
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    // Table not found won't cause errors when the metadata is null.
    assertThat(tableOps.currentMetadataLocation()).isNull();
    tableOps.refresh();
  }

  @Test
  public void testTableNameAsExpected() {
    assertThat(tableOps.tableName()).isEqualTo("db.tbl");
  }

  @Test
  public void testUseEtagForUpdateTable() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(bigQueryMetaStoreClient);
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryMetaStoreClient.update(any(), any())).thenReturn(tableWithEtag);
    loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ArgumentCaptor<TableReference> tableReferenceArgumentCaptor =
        ArgumentCaptor.forClass(TableReference.class);
    ArgumentCaptor<Table> tableArgumentCaptor = ArgumentCaptor.forClass(Table.class);
    verify(bigQueryMetaStoreClient, times(1))
        .update(tableReferenceArgumentCaptor.capture(), tableArgumentCaptor.capture());
    assertThat(tableReferenceArgumentCaptor.getValue()).isEqualTo(TABLE_REFERENCE);
    assertThat(tableArgumentCaptor.getValue().getEtag()).isEqualTo("etag");
  }

  @Test
  public void testFailWhenEtagMismatch() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    reset(bigQueryMetaStoreClient);
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryMetaStoreClient.update(any(), any()))
        .thenThrow(new ValidationException("error message etag mismatch"));
    assertThatThrownBy(
            () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining(
            "Updating table failed due to conflict updates (etag mismatch). Retry the update");
  }

  @Test
  public void testFailWhenMetadataLocationDiff() throws Exception {
    Table tableWithEtag = createTestTable().setEtag("etag");
    Table tableWithNewMetadata =
        new Table()
            .setEtag("etag")
            .setExternalCatalogTableOptions(
                new ExternalCatalogTableOptions()
                    .setParameters(ImmutableMap.of(METADATA_LOCATION_PROP, "a/new/location")));

    reset(bigQueryMetaStoreClient);
    // Two invocations, for loadTable and commit.
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE))
        .thenReturn(tableWithEtag, tableWithNewMetadata);

    org.apache.iceberg.Table loadedTable = bigQueryMetastoreCatalog.loadTable(SPARK_TABLE_ID);

    when(bigQueryMetaStoreClient.update(any(), any())).thenReturn(tableWithEtag);
    assertThatThrownBy(
            () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("is not same as the current table metadata location");
  }

  @Test
  public void testCreateTableCommitSucceeds() throws Exception {
    var testTable = createTestTable();
    when(bigQueryMetaStoreClient.create(any(Table.class))).thenReturn(testTable);
    when(bigQueryMetaStoreClient.load(
            new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID)))
        .thenReturn(
            new Dataset()
                .setExternalCatalogDatasetOptions(
                    new ExternalCatalogDatasetOptions()
                        .setDefaultStorageLocationUri("build/db_folder")));

    Schema schema = testSchema();
    bigQueryMetastoreCatalog.createTable(SPARK_TABLE_ID, schema, PartitionSpec.unpartitioned());
  }

  /** Creates a test table to have Iceberg metadata files in place. */
  private Table createTestTable() throws Exception {
    when(bigQueryMetaStoreClient.load(TABLE_REFERENCE))
        .thenThrow(new NoSuchTableException("error message getTable"));
    return createTestTable(tempFolder, bigQueryMetastoreCatalog, TABLE_REFERENCE);
  }

  public static Table createTestTable(
      File tempFolder,
      BigQueryMetastoreCatalog bigQueryMetastoreCatalog,
      TableReference tableReference)
      throws IOException {
    Schema schema = testSchema();
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

  private static Schema testSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
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
