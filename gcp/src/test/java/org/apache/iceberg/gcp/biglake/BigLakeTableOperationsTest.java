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
package org.apache.iceberg.gcp.biglake;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.AbortedException;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions.StorageDescriptor;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import io.grpc.Status.Code;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.gcp.GCPProperties;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

public class BigLakeTableOperationsTest {

  @TempDir private Path temp;

  private static final String CATALOG_NAME = "iceberg";

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "us";
  private static final String CATALOG_ID = "biglake";
  private static final String DB_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final TableName TABLE_NAME =
      TableName.of(GCP_PROJECT, GCP_REGION, CATALOG_ID, DB_ID, TABLE_ID);
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_ID, TABLE_ID);

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get(), "unique ID"),
          required(2, "data", Types.StringType.get()));

  private BigLakeClient bigLakeClient = mock(BigLakeClient.class);
  private BigLakeCatalog bigLakeCatalog;
  private BigLakeTableOperations tableOps;

  @BeforeEach
  public void before() {
    ImmutableMap<String, String> properties =
        ImmutableMap.of(
            GCPProperties.PROJECT_ID,
            GCP_PROJECT,
            GCPProperties.REGION,
            GCP_REGION,
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toFile().getAbsolutePath(),
            GCPProperties.BIGLAKE_CATALOG_ID,
            CATALOG_ID);

    bigLakeCatalog = new BigLakeCatalog();
    bigLakeCatalog.setConf(new Configuration());
    bigLakeCatalog.initialize(CATALOG_NAME, properties, bigLakeClient);
    tableOps = (BigLakeTableOperations) bigLakeCatalog.newTableOps(TABLE_IDENTIFIER);
  }

  @AfterEach
  public void after() throws Exception {
    bigLakeCatalog.close();
  }

  @Test
  public void testDoCommitShouldUseEtagForUpdateTable() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable = createTestTable();

    Table tableWithEtag = createdTable.toBuilder().setEtag("etag").build();
    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(TABLE_IDENTIFIER);

    when(bigLakeClient.updateTableParameters(any(), any(), any())).thenReturn(tableWithEtag);
    loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit();

    ArgumentCaptor<TableName> nameCaptor = ArgumentCaptor.forClass(TableName.class);
    ArgumentCaptor<String> etagCaptor = ArgumentCaptor.forClass(String.class);
    verify(bigLakeClient, times(1))
        .updateTableParameters(nameCaptor.capture(), any(), etagCaptor.capture());
    assertThat(nameCaptor.getValue()).isEqualTo(TABLE_NAME);
    assertThat(etagCaptor.getValue()).isEqualTo("etag");
  }

  @Test
  public void testDoCommitShouldFailWhenEtagMismatch() throws Exception {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenThrow(new NoSuchTableException("error message getTable"));
    Table createdTable = createTestTable();

    Table tableWithEtag = createdTable.toBuilder().setEtag("etag").build();
    reset(bigLakeClient);
    when(bigLakeClient.getTable(TABLE_NAME)).thenReturn(tableWithEtag, tableWithEtag);

    org.apache.iceberg.Table loadedTable = bigLakeCatalog.loadTable(TABLE_IDENTIFIER);

    when(bigLakeClient.updateTableParameters(any(), any(), any()))
        .thenThrow(
            new AbortedException(
                new RuntimeException("error message etag mismatch"),
                GrpcStatusCode.of(Code.ABORTED),
                false));

    assertThatThrownBy(
            () -> loadedTable.updateSchema().addColumn("n", Types.IntegerType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessage("Updating table failed due to conflicting updates (etag mismatch)");
  }

  @Test
  public void testInitRefreshShouldReturnNullForNonIcebergTable() {
    when(bigLakeClient.getTable(TABLE_NAME))
        .thenReturn(Table.newBuilder().setName(TABLE_NAME.toString()).build());

    assertThat(tableOps.refresh()).isNull();
  }

  @Test
  public void testTableName() {
    assertThat(tableOps.tableName()).isEqualTo("iceberg.db.tbl");
  }

  private Table createTestTable() throws IOException {
    TableIdentifier tableIdent =
        TableIdentifier.of(TABLE_NAME.getDatabase(), TABLE_NAME.getTable());
    String tableDir =
        new File(temp.toFile().getAbsolutePath(), TABLE_NAME.getTable()).getAbsolutePath();

    bigLakeCatalog
        .buildTable(tableIdent, SCHEMA)
        .withLocation(tableDir)
        .createTransaction()
        .commitTransaction();

    Optional<String> metadataLocation = getAnyJsonFilePath(tableDir);
    assertThat(metadataLocation).isPresent();
    return Table.newBuilder()
        .setName(TABLE_NAME.toString())
        .setHiveOptions(
            HiveTableOptions.newBuilder()
                .putParameters(
                    BaseMetastoreTableOperations.METADATA_LOCATION_PROP, metadataLocation.get())
                .setStorageDescriptor(StorageDescriptor.newBuilder().setLocationUri(tableDir)))
        .build();
  }

  private static Optional<String> getAnyJsonFilePath(String tableDir) throws IOException {
    for (File file :
        FileUtils.listFiles(new File(tableDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
      if (file.getCanonicalPath().endsWith(".json")) {
        return Optional.of(file.getCanonicalPath());
      }
    }
    return Optional.empty();
  }
}
