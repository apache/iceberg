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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpResponse;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.LowLevelHttpRequest;
import com.google.api.client.http.LowLevelHttpResponse;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.client.testing.http.HttpTesting;
import com.google.api.client.testing.http.MockHttpContent;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.api.client.testing.http.MockLowLevelHttpRequest;
import com.google.api.client.testing.http.MockLowLevelHttpResponse;
import com.google.api.client.util.Data;
import com.google.api.services.bigquery.Bigquery;
import com.google.api.services.bigquery.Bigquery.Datasets;
import com.google.api.services.bigquery.Bigquery.Tables;
import com.google.api.services.bigquery.BigqueryRequest;
import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.ExternalCatalogTableOptions;
import com.google.api.services.bigquery.model.SerDeInfo;
import com.google.api.services.bigquery.model.StorageDescriptor;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchIcebergTableException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ServiceFailureException;
import org.apache.iceberg.exceptions.ServiceUnavailableException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.gcp.bigquery.metastore.BigQueryMetastoreUtils;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

public class BigQueryClientImplTest {

  public static final JsonFactory JSON_FACTORY = GsonFactory.getDefaultInstance();

  private static final String GCP_PROJECT = "my-project";
  private static final String GCP_REGION = "europe-west4";
  private static final String DATASET_ID = "db";
  private static final String TABLE_ID = "tbl";
  private static final DatasetReference DATASET_REFERENCE =
      new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID);
  private static final TableReference TABLE_REFERENCE =
      new TableReference().setProjectId(GCP_PROJECT).setDatasetId(DATASET_ID).setTableId(TABLE_ID);
  private final Dataset dataset =
      new Dataset()
          .setEtag("datasetEtag")
          .setDatasetReference(DATASET_REFERENCE)
          .setLocation(GCP_REGION)
          .setExternalCatalogDatasetOptions(
              new ExternalCatalogDatasetOptions()
                  .setParameters(Maps.newHashMap(Map.of("initialDatasetKey", "initialDatasetVal")))
                  .setDefaultStorageLocationUri("someDefaultStorageLocationUri"));
  private final Table table =
      new Table()
          .setEtag("tableEtag")
          .setTableReference(TABLE_REFERENCE)
          .setExternalCatalogTableOptions(
              new ExternalCatalogTableOptions()
                  .setParameters(
                      Map.of(
                          "initialTableKey",
                          "initialTableVal",
                          BaseMetastoreTableOperations.METADATA_LOCATION_PROP,
                          "location",
                          BaseMetastoreTableOperations.TABLE_TYPE_PROP,
                          BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE))
                  .setConnectionId("someConnectionId")
                  .setStorageDescriptor(
                      new StorageDescriptor()
                          .setLocationUri("someLocationUri")
                          .setInputFormat(BigQueryMetastoreUtils.FILE_INPUT_FORMAT)
                          .setOutputFormat(BigQueryMetastoreUtils.FILE_OUTPUT_FORMAT)
                          .setSerdeInfo(
                              new SerDeInfo()
                                  .setSerializationLibrary(
                                      BigQueryMetastoreUtils.SERIALIZATION_LIBRARY))));

  private final Map<Integer, Class<? extends Exception>> errorCodeToIcebergException =
      Map.of(
          HttpStatusCodes.STATUS_CODE_UNAUTHORIZED,
          NotAuthorizedException.class,
          HttpStatusCodes.STATUS_CODE_BAD_REQUEST,
          BadRequestException.class,
          HttpStatusCodes.STATUS_CODE_FORBIDDEN,
          ForbiddenException.class,
          HttpStatusCodes.STATUS_CODE_PRECONDITION_FAILED,
          ValidationException.class,
          HttpStatusCodes.STATUS_CODE_NOT_FOUND,
          NotFoundException.class,
          HttpStatusCodes.STATUS_CODE_SERVER_ERROR,
          ServiceFailureException.class,
          HttpStatusCodes.STATUS_CODE_SERVICE_UNAVAILABLE,
          ServiceUnavailableException.class,
          12345, // This lucky number is here to represent a random unsupported HTTP status code
          RuntimeIOException.class);

  private final Bigquery bigqueryMock = mock(Bigquery.class);

  private final Datasets datasetsMock = mock(Datasets.class);
  private final Datasets.Insert datasetInsertMock = mock(Datasets.Insert.class);
  private final Datasets.Get datasetGetMock = mock(Datasets.Get.class);
  private final Datasets.Delete datasetDeleteMock = mock(Datasets.Delete.class);
  private final Datasets.Update datasetUpdateMock = mock(Datasets.Update.class);
  private final Datasets.List datasetListMock = mock(Datasets.List.class);

  private final Tables tablesMock = mock(Tables.class);
  private final Tables.Insert tableInsertMock = mock(Tables.Insert.class);
  private final Tables.Get tableGetMock = mock(Tables.Get.class);
  private final Tables.Get unsupportedTableGetMock = mock(Tables.Get.class);
  private final Tables.Patch tablePatchMock = mock(Tables.Patch.class);
  private final Tables.Delete tableDeleteMock = mock(Tables.Delete.class);
  private final Tables.List tableListMock = mock(Tables.List.class);

  private BigQueryClient client;

  @BeforeEach
  public void before() throws Exception {
    prepareMocks();
    setJsonFactory();
    this.client = new BigQueryClientImpl(bigqueryMock);
  }

  private void prepareMocks() throws IOException {
    when(bigqueryMock.datasets()).thenReturn(datasetsMock);
    when(bigqueryMock.tables()).thenReturn(tablesMock);

    when(datasetsMock.insert(GCP_PROJECT, dataset)).thenReturn(datasetInsertMock);
    when(datasetsMock.get(GCP_PROJECT, DATASET_ID)).thenReturn(datasetGetMock);
    when(datasetsMock.delete(GCP_PROJECT, DATASET_ID)).thenReturn(datasetDeleteMock);
    when(datasetsMock.update(GCP_PROJECT, DATASET_ID, dataset)).thenReturn(datasetUpdateMock);
    when(datasetsMock.list(GCP_PROJECT)).thenReturn(datasetListMock);

    when(tablesMock.insert(GCP_PROJECT, DATASET_ID, table)).thenReturn(tableInsertMock);
    when(tablesMock.get(GCP_PROJECT, DATASET_ID, TABLE_ID)).thenReturn(tableGetMock);
    when(tablesMock.delete(GCP_PROJECT, DATASET_ID, TABLE_ID)).thenReturn(tableDeleteMock);
    when(tablesMock.list(GCP_PROJECT, DATASET_ID)).thenReturn(tableListMock);
  }

  private void setJsonFactory() {
    this.dataset.setFactory(JSON_FACTORY);
    this.table.setFactory(JSON_FACTORY);
  }

  @Test
  public void testCreateDataset_success() throws Exception {
    when(datasetInsertMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));

    assertEquals(client.createDataset(dataset), dataset);
  }

  @Test
  public void testCreateDataset_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(datasetInsertMock, Set.of(), () -> client.createDataset(dataset));
  }

  @Test
  public void testGetDataset_success() throws Exception {
    when(datasetGetMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));

    assertEquals(client.getDataset(DATASET_REFERENCE), dataset);
  }

  @Test
  public void testGetDataset_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(
        datasetGetMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> client.getDataset(DATASET_REFERENCE));
  }

  @Test
  public void testGetDataset_throwsNoSuchNamespaceExceptionWhenNotFound() throws Exception {
    when(datasetGetMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    NoSuchNamespaceException exception =
        assertThrows(NoSuchNamespaceException.class, () -> client.getDataset(DATASET_REFERENCE));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testDeleteDataset_success() throws Exception {
    when(datasetDeleteMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_OK));

    client.deleteDataset(DATASET_REFERENCE);

    verify(datasetDeleteMock, times(1)).executeUnparsed();
  }

  @Test
  public void testDeleteDataset_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(
        datasetDeleteMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> client.deleteDataset(DATASET_REFERENCE));
  }

  @Test
  public void testDeleteDataset_throwsNoSuchNamespaceExceptionWhenNotFound() throws Exception {
    when(datasetDeleteMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    NoSuchNamespaceException exception =
        assertThrows(NoSuchNamespaceException.class, () -> client.deleteDataset(DATASET_REFERENCE));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testSetDatasetParameters_success() throws Exception {
    when(datasetGetMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));
    dataset.getExternalCatalogDatasetOptions().getParameters().put("newKey", "newVal");
    when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
        .thenReturn(datasetUpdateMock);
    when(datasetUpdateMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));

    assertEquals(
        dataset, client.setDatasetParameters(DATASET_REFERENCE, Map.of("newKey", "newVal")));
  }

  @Test
  public void testSetDatasetParameters_throwsCorrectGeneralIcebergExceptions() throws Exception {
    String initialDatasetStringState = dataset.toPrettyString();
    dataset.getExternalCatalogDatasetOptions().getParameters().put("newKey", "newVal");

    testThrowsCommonExceptions(
        datasetUpdateMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> {
          when(datasetGetMock.executeUnparsed())
              .thenReturn(buildResponse(initialDatasetStringState));
          when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
              .thenReturn(datasetUpdateMock);
          client.setDatasetParameters(DATASET_REFERENCE, Map.of("newKey", "newVal"));
        });
  }

  @Test
  public void testSetDatasetParameters_throwsNoSuchNamespaceExceptionWhenNotFound()
      throws Exception {
    when(datasetGetMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));
    dataset.getExternalCatalogDatasetOptions().getParameters().put("newKey", "newVal");
    when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
        .thenReturn(datasetUpdateMock);
    when(datasetUpdateMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    NoSuchNamespaceException exception =
        assertThrows(
            NoSuchNamespaceException.class,
            () -> client.setDatasetParameters(DATASET_REFERENCE, Map.of("newKey", "newVal")));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testRemoveDatasetParameters_success() throws Exception {
    when(datasetGetMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));
    dataset.getExternalCatalogDatasetOptions().getParameters().remove("initialDatasetKey");
    when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
        .thenReturn(datasetUpdateMock);
    when(datasetUpdateMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));

    assertEquals(
        dataset, client.removeDatasetParameters(DATASET_REFERENCE, Set.of("initialDatasetKey")));
  }

  @Test
  public void testRemoveDatasetParameters_throwsCorrectGeneralIcebergExceptions() throws Exception {
    String initialDatasetStringState = dataset.toPrettyString();
    dataset.getExternalCatalogDatasetOptions().getParameters().remove("initialDatasetKey");

    testThrowsCommonExceptions(
        datasetUpdateMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> {
          when(datasetGetMock.executeUnparsed())
              .thenReturn(buildResponse(initialDatasetStringState));
          when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
              .thenReturn(datasetUpdateMock);
          client.removeDatasetParameters(DATASET_REFERENCE, Set.of("initialDatasetKey"));
        });
  }

  @Test
  public void testRemoveDatasetParameters_throwsNoSuchNamespaceExceptionWhenNotFound()
      throws Exception {
    when(datasetGetMock.executeUnparsed()).thenReturn(buildResponse(dataset.toPrettyString()));
    dataset.getExternalCatalogDatasetOptions().getParameters().remove("initialDatasetKey");
    when(datasetUpdateMock.setRequestHeaders(new HttpHeaders().setIfMatch(dataset.getEtag())))
        .thenReturn(datasetUpdateMock);
    when(datasetUpdateMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    NoSuchNamespaceException exception =
        assertThrows(
            NoSuchNamespaceException.class,
            () -> client.removeDatasetParameters(DATASET_REFERENCE, Set.of("initialDatasetKey")));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testListDatasets_success() throws Exception {
    DatasetList.Datasets dataset1 = generateDatasetListItem("dataset1");
    DatasetList.Datasets dataset2 = generateDatasetListItem("dataset2");
    DatasetList firstPage =
        new DatasetList()
            .setDatasets(List.of(dataset1, dataset2))
            .setNextPageToken("firstPageToken");
    firstPage.setFactory(JSON_FACTORY);

    DatasetList.Datasets dataset3 = generateDatasetListItem("dataset3");
    DatasetList secondPage = new DatasetList().setDatasets(List.of(dataset3));
    secondPage.setFactory(JSON_FACTORY);

    when(datasetListMock.setPageToken(null))
        .thenReturn(datasetListMock); // For getting the first page
    when(datasetListMock.setPageToken("firstPageToken"))
        .thenReturn(datasetListMock); // For getting the second page
    // Two invocations for the two pages.
    when(datasetListMock.executeUnparsed())
        .thenReturn(
            buildResponse(firstPage.toPrettyString()), buildResponse(secondPage.toPrettyString()));

    assertEquals(List.of(dataset1, dataset2, dataset3), client.listDatasets(GCP_PROJECT));
  }

  @Test
  public void testListDatasets_throwsCorrectGeneralIcebergExceptions() throws Exception {
    when(datasetListMock.setPageToken(null))
        .thenReturn(datasetListMock); // For constructing the request to get the first page
    testThrowsCommonExceptions(datasetListMock, Set.of(), () -> client.listDatasets(GCP_PROJECT));
  }

  @Test
  public void testCreateTable_success() throws Exception {
    when(tableInsertMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));

    assertEquals(table, client.createTable(table));
  }

  @Test
  public void testCreateTable_throwsWhenNotBqmsTable() {
    table.setExternalCatalogTableOptions(null);

    assertThrows(NoSuchIcebergTableException.class, () -> client.createTable(table));
  }

  @Test
  public void testCreateTable_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(tableInsertMock, Set.of(), () -> client.createTable(table));
  }

  @Test
  public void testGetTable_success() throws Exception {
    when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));

    assertEquals(table, client.getTable(TABLE_REFERENCE));
  }

  @Test
  public void testGetTable_throwsWhenNotBqmsTable() throws Exception {
    table.setExternalCatalogTableOptions(null);
    when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));

    assertThrows(NoSuchIcebergTableException.class, () -> client.getTable(TABLE_REFERENCE));
  }

  @Test
  public void testGetTable_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(
        tableGetMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> client.getTable(TABLE_REFERENCE));
  }

  @Test
  public void testGetTable_throwsNoSuchTableExceptionWhenNotFound() throws Exception {
    when(tableGetMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    assertThrows(NoSuchTableException.class, () -> client.getTable(TABLE_REFERENCE));
  }

  @Test
  public void testPatchTable_success() throws Exception {
    ExternalCatalogTableOptions newExternalCatalogTableOptions =
        table.getExternalCatalogTableOptions();
    Table patch =
        new Table()
            .setSchema(Data.nullOf(TableSchema.class))
            .setExternalCatalogTableOptions(newExternalCatalogTableOptions);

    when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch)).thenReturn(tablePatchMock);
    when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
        .thenReturn(tablePatchMock);
    when(tablePatchMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));

    assertEquals(
        newExternalCatalogTableOptions,
        client.patchTable(TABLE_REFERENCE, table).getExternalCatalogTableOptions());
  }

  @Test
  public void testPatchTable_clearsSchema() throws Exception {
    table.setSchema(Data.nullOf(TableSchema.class));
    String tableStateAfterPatch = table.toPrettyString();
    table.setSchema(new TableSchema());

    Table patch =
        new Table()
            .setSchema(Data.nullOf(TableSchema.class))
            .setExternalCatalogTableOptions(table.getExternalCatalogTableOptions());

    when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch)).thenReturn(tablePatchMock);
    when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
        .thenReturn(tablePatchMock);
    when(tablePatchMock.executeUnparsed()).thenReturn(buildResponse(tableStateAfterPatch));

    assertEquals(
        Data.nullOf(TableSchema.class), client.patchTable(TABLE_REFERENCE, table).getSchema());
  }

  @Test
  public void testPatchTable_throwsCorrectGeneralIcebergExceptions() throws Exception {
    ExternalCatalogTableOptions newExternalCatalogTableOptions =
        table.getExternalCatalogTableOptions();
    Table patch =
        new Table()
            .setSchema(Data.nullOf(TableSchema.class))
            .setExternalCatalogTableOptions(newExternalCatalogTableOptions);

    testThrowsCommonExceptions(
        tablePatchMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> {
          when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
              .thenReturn(tablePatchMock);
          when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch))
              .thenReturn(tablePatchMock);
          client.patchTable(TABLE_REFERENCE, table);
        });
  }

  @Test
  public void testPatchTableParameters_throwsNoSuchTableExceptionWhenNotFound() throws Exception {
    Table patch =
        new Table()
            .setSchema(Data.nullOf(TableSchema.class))
            .setExternalCatalogTableOptions(table.getExternalCatalogTableOptions());
    when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
        .thenReturn(tablePatchMock);
    when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch)).thenReturn(tablePatchMock);
    when(tablePatchMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    assertThrows(NoSuchTableException.class, () -> client.patchTable(TABLE_REFERENCE, table));
  }

  @Test
  public void testPatchTableParameters_throwsBadRequestExceptionWhenConnectionNotFound()
      throws Exception {
    table.getExternalCatalogTableOptions().setConnectionId("A-connection-that-does-not-exist");
    Table patch =
        new Table()
            .setSchema(Data.nullOf(TableSchema.class))
            .setExternalCatalogTableOptions(table.getExternalCatalogTableOptions());
    when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
        .thenReturn(tablePatchMock);
    when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch)).thenReturn(tablePatchMock);
    when(tablePatchMock.executeUnparsed())
        .thenReturn(
            buildResponseWithStatus(
                HttpStatusCodes.STATUS_CODE_NOT_FOUND,
                "Not found: Connection A-connection-that-does-not-exist"));

    assertThrows(BadRequestException.class, () -> client.patchTable(TABLE_REFERENCE, table));
  }

  @Test
  public void testRenameTable_success() throws Exception {
    when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));
    String newTableId = "newTableId";
    TableReference newTableReference =
        new TableReference()
            .setProjectId(GCP_PROJECT)
            .setDatasetId(DATASET_ID)
            .setTableId(newTableId);
    table.setTableReference(newTableReference);
    Table patch = new Table().setTableReference(newTableReference);
    when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch)).thenReturn(tablePatchMock);
    when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
        .thenReturn(tablePatchMock);
    when(tablePatchMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));

    assertEquals(table, client.renameTable(TABLE_REFERENCE, newTableId));
  }

  @Test
  public void testRenameTable_throwsCorrectGeneralIcebergExceptions() throws Exception {
    String initialTableStringState = table.toPrettyString();
    String newTableId = "newTableId";
    TableReference newTableReference =
        new TableReference()
            .setProjectId(GCP_PROJECT)
            .setDatasetId(DATASET_ID)
            .setTableId(newTableId);
    table.setTableReference(newTableReference);
    Table patch = new Table().setTableReference(newTableReference);

    testThrowsCommonExceptions(
        tablePatchMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> {
          when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(initialTableStringState));
          when(tablePatchMock.setRequestHeaders(new HttpHeaders().setIfMatch(table.getEtag())))
              .thenReturn(tablePatchMock);
          when(tablesMock.patch(GCP_PROJECT, DATASET_ID, TABLE_ID, patch))
              .thenReturn(tablePatchMock);
          client.renameTable(TABLE_REFERENCE, newTableId);
        });
  }

  @Test
  public void testRenameTable_throwsNoSuchTableExceptionWhenNotFound() throws Exception {
    when(tableGetMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    assertThrows(
        NoSuchTableException.class, () -> client.renameTable(TABLE_REFERENCE, "someNewTableId"));
  }

  @Test
  public void testDeleteTable_success() throws Exception {
    when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));
    when(tableDeleteMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_OK));

    client.deleteTable(TABLE_REFERENCE);

    verify(tableDeleteMock, times(1)).executeUnparsed();
  }

  @Test
  public void testDeleteTable_throwsCorrectGeneralIcebergExceptions() throws Exception {
    testThrowsCommonExceptions(
        tableDeleteMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> {
          when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));
          client.deleteTable(TABLE_REFERENCE);
        });
  }

  @Test
  public void testDeleteTable_throwsNoSuchTableExceptionWhenNotFoundWhileDeleting()
      throws Exception {
    when(tableGetMock.executeUnparsed()).thenReturn(buildResponse(table.toPrettyString()));
    when(tableDeleteMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    NoSuchTableException exception =
        assertThrows(NoSuchTableException.class, () -> client.deleteTable(TABLE_REFERENCE));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testDeleteTable_throwsNoSuchTableExceptionWhenNotFoundInitially() throws Exception {
    when(tableGetMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));
    when(tableDeleteMock.executeUnparsed())
        .thenThrow(new IllegalStateException("Why are you even calling this, test?"));

    NoSuchTableException exception =
        assertThrows(NoSuchTableException.class, () -> client.deleteTable(TABLE_REFERENCE));
    assertEquals("Got status code: 404", exception.getMessage());
  }

  @Test
  public void testListTables_success() throws Exception {
    when(tableGetMock.executeUnparsed())
        .thenReturn(buildResponse(table.toPrettyString())) // Response for table1
        .thenReturn(buildResponse(table.toPrettyString())); // Response for table3
    when(unsupportedTableGetMock.executeUnparsed())
        .thenThrow(
            new NoSuchIcebergTableException(
                "We don't support that")); // Response for table2ThatIsNotSupported

    TableList.Tables table1 = generateTableListItem("table1");
    when(tablesMock.get(
            table1.getTableReference().getProjectId(),
            table1.getTableReference().getDatasetId(),
            table1.getTableReference().getTableId()))
        .thenReturn(tableGetMock);
    TableList.Tables table2ThatIsNotSupported = generateTableListItem("table2ThatIsNotSupported");
    when(tablesMock.get(
            table2ThatIsNotSupported.getTableReference().getProjectId(),
            table2ThatIsNotSupported.getTableReference().getDatasetId(),
            table2ThatIsNotSupported.getTableReference().getTableId()))
        .thenReturn(unsupportedTableGetMock);
    TableList firstPage =
        new TableList()
            .setTables(List.of(table1, table2ThatIsNotSupported))
            .setNextPageToken("firstPageToken");
    firstPage.setFactory(JSON_FACTORY);

    TableList.Tables table3 = generateTableListItem("table3");
    when(tablesMock.get(
            table3.getTableReference().getProjectId(),
            table3.getTableReference().getDatasetId(),
            table3.getTableReference().getTableId()))
        .thenReturn(tableGetMock);
    TableList secondPage = new TableList().setTables(List.of(table3));
    secondPage.setFactory(JSON_FACTORY);

    when(tableListMock.setPageToken(null)).thenReturn(tableListMock); // For getting the first page
    when(tableListMock.setPageToken("firstPageToken"))
        .thenReturn(tableListMock); // For getting the second page
    // Two invocations for the two pages.
    when(tableListMock.executeUnparsed())
        .thenReturn(
            buildResponse(firstPage.toPrettyString()), buildResponse(secondPage.toPrettyString()));

    // Notice how table2ThatIsNotSupported is intentionally not included.
    assertEquals(
        List.of(table1, table3),
        client.listTables(DATASET_REFERENCE, /* filterUnsupportedTables= */ true));
  }

  @Test
  public void testListTables_throwsCorrectGeneralIcebergExceptions() throws Exception {
    when(tableListMock.setPageToken(null))
        .thenReturn(tableListMock); // For constructing the request to get the first page
    testThrowsCommonExceptions(
        tableListMock,
        Set.of(HttpStatusCodes.STATUS_CODE_NOT_FOUND),
        () -> client.listTables(DATASET_REFERENCE, true));
  }

  @Test
  public void testListTables_throwsNoSuchNamespaceExceptionWhenDatasetNotFound() throws Exception {
    when(tableListMock.setPageToken(null))
        .thenReturn(tableListMock); // For constructing the request to get the first page
    when(tableListMock.executeUnparsed())
        .thenReturn(buildResponseWithStatus(HttpStatusCodes.STATUS_CODE_NOT_FOUND));

    assertThrows(NoSuchNamespaceException.class, () -> client.listTables(DATASET_REFERENCE, false));
  }

  private DatasetList.Datasets generateDatasetListItem(String datasetId) {
    return new DatasetList.Datasets()
        .setDatasetReference(
            new DatasetReference().setProjectId(GCP_PROJECT).setDatasetId(datasetId));
  }

  private TableList.Tables generateTableListItem(String tableId) {
    return new TableList.Tables()
        .setTableReference(new TableReference().setProjectId(GCP_PROJECT).setTableId(tableId));
  }

  private void testThrowsCommonExceptions(
      @SuppressWarnings("rawtypes") BigqueryRequest requestMocker, // Intended to be generic
      Set<Integer> excludedStatusCodes,
      Executable executable)
      throws IOException {
    for (Map.Entry<Integer, Class<? extends Exception>> codeToException :
        errorCodeToIcebergException.entrySet()) {
      int statusCode = codeToException.getKey();
      if (excludedStatusCodes.contains(statusCode)) {
        continue;
      }

      when(requestMocker.executeUnparsed())
          .thenReturn(buildResponseWithStatus(codeToException.getKey()));
      assertThrows(codeToException.getValue(), executable);
    }
  }

  private HttpResponse buildResponseWithStatus(int statusCode) throws IOException {
    return buildResponseWithStatus(statusCode, null);
  }

  private HttpResponse buildResponseWithStatus(int statusCode, String content) throws IOException {
    return buildResponse(statusCode, content);
  }

  private HttpResponse buildResponse(String content) throws IOException {
    return buildResponse(HttpStatusCodes.STATUS_CODE_OK, content);
  }

  private HttpResponse buildResponse(int statusCode, String content) throws IOException {
    HttpTransport mockHttpTransport =
        new MockHttpTransport() {
          @Override
          public LowLevelHttpRequest buildRequest(String method, String url) {
            return new MockLowLevelHttpRequest() {
              @Override
              public LowLevelHttpResponse execute() {
                MockLowLevelHttpResponse result = new MockLowLevelHttpResponse();
                if (content != null) {
                  result.setContent(content);
                }
                result.setStatusCode(statusCode);
                if (statusCode != HttpStatusCodes.STATUS_CODE_OK) {
                  result.setReasonPhrase("Got status code: " + statusCode);
                }
                result.setContentEncoding("UTF-8");
                result.setContentType("application/json");
                result.setHeaderNames(List.of("header1", "header2"));
                result.setHeaderValues(List.of("header1", "header2"));
                return result;
              }
            };
          }
        };

    return mockHttpTransport
        .createRequestFactory()
        .buildPostRequest(HttpTesting.SIMPLE_GENERIC_URL, new MockHttpContent())
        .setThrowExceptionOnExecuteError(false)
        .setParser(JSON_FACTORY.createJsonObjectParser())
        .execute();
  }
}
