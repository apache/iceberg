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

import com.google.api.services.bigquery.model.Dataset;
import com.google.api.services.bigquery.model.DatasetList;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.ExternalCatalogDatasetOptions;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList;
import com.google.api.services.bigquery.model.TableReference;
import com.google.cloud.bigquery.BigQueryOptions;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class FakeBigQueryMetaStoreClient implements BigQueryMetaStoreClient {
  private final Map<DatasetReference, Dataset> datasets = Maps.newHashMap();
  private final Map<TableReference, Table> tables = Maps.newHashMap();

  public FakeBigQueryMetaStoreClient(BigQueryOptions options)
      throws IOException, GeneralSecurityException {}

  public Map<DatasetReference, Dataset> datasets() {
    return datasets;
  }

  public Map<TableReference, Table> tables() {
    return tables;
  }

  @Override
  public Dataset createDataset(Dataset dataset) {
    if (datasets.containsKey(dataset.getDatasetReference())) {
      throw new AlreadyExistsException(
          "Namespace already exists: %s", dataset.getDatasetReference());
    }
    // Assign an ETag for consistency
    dataset.setEtag(generateEtag());
    datasets.put(dataset.getDatasetReference(), dataset);
    return dataset;
  }

  @Override
  public Dataset getDataset(DatasetReference datasetReference) {
    Dataset dataset = datasets.get(datasetReference);
    if (dataset == null) {
      throw new NoSuchNamespaceException(
          "Namespace does not exist: %s", datasetReference.getDatasetId());
    }
    return dataset;
  }

  @Override
  public void deleteDataset(DatasetReference datasetReference) {
    if (!datasets.containsKey(datasetReference)) {
      throw new NoSuchNamespaceException("Dataset not found: %s", datasetReference);
    }

    // Check if there are any tables in this dataset
    if (tables.keySet().stream()
        .anyMatch(tableRef -> tableRef.getDatasetId().equals(datasetReference.getDatasetId()))) {
      throw new NamespaceNotEmptyException(
          "Dataset is not empty. Cannot delete: %s", datasetReference);
    }

    datasets.remove(datasetReference);
  }

  @Override
  public Dataset setDatasetParameters(
      DatasetReference datasetReference, Map<String, String> parameters) {
    Dataset dataset = getDataset(datasetReference);
    if (dataset.getExternalCatalogDatasetOptions() == null) {
      dataset.setExternalCatalogDatasetOptions(new ExternalCatalogDatasetOptions());
    }
    Map<String, String> finalParameters = Maps.newHashMap(parameters);
    dataset.setExternalCatalogDatasetOptions(
        dataset.getExternalCatalogDatasetOptions().setParameters(finalParameters));
    return updateDataset(dataset);
  }

  @Override
  public Dataset removeDatasetParameters(
      DatasetReference datasetReference, Set<String> parameters) {
    Dataset dataset = getDataset(datasetReference);
    if (dataset.getExternalCatalogDatasetOptions() == null) {
      dataset.setExternalCatalogDatasetOptions(new ExternalCatalogDatasetOptions());
    }
    Map<String, String> finalParameters =
        dataset.getExternalCatalogDatasetOptions().getParameters() == null
            ? Maps.newHashMap()
            : Maps.newHashMap(dataset.getExternalCatalogDatasetOptions().getParameters());
    parameters.forEach(finalParameters::remove);
    dataset.setExternalCatalogDatasetOptions(
        dataset.getExternalCatalogDatasetOptions().setParameters(finalParameters));
    return updateDataset(dataset);
  }

  @Override
  public List<DatasetList.Datasets> listDatasets(String projectId) {
    return datasets.values().stream()
        .map(
            dataset -> {
              DatasetList.Datasets ds = new DatasetList.Datasets();
              ds.setDatasetReference(dataset.getDatasetReference());
              return ds;
            })
        .collect(Collectors.toList());
  }

  @Override
  public Table createTable(Table table) {
    if (tables.containsKey(table.getTableReference())) {
      throw new AlreadyExistsException("Table already exists: %s", table.getTableReference());
    }
    // Assign an ETag
    table.setEtag(generateEtag());
    tables.put(table.getTableReference(), table);
    return table;
  }

  @Override
  public Table getTable(TableReference tableReference) {
    Table table = tables.get(tableReference);
    if (table == null) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }
    return table;
  }

  @Override
  public Table patchTable(TableReference tableReference, Table table) {
    Table existingTable = getTable(tableReference);
    if (existingTable == null) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }
    // Robust ETag validation
    if (table.getEtag() != null && !table.getEtag().equals(existingTable.getEtag())) {
      throw new CommitFailedException("Etag mismatch: concurrent modification");
    }
    // Update ETag
    existingTable.setEtag(generateEtag());
    existingTable.setExternalCatalogTableOptions(table.getExternalCatalogTableOptions());
    return existingTable;
  }

  @Override
  public Table renameTable(TableReference tableToRename, String newTableId) {
    throw new UnsupportedOperationException("Rename table is not supported");
  }

  @Override
  public void deleteTable(TableReference tableReference) {
    if (!tables.containsKey(tableReference)) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }
    tables.remove(tableReference);
  }

  @Override
  public List<TableList.Tables> listTables(
      DatasetReference datasetReference, boolean filterUnsupportedTables) {
    return tables.values().stream()
        .filter(
            table ->
                table.getTableReference().getDatasetId().equals(datasetReference.getDatasetId()))
        .map(
            table -> {
              TableList.Tables tbl = new TableList.Tables();
              tbl.setTableReference(table.getTableReference());
              return tbl;
            })
        .collect(Collectors.toList());
  }

  public Dataset updateDataset(Dataset dataset) {
    DatasetReference datasetReference = dataset.getDatasetReference();
    if (!datasets.containsKey(datasetReference)) {
      throw new NoSuchNamespaceException(
          "Namespace does not exist: %s", datasetReference.getDatasetId());
    }
    Dataset existingDataset = datasets.get(datasetReference);
    if (existingDataset == null) {
      throw new NoSuchNamespaceException(
          "Namespace does not exist: %s", datasetReference.getDatasetId());
    }
    // Robust ETag validation
    if (dataset.getEtag() != null && !dataset.getEtag().equals(existingDataset.getEtag())) {
      throw new CommitFailedException("Etag mismatch: concurrent modification");
    }
    // Update ETag
    dataset.setEtag(generateEtag());
    // Simulate update by replacing the existing dataset
    datasets.put(datasetReference, dataset);
    return dataset;
  }

  private String generateEtag() {
    return UUID.randomUUID().toString();
  }
}
