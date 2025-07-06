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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public class FakeBigQueryMetastoreClient implements BigQueryMetastoreClient {
  private final Map<DatasetReference, Dataset> datasets = Maps.newHashMap();
  private final Map<TableReference, Table> tables = Maps.newHashMap();

  public FakeBigQueryMetastoreClient() {}

  @Override
  public Dataset create(Dataset dataset) {
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
  public Dataset load(DatasetReference datasetReference) {
    Dataset dataset = datasets.get(datasetReference);
    if (dataset == null) {
      throw new NoSuchNamespaceException(
          "Namespace does not exist: %s", datasetReference.getDatasetId());
    }

    return dataset;
  }

  @Override
  public void delete(DatasetReference datasetReference) {
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
  public boolean setParameters(DatasetReference datasetReference, Map<String, String> parameters) {
    Dataset dataset = load(datasetReference);

    ExternalCatalogDatasetOptions existingOptions = dataset.getExternalCatalogDatasetOptions();

    Map<String, String> existingParameters =
        (existingOptions == null || existingOptions.getParameters() == null)
            ? Maps.newHashMap()
            : Maps.newHashMap(existingOptions.getParameters());

    Map<String, String> newParameters = Maps.newHashMap(existingParameters);
    newParameters.putAll(parameters);

    if (Objects.equals(existingParameters, newParameters)) {
      return false;
    }

    if (dataset.getExternalCatalogDatasetOptions() == null) {
      dataset.setExternalCatalogDatasetOptions(new ExternalCatalogDatasetOptions());
    }
    dataset.getExternalCatalogDatasetOptions().setParameters(newParameters);

    updateDataset(dataset);
    return true;
  }

  @Override
  public boolean removeParameters(DatasetReference datasetReference, Set<String> parameters) {
    Dataset dataset = load(datasetReference);

    ExternalCatalogDatasetOptions existingOptions = dataset.getExternalCatalogDatasetOptions();

    // If there are no options or no parameters, we cannot remove anything.
    if (existingOptions == null
        || existingOptions.getParameters() == null
        || existingOptions.getParameters().isEmpty()) {
      return false;
    }

    Map<String, String> existingParameters =
        Maps.newHashMap(existingOptions.getParameters()); // Copy
    Map<String, String> newParameters = Maps.newHashMap(existingParameters);
    parameters.forEach(newParameters::remove);

    if (Objects.equals(existingParameters, newParameters)) {
      return false;
    }

    dataset.getExternalCatalogDatasetOptions().setParameters(newParameters);
    updateDataset(dataset);
    return true;
  }

  @Override
  public List<DatasetList.Datasets> list(String projectId) {
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
  public Table create(Table table) {
    if (tables.containsKey(table.getTableReference())) {
      throw new AlreadyExistsException("Table already exists: %s", table.getTableReference());
    }
    // Assign an ETag
    table.setEtag(generateEtag());
    tables.put(table.getTableReference(), table);
    return table;
  }

  @Override
  public Table load(TableReference tableReference) {
    Table table = tables.get(tableReference);
    if (table == null) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }

    return table;
  }

  @Override
  public Table update(TableReference tableReference, Table table) {
    Table existingTable = tables.get(tableReference);
    if (existingTable == null) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }

    String incomingEtag = table.getEtag();
    String requiredEtag = existingTable.getEtag();

    // The real patch() uses an If-Match header which is passed separately,
    // NOT on the incoming table object.
    // The BigQueryTableOperations does NOT set the ETag on the Table object
    // it passes to the client update() method.
    // For a fake, we assume the ETag check needs to be simulated based on
    // state, BUT the real client.update() expects the ETAG as a separate parameter
    // (or implicitly via setIfMatch header, which this Fake doesn't see).
    // To make the fake usable, we'll assume that if an ETag *is* present
    // on the incoming table object, it must match.
    if (incomingEtag != null && !incomingEtag.equals(requiredEtag)) {
      throw new CommitFailedException(
          "Etag mismatch for table: %s. Required: %s, Found: %s",
          tableReference, requiredEtag, incomingEtag);
    }

    Table tableToStore = table.clone();
    tableToStore.setEtag(generateEtag());
    tables.put(tableReference, tableToStore);

    return tableToStore.clone();
  }

  @Override
  public void delete(TableReference tableReference) {
    if (tables.remove(tableReference) == null) {
      throw new NoSuchTableException("Table not found: %s", tableReference);
    }
  }

  @Override
  public List<TableList.Tables> list(DatasetReference datasetReference, boolean listAllTables) {
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
