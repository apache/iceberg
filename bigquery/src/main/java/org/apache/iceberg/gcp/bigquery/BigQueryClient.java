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
import com.google.api.services.bigquery.model.DatasetList.Datasets;
import com.google.api.services.bigquery.model.DatasetReference;
import com.google.api.services.bigquery.model.Table;
import com.google.api.services.bigquery.model.TableList.Tables;
import com.google.api.services.bigquery.model.TableReference;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A client of Google BigQuery Metastore functions over the BigQuery service. Uses the Google
 * BigQuery API.
 */
public interface BigQueryClient {

  /**
   * Creates and returns a new dataset.
   *
   * @param dataset the dataset to create
   */
  Dataset createDataset(Dataset dataset);

  /**
   * Returns a dataset.
   *
   * @param datasetReference full dataset reference
   */
  Dataset getDataset(DatasetReference datasetReference);

  /**
   * Deletes a dataset.
   *
   * @param datasetReference full dataset reference
   */
  void deleteDataset(DatasetReference datasetReference);

  /**
   * Updates parameters of a dataset or adds them if did not exist, leaving already-existing ones
   * intact.
   *
   * @param datasetReference full dataset reference
   * @param parameters metadata parameters to add
   * @return dataset after patch
   */
  Dataset setDatasetParameters(DatasetReference datasetReference, Map<String, String> parameters);

  /**
   * Removes given set of keys of parameters of a dataset. Ignores keys that do not exist already.
   *
   * @param datasetReference full dataset reference
   * @param parameters metadata parameter keys to remove
   * @return dataset after patch
   */
  Dataset removeDatasetParameters(DatasetReference datasetReference, Set<String> parameters);

  /**
   * Lists datasets under a given project
   *
   * @param projectId the identifier of the project to list datasets under
   */
  List<Datasets> listDatasets(String projectId);

  /**
   * Creates and returns a new table.
   *
   * @param table body of the table to create
   */
  Table createTable(Table table);

  /**
   * Returns a table.
   *
   * @param tableReference full table reference
   */
  Table getTable(TableReference tableReference);

  /**
   * Updates the catalog table options of an Iceberg table and returns the updated table.
   *
   * @param tableReference full table reference
   * @param table to patch
   */
  Table patchTable(TableReference tableReference, Table table);

  /**
   * Renames a table.
   *
   * @param tableToRename full table reference
   * @param newTableId new table identifier
   */
  Table renameTable(TableReference tableToRename, String newTableId);

  /**
   * Deletes a table.
   *
   * @param tableReference full table reference
   */
  void deleteTable(TableReference tableReference);

  /**
   * Returns all tables in a database.
   *
   * @param datasetReference full dataset reference
   * @param filterUnsupportedTables if true, fetches every item on the list to verify it is
   *     supported, in order to filter the list from unsupported Iceberg tables
   */
  List<Tables> listTables(DatasetReference datasetReference, boolean filterUnsupportedTables);
}
