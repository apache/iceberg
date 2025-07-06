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
interface BigQueryMetastoreClient {

  /**
   * Creates and returns a new dataset.
   *
   * @param dataset the dataset to create
   */
  Dataset create(Dataset dataset);

  /**
   * Returns a dataset.
   *
   * @param datasetReference full dataset reference
   */
  Dataset load(DatasetReference datasetReference);

  /**
   * Deletes a dataset.
   *
   * @param datasetReference full dataset reference
   */
  void delete(DatasetReference datasetReference);

  /**
   * Sets (Adds or Overwrites) the specified parameters on the Dataset.
   *
   * <p>Loads the dataset, compares the parameters, and performs a retrying-update ONLY if the
   * parameters will change.
   *
   * @param datasetReference Reference to the Dataset.
   * @param parameters Map of parameters to add/overwrite.
   * @return {@code true} if the Dataset was updated, {@code false} if no changes were needed.
   */
  boolean setParameters(DatasetReference datasetReference, Map<String, String> parameters);

  /**
   * Removes the specified parameters from the Dataset.
   *
   * <p>Loads the dataset, compares the parameters, and performs a retrying-update ONLY if the
   * parameters will change as a result.
   *
   * @param datasetReference Reference to the Dataset.
   * @param parameters Set of parameter keys to remove.
   * @return {@code true} if the Dataset was updated, {@code false} if no changes were needed (e.g.
   *     keys did not exist).
   */
  boolean removeParameters(DatasetReference datasetReference, Set<String> parameters);

  /**
   * Lists datasets under a given project
   *
   * @param projectId the identifier of the project to list datasets under
   */
  List<Datasets> list(String projectId);

  /**
   * Creates and returns a new table.
   *
   * @param table body of the table to create
   */
  Table create(Table table);

  /**
   * Returns a table.
   *
   * @param tableReference full table reference
   */
  Table load(TableReference tableReference);

  /**
   * Updates the catalog table options of an Iceberg table and returns the updated table.
   *
   * @param tableReference full table reference
   * @param table to patch
   */
  Table update(TableReference tableReference, Table table);

  /**
   * Deletes a table.
   *
   * @param tableReference full table reference
   */
  void delete(TableReference tableReference);

  /**
   * Returns all tables in a database.
   *
   * @param datasetReference full dataset reference
   * @param listAllTables if true, fetches every item on the list including unsupported Iceberg
   *     Tables. If false, unsupported Iceberg Tables will be filtered out.
   */
  List<Tables> list(DatasetReference datasetReference, boolean listAllTables);
}
