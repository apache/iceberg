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
package org.apache.iceberg.gcp.bigquery.metastore;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;

/** Test utility methods for BigQuery Metastore Iceberg catalog. */
public final class BigQueryMetastoreTestUtils {

  /** A utility class that is not to be instantiated */
  private BigQueryMetastoreTestUtils() {}

  public static final String METADATA_LOCATION_PROP = "metadata_location";

  public static Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }

  public static Table createTestTable(
      File tempFolder,
      BigQueryMetastoreCatalog bigQueryMetastoreCatalog,
      TableReference tableReference)
      throws IOException {
    Schema schema = getTestSchema();
    TableIdentifier tableIdentifier =
        TableIdentifier.of(tableReference.getDatasetId(), tableReference.getTableId());
    String tableDir = tempFolder.toPath().resolve(tableReference.getTableId()).toString();

    bigQueryMetastoreCatalog
        .buildTable(tableIdentifier, schema)
        .withLocation(tableDir)
        .createTransaction()
        .commitTransaction();

    Optional<String> metadataLocation = getIcebergMetadataFilePath(tableDir);
    assertTrue(metadataLocation.isPresent());
    return new Table()
        .setTableReference(tableReference)
        .setExternalCatalogTableOptions(
            new ExternalCatalogTableOptions()
                .setStorageDescriptor(new StorageDescriptor().setLocationUri(tableDir))
                .setParameters(
                    Collections.singletonMap(METADATA_LOCATION_PROP, metadataLocation.get())));
  }

  public static Optional<String> getIcebergMetadataFilePath(String tableDir) throws IOException {
    for (File file :
        FileUtils.listFiles(new File(tableDir), TrueFileFilter.INSTANCE, TrueFileFilter.INSTANCE)) {
      if (file.getCanonicalPath().endsWith(".json")) {
        return Optional.of(file.getCanonicalPath());
      }
    }
    return Optional.empty();
  }
}
