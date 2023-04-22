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
import static org.junit.Assert.assertTrue;

import com.google.cloud.bigquery.biglake.v1.HiveTableOptions;
import com.google.cloud.bigquery.biglake.v1.HiveTableOptions.StorageDescriptor;
import com.google.cloud.bigquery.biglake.v1.Table;
import com.google.cloud.bigquery.biglake.v1.TableName;
import java.io.File;
import java.io.IOException;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.junit.rules.TemporaryFolder;

/** Test utility methods for BigLake Iceberg catalog. */
public final class BigLakeTestUtils {

  public static final String METADATA_LOCATION_PROP = "metadata_location";

  public static Schema getTestSchema() {
    return new Schema(
        required(1, "id", Types.IntegerType.get(), "unique ID"),
        required(2, "data", Types.StringType.get()));
  }

  public static Table createTestTable(
      TemporaryFolder tempFolder, BigLakeCatalog biglakeCatalog, TableName tableName)
      throws IOException {
    Schema schema = getTestSchema();
    TableIdentifier tableIdent = TableIdentifier.of(tableName.getDatabase(), tableName.getTable());
    String tableDir = tempFolder.newFolder(tableName.getTable()).toString();

    biglakeCatalog
        .buildTable(tableIdent, schema)
        .withLocation(tableDir)
        .createTransaction()
        .commitTransaction();

    Optional<String> metadataLocation = getIcebergMetadataFilePath(tableDir);
    assertTrue(metadataLocation.isPresent());
    return Table.newBuilder()
        .setName(tableName.toString())
        .setHiveOptions(
            HiveTableOptions.newBuilder()
                .putParameters(METADATA_LOCATION_PROP, metadataLocation.get())
                .setStorageDescriptor(StorageDescriptor.newBuilder().setLocationUri(tableDir)))
        .build();
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
