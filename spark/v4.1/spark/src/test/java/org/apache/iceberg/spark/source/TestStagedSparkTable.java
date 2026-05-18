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
package org.apache.iceberg.spark.source;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.net.URI;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.TestTemplate;

public class TestStagedSparkTable extends CatalogTestBase {

  @TestTemplate
  public void testAbortStagedChangesCleansUpUncommittedFiles() throws Exception {
    CatalogPlugin plugin = spark.sessionState().catalogManager().catalog(catalogName);
    // StagedSparkTable is only produced by SparkCatalog; the session catalog uses
    // RollbackStagedTable
    assumeThat(plugin).isInstanceOf(SparkCatalog.class);
    SparkCatalog catalog = (SparkCatalog) plugin;

    Identifier ident = Identifier.of(new String[] {"default"}, "abort_staged");
    StructType schema =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("data", DataTypes.StringType, true);

    StagedTable staged = catalog.stageCreate(ident, schema, new Transform[0], ImmutableMap.of());

    // route an append into the staged (uncommitted) transaction so a manifest and a manifest list
    // are written to the table location without the table being committed to the catalog
    Table table = ((SparkTable) staged).table();
    DataFile dataFile =
        DataFiles.builder(table.spec())
            .withPath("/path/to/data-abort.parquet")
            .withFileSizeInBytes(10)
            .withRecordCount(1)
            .build();
    table.newAppend().appendFile(dataFile).commit();

    File metadataDir = new File(URI.create(table.location()).getPath(), "metadata");
    File[] stagedFiles = metadataDir.listFiles((dir, name) -> name.endsWith(".avro"));
    assertThat(stagedFiles)
        .as("staged write should produce manifest and manifest-list files")
        .isNotNull()
        .isNotEmpty();

    staged.abortStagedChanges();

    File[] remaining = metadataDir.listFiles((dir, name) -> name.endsWith(".avro"));
    assertThat(remaining == null || remaining.length == 0)
        .as("abort should delete all uncommitted manifest and manifest-list files")
        .isTrue();
    assertThat(catalog.tableExists(ident))
        .as("staged table must not be created in the catalog after abort")
        .isFalse();

    // abort is best-effort and idempotent: a second call must not throw
    staged.abortStagedChanges();
  }
}
