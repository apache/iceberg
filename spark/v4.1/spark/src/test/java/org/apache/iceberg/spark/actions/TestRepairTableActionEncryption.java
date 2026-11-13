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
package org.apache.iceberg.spark.actions;

import static org.apache.iceberg.spark.data.TestHelpers.dataFiles;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.data.GenericFileWriterFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class TestRepairTableActionEncryption extends CatalogTestBase {

  private static final int FORMAT_VERSION = 3;

  TestRepairTableActionEncryption() {
    this.catalogName = SparkCatalogConfig.HIVE.catalogName();
    this.implementation = SparkCatalogConfig.HIVE.implementation();
    this.catalogConfig = Maps.newHashMap(SparkCatalogConfig.HIVE.properties());
    catalogConfig.put(CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getCanonicalName());
  }

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  // Checks what the repair flow assumes about encryption across components: data files are read
  // with their key metadata, rewritten entries keep it, and new manifests are encrypted.
  @Test
  void repairFileMetricsWithEncryption() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('encryption.key-id'='%s', 'format-version'='%s')",
        tableName, UnitestKMS.MASTER_KEY_NAME1, FORMAT_VERSION);
    validationCatalog.initialize(catalogName, catalogConfig);
    Table table = validationCatalog.loadTable(tableIdent);
    OutputFile output = table.io().newOutputFile(temp.resolve("data.parquet").toString());
    DataWriter<Record> writer =
        new GenericFileWriterFactory.Builder(table)
            .build()
            .newDataWriter(table.encryption().encrypt(output), table.spec(), null);
    try (writer) {
      writer.write(GenericRecord.create(table.schema()).copy("id", 1L, "data", "a"));
    }

    DataFile original = writer.toDataFile();
    assertThat(original.keyMetadata()).isNotNull();
    DataFile corrupt =
        DataFiles.builder(table.spec())
            .copy(original)
            .withRecordCount(original.recordCount() + 1)
            .build();
    table.newFastAppend().appendFile(corrupt).commit();

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    table.refresh();
    List<DataFile> files = dataFiles(table);
    assertThat(files).hasSize(1);
    DataFile repaired = files.get(0);
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.keyMetadata()).isEqualTo(original.keyMetadata());
    assertThat(table.currentSnapshot().dataManifests(table.io()).get(0).keyMetadata()).isNotNull();
  }
}
