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

import static org.apache.iceberg.Files.localInput;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RepairTable;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;

class TestRepairTableActionEncryption extends CatalogTestBase {

  private static final int FORMAT_VERSION = 3;

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    Map<String, String> properties = Maps.newHashMap(SparkCatalogConfig.HIVE.properties());
    properties.put(CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getCanonicalName());
    return new Object[][] {
      {SparkCatalogConfig.HIVE.catalogName(), SparkCatalogConfig.HIVE.implementation(), properties}
    };
  }

  @AfterEach
  void removeTable() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  void repairFileMetricsWithEncryption() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string) USING iceberg "
            + "TBLPROPERTIES ('encryption.key-id'='%s', 'format-version'='%s')",
        tableName, UnitestKMS.MASTER_KEY_NAME1, FORMAT_VERSION);
    append(tableName, "{\"id\":1,\"data\":\"a\"}", "{\"id\":2,\"data\":\"b\"}");
    validationCatalog.initialize(catalogName, catalogConfig);
    Table table = validationCatalog.loadTable(tableIdent);

    DataFile original = onlyDataFile(table);
    assertThat(original.keyMetadata()).isNotNull();
    assertThat(original.keyMetadata().remaining()).isPositive();

    Snapshot snapshot = table.currentSnapshot();
    ManifestFile manifest = snapshot.dataManifests(table.io()).get(0);
    DataFile corrupt =
        DataFiles.builder(table.spec())
            .copy(original)
            .withRecordCount(original.recordCount() + 1)
            .withFileSizeInBytes(original.fileSizeInBytes() + 1)
            .build();
    EncryptedOutputFile output =
        table
            .encryption()
            .encrypt(table.io().newOutputFile(temp.resolve("corrupt-manifest.avro").toString()));
    // Preserve the entry's lineage and encryption key metadata while corrupting its metrics.
    ManifestWriter<DataFile> writer =
        ManifestFiles.write(FORMAT_VERSION, table.spec(), output, null);
    try (ManifestWriter<DataFile> closeableWriter = writer) {
      closeableWriter.existing(
          corrupt,
          snapshot.snapshotId(),
          original.dataSequenceNumber(),
          original.fileSequenceNumber());
    }

    ManifestFile corruptManifest = writer.toManifestFile();
    table.rewriteManifests().deleteManifest(manifest).addManifest(corruptManifest).commit();
    table.refresh();
    DataFile beforeRepair = onlyDataFile(table);
    assertThat(beforeRepair.recordCount()).isEqualTo(corrupt.recordCount());
    assertThat(beforeRepair.fileSizeInBytes()).isEqualTo(corrupt.fileSizeInBytes());

    RepairTable.Result result = SparkActions.get().repairTable(table).repairFileMetrics().execute();

    assertThat(result.repairedEntryCount()).isEqualTo(1);
    assertThat(result.repairedManifests()).hasSize(1);

    table.refresh();
    DataFile repaired = onlyDataFile(table);
    assertThat(repaired.location()).isEqualTo(original.location());
    assertThat(repaired.recordCount()).isEqualTo(original.recordCount());
    assertThat(repaired.fileSizeInBytes()).isEqualTo(original.fileSizeInBytes());
    assertThat(repaired.keyMetadata()).isEqualTo(original.keyMetadata());

    ManifestFile repairedManifest = table.currentSnapshot().dataManifests(table.io()).get(0);
    assertThat(repairedManifest.path()).isNotEqualTo(corruptManifest.path());
    assertThat(repairedManifest.keyMetadata()).isNotNull();
    assertThat(repairedManifest.keyMetadata().remaining()).isPositive();
    byte[] magic = Ciphers.GCM_STREAM_MAGIC_STRING.getBytes(StandardCharsets.UTF_8);
    // Read the physical bytes without FileIO decryption, as in TestTableEncryption.
    try (SeekableInputStream stream = localInput(repairedManifest.path()).newStream()) {
      assertThat(stream.readNBytes(magic.length)).isEqualTo(magic);
    }

    sql("REFRESH TABLE %s", tableName);
    assertEquals(
        "Should return all expected rows after repair",
        List.of(row(1L, "a"), row(2L, "b")),
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  private DataFile onlyDataFile(Table table) throws IOException {
    List<ManifestFile> manifests = table.currentSnapshot().dataManifests(table.io());
    assertThat(manifests).hasSize(1);
    List<DataFile> files = Lists.newArrayList();
    try (CloseableIterable<DataFile> reader =
        ManifestFiles.read(manifests.get(0), table.io(), table.specs())) {
      reader.forEach(file -> files.add(file.copy()));
    }

    assertThat(files).hasSize(1);
    return files.get(0);
  }
}
