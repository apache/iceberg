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
package org.apache.iceberg.spark.sql;

import static org.apache.iceberg.Files.localInput;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ChecksumFileSystem;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.CatalogTestBase;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.types.Types;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.mockito.internal.util.collections.Iterables;

public class TestTableEncryption extends CatalogTestBase {
  private static Map<String, String> appendCatalogEncryptionProperties(Map<String, String> props) {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.putAll(props);
    newProps.put(CatalogProperties.ENCRYPTION_KMS_IMPL, UnitestKMS.class.getCanonicalName());
    return newProps;
  }

  @Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  protected static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        appendCatalogEncryptionProperties(SparkCatalogConfig.HIVE.properties())
      }
    };
  }

  @BeforeEach
  public void createTables() {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( "
            + "'encryption.key-id'='%s', 'format-version'='3')",
        tableName, UnitestKMS.MASTER_KEY_NAME1);

    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);
  }

  @AfterEach
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @TestTemplate
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
  }

  private static List<DataFile> currentDataFiles(Table table) {
    return Streams.stream(table.newScan().planFiles())
        .map(FileScanTask::file)
        .collect(Collectors.toList());
  }

  @TestTemplate
  public void testRefresh() {
    catalog.initialize(catalogName, catalogConfig);
    Table table = catalog.loadTable(tableIdent);

    assertThat(currentDataFiles(table)).isNotEmpty();

    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0), (6, 'f', float('NaN'))", tableName);

    table.refresh();
    assertThat(currentDataFiles(table)).isNotEmpty();
  }

  @TestTemplate
  public void testTransaction() {
    catalog.initialize(catalogName, catalogConfig);

    Table table = catalog.loadTable(tableIdent);

    List<DataFile> dataFiles = currentDataFiles(table);
    Transaction transaction = table.newTransaction();
    AppendFiles append = transaction.newAppend();

    // add an arbitrary datafile
    append.appendFile(dataFiles.get(0));
    append.commit();
    transaction.commitTransaction();

    assertThat(currentDataFiles(table).size()).isEqualTo(dataFiles.size() + 1);
  }

  @TestTemplate
  public void testInsertAndDelete() {
    sql("INSERT INTO %s VALUES (4, 'd', 4.0), (5, 'e', 5.0), (6, 'f', float('NaN'))", tableName);

    List<Object[]> expected =
        ImmutableList.of(
            row(1L, "a", 1.0F),
            row(2L, "b", 2.0F),
            row(3L, "c", Float.NaN),
            row(4L, "d", 4.0F),
            row(5L, "e", 5.0F),
            row(6L, "f", Float.NaN));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));

    sql("DELETE FROM %s WHERE id < 4", tableName);

    expected = ImmutableList.of(row(4L, "d", 4.0F), row(5L, "e", 5.0F), row(6L, "f", Float.NaN));

    assertEquals(
        "Should return all expected rows",
        expected,
        sql("SELECT * FROM %s ORDER BY id", tableName));
  }

  @TestTemplate
  public void testMetadataTamperproofing() throws IOException {
    ChecksumFileSystem fs = ((ChecksumFileSystem) FileSystem.newInstance(new Configuration()));
    catalog.initialize(catalogName, catalogConfig);

    Table table = catalog.loadTable(tableIdent);
    TableMetadata currentMetadata = ((HasTableOperations) table).operations().current();
    Path metadataFile = new Path(currentMetadata.metadataFileLocation());
    Path previousMetadataFile = new Path(Iterables.firstOf(currentMetadata.previousFiles()).file());

    // manual FS tampering: replacing the current metadata file with a previous one
    Path crcPath = fs.getChecksumFile(metadataFile);
    fs.delete(crcPath, false);
    fs.delete(metadataFile, false);
    fs.rename(previousMetadataFile, metadataFile);

    assertThatThrownBy(() -> catalog.loadTable(tableIdent))
        .hasMessageContaining(
            String.format(
                "The current metadata file %s might have been modified. Hash of metadata loaded from storage differs from HMS-stored metadata hash.",
                metadataFile));
  }

  @TestTemplate
  public void testKeyDelete() {
    assertThatThrownBy(
            () -> sql("ALTER TABLE %s UNSET TBLPROPERTIES (`encryption.key-id`)", tableName))
        .hasMessageContaining("Cannot remove key in encrypted table");
  }

  @TestTemplate
  public void testKeyAlter() {
    assertThatThrownBy(
            () -> sql("ALTER TABLE %s SET TBLPROPERTIES ('encryption.key-id'='abcd')", tableName))
        .hasMessageContaining("Cannot modify key in encrypted table");
  }

  @TestTemplate
  public void testDirectDataFileRead() {
    List<Object[]> dataFileTable =
        sql("SELECT file_path FROM %s.%s", tableName, MetadataTableType.ALL_DATA_FILES);
    List<String> dataFiles =
        Streams.concat(dataFileTable.stream())
            .map(row -> (String) row[0])
            .collect(Collectors.toList());

    if (dataFiles.isEmpty()) {
      throw new RuntimeException("No data files found for table " + tableName);
    }

    Schema schema = new Schema(optional(0, "id", Types.IntegerType.get()));
    for (String filePath : dataFiles) {
      assertThatThrownBy(
              () ->
                  Parquet.read(localInput(filePath))
                      .project(schema)
                      .callInit()
                      .build()
                      .iterator()
                      .next())
          .isInstanceOf(ParquetCryptoRuntimeException.class)
          .hasMessageContaining("Trying to read file with encrypted footer. No keys available");
    }
  }

  @TestTemplate
  public void testManifestEncryption() throws IOException {
    List<Object[]> manifestFileTable =
        sql("SELECT path FROM %s.%s", tableName, MetadataTableType.MANIFESTS);

    List<String> manifestFiles =
        Streams.concat(manifestFileTable.stream())
            .map(row -> (String) row[0])
            .collect(Collectors.toList());

    if (manifestFiles.isEmpty()) {
      throw new RuntimeException("No manifest files found for table " + tableName);
    }

    String metadataFolderPath = null;

    // Check encryption of manifest files
    for (String manifestFilePath : manifestFiles) {
      checkMetadataFileEncryption(localInput(manifestFilePath));

      if (metadataFolderPath == null) {
        metadataFolderPath = new File(manifestFilePath).getParent().replaceFirst("file:", "");
      }
    }

    if (metadataFolderPath == null) {
      throw new RuntimeException("No metadata folder found for table " + tableName);
    }

    // Find manifest list and metadata files; check their encryption
    File[] listOfMetadataFiles = new File(metadataFolderPath).listFiles();
    boolean foundManifestListFile = false;

    for (File metadataFile : listOfMetadataFiles) {
      if (metadataFile.getName().startsWith("snap-")) {
        foundManifestListFile = true;
        checkMetadataFileEncryption(localInput(metadataFile));
      }
    }

    if (!foundManifestListFile) {
      throw new RuntimeException("No manifest list files found for table " + tableName);
    }
  }

  private void checkMetadataFileEncryption(InputFile file) throws IOException {
    SeekableInputStream stream = file.newStream();
    byte[] magic = new byte[4];
    stream.read(magic);
    stream.close();
    assertThat(magic).isEqualTo(Ciphers.GCM_STREAM_MAGIC_STRING.getBytes(StandardCharsets.UTF_8));
  }
}
