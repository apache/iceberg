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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.Ciphers;
import org.apache.iceberg.encryption.UnitestKMS;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Streams;
import org.apache.iceberg.spark.SparkCatalogConfig;
import org.apache.iceberg.spark.SparkTestBaseWithCatalog;
import org.apache.iceberg.types.Types;
import org.apache.parquet.crypto.ParquetCryptoRuntimeException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestTableEncryption extends SparkTestBaseWithCatalog {

  private static Map<String, String> appendCatalogEncryptionConfigProperties(
      Map<String, String> props) {
    Map<String, String> newProps = Maps.newHashMap();
    newProps.putAll(props);
    newProps.put(
        CatalogProperties.ENCRYPTION_KMS_TYPE, CatalogProperties.ENCRYPTION_KMS_CUSTOM_TYPE);
    newProps.put(CatalogProperties.ENCRYPTION_KMS_CLIENT_IMPL, UnitestKMS.class.getCanonicalName());
    return newProps;
  }

  // these parameters are broken out to avoid changes that need to modify lots of test suites
  @Parameterized.Parameters(name = "catalogName = {0}, implementation = {1}, config = {2}")
  public static Object[][] parameters() {
    return new Object[][] {
      {
        SparkCatalogConfig.HIVE.catalogName(),
        SparkCatalogConfig.HIVE.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.HIVE.properties())
      },
      {
        SparkCatalogConfig.HADOOP.catalogName(),
        SparkCatalogConfig.HADOOP.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.HADOOP.properties())
      },
      {
        SparkCatalogConfig.SPARK.catalogName(),
        SparkCatalogConfig.SPARK.implementation(),
        appendCatalogEncryptionConfigProperties(SparkCatalogConfig.SPARK.properties())
      }
    };
  }

  public TestTableEncryption(
      String catalogName, String implementation, Map<String, String> config) {
    super(catalogName, implementation, config);
  }

  @Before
  public void createTables() throws IOException {
    sql(
        "CREATE TABLE %s (id bigint, data string, float float) USING iceberg "
            + "TBLPROPERTIES ( "
            + "'encryption.key-id'='%s' , "
            + "'format-version'='2')",
        tableName, UnitestKMS.MASTER_KEY_NAME1);
    sql("INSERT INTO %s VALUES (1, 'a', 1.0), (2, 'b', 2.0), (3, 'c', float('NaN'))", tableName);
  }

  @After
  public void removeTables() {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testSelect() {
    List<Object[]> expected =
        ImmutableList.of(row(1L, "a", 1.0F), row(2L, "b", 2.0F), row(3L, "c", Float.NaN));

    assertEquals("Should return all expected rows", expected, sql("SELECT * FROM %s", tableName));
  }

  @Test
  public void testDirectDataFileRead() {
    List<Object[]> dataFileTable =
        sql("SELECT file_path FROM %s.%s", tableName, MetadataTableType.ALL_DATA_FILES);
    List<String> dataFiles =
        Streams.concat(dataFileTable.stream())
            .map(row -> (String) row[0])
            .collect(Collectors.toList());
    Schema schema = new Schema(optional(0, "id", Types.IntegerType.get()));
    for (String filePath : dataFiles) {
      AssertHelpers.assertThrows(
          "Read without keys",
          ParquetCryptoRuntimeException.class,
          "Trying to read file with encrypted footer. No keys available",
          () ->
              Parquet.read(localInput(filePath))
                  .project(schema)
                  .callInit()
                  .build()
                  .iterator()
                  .next());
    }
  }

  @Test
  public void testManifestEncryption() throws IOException {
    List<Object[]> manifestFileTable =
        sql("SELECT path FROM %s.%s", tableName, MetadataTableType.MANIFESTS);

    List<String> manifestFiles =
        Streams.concat(manifestFileTable.stream())
            .map(row -> (String) row[0])
            .collect(Collectors.toList());

    if (!(manifestFiles.size() > 0)) {
      throw new RuntimeException("No manifest files found for table " + tableName);
    }

    String metadataFolderPath = null;
    byte[] magic = new byte[4];

    // Check encryption of manifest files
    for (String manifestFilePath : manifestFiles) {
      SeekableInputStream manifestFileReader = localInput(manifestFilePath).newStream();
      manifestFileReader.read(magic);
      manifestFileReader.close();
      Assert.assertArrayEquals(
          magic, Ciphers.GCM_STREAM_MAGIC_STRING.getBytes(StandardCharsets.UTF_8));

      if (metadataFolderPath == null) {
        metadataFolderPath = new File(manifestFilePath).getParent().replaceFirst("file:", "");
      }
    }

    // Find metadata list files and check their encryption
    File[] listOfMetadataFiles = new File(metadataFolderPath).listFiles();
    boolean foundManifestListFile = false;
    for (File metadataFile : listOfMetadataFiles) {
      if (metadataFile.getName().startsWith("snap-")) {
        foundManifestListFile = true;
        SeekableInputStream manifestFileReader = localInput(metadataFile).newStream();
        manifestFileReader.read(magic);
        manifestFileReader.close();
        Assert.assertArrayEquals(
            magic, Ciphers.GCM_STREAM_MAGIC_STRING.getBytes(StandardCharsets.UTF_8));
      }
    }

    if (!foundManifestListFile) {
      throw new RuntimeException("No manifest list files found for table " + tableName);
    }
  }
}
