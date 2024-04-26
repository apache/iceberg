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
package org.apache.iceberg.flink;

import static org.apache.iceberg.CatalogProperties.URI;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.entry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Test for {@link CatalogLoader}. */
public class TestCatalogLoader extends TestBase {

  private static File warehouse = null;
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("default", "my_table");
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "f1", Types.StringType.get()));

  @BeforeAll
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    assertThat(warehouse.delete()).isTrue();
    hiveConf.set("my_key", "my_value");
  }

  @AfterAll
  public static void dropWarehouse() throws IOException {
    if (warehouse != null && warehouse.exists()) {
      Path warehousePath = new Path(warehouse.getAbsolutePath());
      FileSystem fs = warehousePath.getFileSystem(hiveConf);
      assertThat(fs.delete(warehousePath, true)).as("Failed to delete " + warehousePath).isTrue();
    }
  }

  @Test
  public void testHadoopCatalogLoader() throws IOException, ClassNotFoundException {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, "file:" + warehouse);
    CatalogLoader loader = CatalogLoader.hadoop("my_catalog", hiveConf, properties);
    validateCatalogLoader(loader);
  }

  @Test
  public void testHiveCatalogLoader() throws IOException, ClassNotFoundException {
    CatalogLoader loader = CatalogLoader.hive("my_catalog", hiveConf, Maps.newHashMap());
    validateCatalogLoader(loader);
  }

  @Test
  public void testRESTCatalogLoader() {
    Map<String, String> properties = Maps.newHashMap();
    properties.put(URI, "http://localhost/");
    CatalogLoader.rest("my_catalog", hiveConf, Maps.newHashMap());
  }

  private static void validateCatalogLoader(CatalogLoader loader)
      throws IOException, ClassNotFoundException {
    Table table = javaSerAndDeSer(loader).loadCatalog().createTable(IDENTIFIER, SCHEMA);
    validateHadoopConf(table);
  }

  private static void validateHadoopConf(Table table) {
    FileIO io = table.io();
    assertThat(io)
        .as("FileIO should be a HadoopFileIO")
        .isInstanceOf(HadoopFileIO.class);
    HadoopFileIO hadoopIO = (HadoopFileIO) io;
    assertThat(hadoopIO.conf()).contains(entry("my_key", "my_value"));
  }

  @SuppressWarnings("unchecked")
  private static <T> T javaSerAndDeSer(T object) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }

    try (ObjectInputStream in =
        new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
