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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for {@link CatalogLoader} and {@link TableLoader}.
 */
public class TestCatalogTableLoader extends FlinkTestBase {

  private static File warehouse = null;
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("default", "my_table");
  private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "f1", Types.StringType.get()));

  @BeforeClass
  public static void createWarehouse() throws IOException {
    warehouse = File.createTempFile("warehouse", null);
    Assert.assertTrue(warehouse.delete());
    hiveConf.set("my_key", "my_value");
  }

  @AfterClass
  public static void dropWarehouse() {
    if (warehouse != null && warehouse.exists()) {
      warehouse.delete();
    }
  }

  @Test
  public void testHadoopCatalogLoader() throws IOException, ClassNotFoundException {
    CatalogLoader loader = CatalogLoader.hadoop("my_catalog", hiveConf, "file:" + warehouse);
    validateCatalogLoader(loader);
  }

  @Test
  public void testHiveCatalogLoader() throws IOException, ClassNotFoundException {
    CatalogLoader loader = CatalogLoader.hive("my_catalog", hiveConf, null, null, 2);
    validateCatalogLoader(loader);
  }

  @Test
  public void testHadoopTableLoader() throws IOException, ClassNotFoundException {
    String location = "file:" + warehouse + "/my_table";
    new HadoopTables(hiveConf).create(SCHEMA, location);
    validateTableLoader(TableLoader.fromHadoopTable(location, hiveConf));
  }

  @Test
  public void testHiveCatalogTableLoader() throws IOException, ClassNotFoundException {
    CatalogLoader catalogLoader = CatalogLoader.hive("my_catalog", hiveConf, null, null, 2);
    validateTableLoader(TableLoader.fromCatalog(catalogLoader, IDENTIFIER));
  }

  private static void validateCatalogLoader(CatalogLoader loader) throws IOException, ClassNotFoundException {
    Table table = javaSerAndDeSer(loader).loadCatalog().createTable(IDENTIFIER, SCHEMA);
    validateHadoopConf(table);
  }

  private static void validateTableLoader(TableLoader loader) throws IOException, ClassNotFoundException {
    TableLoader copied = javaSerAndDeSer(loader);
    copied.open();
    try {
      validateHadoopConf(copied.loadTable());
    } finally {
      copied.close();
    }
  }

  private static void validateHadoopConf(Table table) {
    FileIO io = table.io();
    Assert.assertTrue("FileIO should be a HadoopFileIO", io instanceof HadoopFileIO);
    HadoopFileIO hadoopIO = (HadoopFileIO) io;
    Assert.assertEquals("my_value", hadoopIO.conf().get("my_key"));
  }

  @SuppressWarnings("unchecked")
  private static <T> T javaSerAndDeSer(T object) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(object);
    }

    try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }
}
