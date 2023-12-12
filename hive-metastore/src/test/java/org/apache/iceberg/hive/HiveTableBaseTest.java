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
package org.apache.iceberg.hive;

import static org.apache.iceberg.PartitionSpec.builderFor;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;

public class HiveTableBaseTest {

  static final String TABLE_NAME = "tbl";
  static final String DB_NAME = "hivedb";
  static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);

  @RegisterExtension
  protected static final HiveMetastoreExtension HIVE_METASTORE_EXTENSION =
      new HiveMetastoreExtension(DB_NAME, Collections.emptyMap());

  protected HiveCatalog catalog;

  static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

  static final Schema altered =
      new Schema(
          Types.StructType.of(
                  required(1, "id", Types.LongType.get()),
                  optional(2, "data", Types.LongType.get()))
              .fields());

  private static final PartitionSpec partitionSpec = builderFor(schema).identity("id").build();

  @BeforeEach
  public void createTestTable() {
    catalog =
        (HiveCatalog)
            CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(),
                CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
                ImmutableMap.of(
                    CatalogProperties.CLIENT_POOL_CACHE_EVICTION_INTERVAL_MS,
                    String.valueOf(TimeUnit.SECONDS.toMillis(10))),
                HIVE_METASTORE_EXTENSION.hiveConf());
    catalog.createTable(TABLE_IDENTIFIER, schema, partitionSpec);
  }

  private static String getTableBasePath(String tableName) {
    String databasePath = HIVE_METASTORE_EXTENSION.metastore().getDatabasePath(DB_NAME);
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  protected static Path getTableLocationPath(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString());
  }

  protected static String getTableLocation(String tableName) {
    return getTableLocationPath(tableName).toString();
  }

  protected static String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private static List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());
  }

  protected static List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
  }

  protected static List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private static List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName).stream()
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }
}
