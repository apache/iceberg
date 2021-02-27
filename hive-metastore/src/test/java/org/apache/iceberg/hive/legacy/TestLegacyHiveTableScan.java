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

package org.apache.iceberg.hive.legacy;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeUtils;
import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.HiveMetastoreTest;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Type;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.ORC;


public class TestLegacyHiveTableScan extends HiveMetastoreTest {
  private static final List<FieldSchema> DATA_COLUMNS = ImmutableList.of(
      new FieldSchema("strCol", "string", ""),
      new FieldSchema("intCol", "int", ""));
  private static final List<FieldSchema> PARTITION_COLUMNS = ImmutableList.of(
      new FieldSchema("pcol", "string", ""),
      new FieldSchema("pIntCol", "int", ""));

  private static HiveCatalog legacyCatalog;
  private static Path dbPath;

  @BeforeClass
  public static void beforeClass() throws Exception {
    legacyCatalog = new LegacyHiveCatalog(HiveMetastoreTest.hiveConf);
    dbPath = Paths.get(URI.create(metastoreClient.getDatabase(DB_NAME).getLocationUri()));
  }

  @AfterClass
  public static void afterClass() {
    legacyCatalog.close();
    TestLegacyHiveTableScan.legacyCatalog = null;
  }

  @Test
  public void testHiveScanUnpartitioned() throws Exception {
    String tableName = "unpartitioned";
    Table table = createTable(tableName, DATA_COLUMNS, ImmutableList.of());
    addFiles(table, AVRO, "A", "B");
    filesMatch(ImmutableMap.of("A", AVRO, "B", AVRO), hiveScan(table));
  }

  @Test
  public void testHiveScanSinglePartition() throws Exception {
    String tableName = "single_partition";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    addPartition(table, ImmutableList.of("ds", 1), AVRO, "A", "B");
    filesMatch(ImmutableMap.of("pcol=ds/pIntCol=1/B", AVRO, "pcol=ds/pIntCol=1/A", AVRO), hiveScan(table));
  }

  @Test
  public void testHiveScanMultiPartition() throws Exception {
    String tableName = "multi_partition";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    addPartition(table, ImmutableList.of("ds", 1), AVRO, "A");
    addPartition(table, ImmutableList.of("ds", 2), AVRO, "B");
    filesMatch(ImmutableMap.of("pcol=ds/pIntCol=2/B", AVRO, "pcol=ds/pIntCol=1/A", AVRO), hiveScan(table));
  }

  @Test
  public void testHiveScanNoAvroSchema() throws Exception {
    String tableName = "hive_scan_no_avro_schema";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS, ORC);
    addPartition(table, ImmutableList.of("ds", 1), ORC, "A");
    addPartition(table, ImmutableList.of("ds", 2), ORC, "B");
    filesMatch(ImmutableMap.of("pcol=ds/pIntCol=2/B", ORC, "pcol=ds/pIntCol=1/A", ORC), hiveScan(table));
  }

  @Test
  public void testHiveScanMultiPartitionWithFilter() throws Exception {
    String tableName = "multi_partition_with_filter";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    addPartition(table, ImmutableList.of("ds", 1), AVRO, "A");
    addPartition(table, ImmutableList.of("ds", 2), AVRO, "B");
    filesMatch(
        ImmutableMap.of("pcol=ds/pIntCol=1/A", AVRO, "pcol=ds/pIntCol=2/B", AVRO),
        hiveScan(table, Expressions.equal("pcol", "ds")));
  }

  @Test
  public void testHiveScanNonStringPartitionQuery() throws Exception {
    String tableName = "multi_partition_with_filter_on_non_string_partition_cols";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    AssertHelpers.assertThrows(
        "Filtering on non string partition is not supported by ORM layer and we can enable direct sql only on mysql",
        RuntimeException.class, "Failed to get partition info",
        () -> hiveScan(table, Expressions.and(Expressions.equal("pcol", "ds"), Expressions.equal("pIntCol", "1"))));
  }

  @Test
  public void testHiveScanMultiPartitionWithNonPartitionFilter() throws Exception {
    String tableName = "multi_partition_with_non_partition_filter";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    addPartition(table, ImmutableList.of("ds", 1), AVRO, "A");
    addPartition(table, ImmutableList.of("ds", 2), AVRO, "B");
    filesMatch(
        ImmutableMap.of("pcol=ds/pIntCol=1/A", AVRO, "pcol=ds/pIntCol=2/B", AVRO),
        hiveScan(table, Expressions.equal("intCol", 1)));
  }

  @Test
  public void testHiveScanHybridTable() throws Exception {
    String tableName = "hybrid_table";
    Table table = createTable(tableName, DATA_COLUMNS, PARTITION_COLUMNS);
    addPartition(table, ImmutableList.of("ds", 1), AVRO, "A");
    addPartition(table, ImmutableList.of("ds", 2), ORC, "B");
    filesMatch(ImmutableMap.of("pcol=ds/pIntCol=1/A", AVRO, "pcol=ds/pIntCol=2/B", ORC), hiveScan(table));
  }

  private static Table createTable(String tableName, List<FieldSchema> columns, List<FieldSchema> partitionColumns)
      throws Exception {
    return createTable(tableName, columns, partitionColumns, AVRO);
  }

  private static Table createTable(
      String tableName, List<FieldSchema> columns, List<FieldSchema> partitionColumns, FileFormat format)
      throws Exception {
    long currentTimeMillis = System.currentTimeMillis();
    Path tableLocation = dbPath.resolve(tableName);
    Files.createDirectories(tableLocation);
    Table tbl = new Table(tableName,
        DB_NAME,
        System.getProperty("user.name"),
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        Integer.MAX_VALUE,
        storageDescriptor(columns, tableLocation.toString(), format),
        partitionColumns,
        new HashMap<>(),
        null,
        null,
        TableType.EXTERNAL_TABLE.toString());
    tbl.getParameters().put("EXTERNAL", "TRUE");
    metastoreClient.createTable(tbl);
    return tbl;
  }

  private static StorageDescriptor storageDescriptor(List<FieldSchema> columns, String location, FileFormat format) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns);
    storageDescriptor.setLocation(location);
    SerDeInfo serDeInfo = new SerDeInfo();
    switch (format) {
      case AVRO:
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat");
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat");
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.serde2.avro.AvroSerDe");
        storageDescriptor.setParameters(ImmutableMap.of(
            AvroSerdeUtils.AvroTableProperties.SCHEMA_LITERAL.getPropName(), schemaLiteral(columns)));
        break;
      case ORC:
        storageDescriptor.setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat");
        storageDescriptor.setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat");
        serDeInfo.setSerializationLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde");
        break;
      default:
        throw new UnsupportedOperationException("Unsupported file format: " + format);
    }
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private static String schemaLiteral(List<FieldSchema> columns) {
    Type icebergType = HiveTypeUtil.convert(LegacyHiveTableUtils.structTypeInfoFromCols(columns));
    return AvroSchemaUtil.convert(icebergType).toString();
  }

  private static Path location(Table table) {
    return Paths.get(table.getSd().getLocation());
  }

  private static Path location(Table table, List<Object> partitionValues) {
    Path partitionLocation = location(table);
    for (int i = 0; i < table.getPartitionKeysSize(); i++) {
      partitionLocation = partitionLocation.resolve(
          table.getPartitionKeys().get(i).getName() + "=" + partitionValues.get(i));
    }
    return partitionLocation;
  }

  private void addFiles(Table table, FileFormat format, String... fileNames) throws IOException {
    Path tableLocation = location(table);
    for (String fileName : fileNames) {
      Path filePath = tableLocation.resolve(format.addExtension(fileName));
      Files.createFile(filePath);
    }
  }

  private void addPartition(Table table, List<Object> partitionValues, FileFormat format, String... fileNames)
      throws Exception {
    Path partitionLocation = location(table, partitionValues);
    Files.createDirectories(partitionLocation);
    long currentTimeMillis = System.currentTimeMillis();
    metastoreClient.add_partition(new Partition(
        Lists.transform(partitionValues, Object::toString),
        table.getDbName(),
        table.getTableName(),
        (int) currentTimeMillis / 1000,
        (int) currentTimeMillis / 1000,
        storageDescriptor(table.getSd().getCols(), partitionLocation.toString(), format),
        new HashMap<>()
    ));
    for (String fileName : fileNames) {
      Path filePath = partitionLocation.resolve(format.addExtension(fileName));
      Files.createFile(filePath);
    }
  }

  private Map<String, FileFormat> hiveScan(Table table) {
    return hiveScan(table, Expressions.alwaysTrue());
  }

  private Map<String, FileFormat> hiveScan(Table table, Expression filter) {
    Path tableLocation = location(table);
    CloseableIterable<FileScanTask> fileScanTasks = legacyCatalog
        .loadTable(TableIdentifier.of(table.getDbName(), table.getTableName()))
        .newScan().filter(filter).planFiles();
    return StreamSupport
        .stream(fileScanTasks.spliterator(), false)
        .collect(Collectors.toMap(
            f -> tableLocation.relativize(Paths.get(URI.create(f.file().path().toString()))).toString().split("\\.")[0],
            f -> f.file().format()));
  }

  private static void filesMatch(Map<String, FileFormat> expected, Map<String, FileFormat> actual) {
    Assert.assertEquals(expected, actual);
  }
}
