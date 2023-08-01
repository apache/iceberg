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

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.hadoop.ConfigProperties;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class HiveTableTest extends HiveTableBaseTest {
  static final String NON_DEFAULT_DATABASE = "nondefault";

  @TempDir private Path tempFolder;

  @Test
  public void testCreate() throws TException {
    // Table should be created in hive metastore
    // Table should be renamed in hive metastore
    String tableName = TABLE_IDENTIFIER.name();
    org.apache.hadoop.hive.metastore.api.Table table =
        metastoreClient.getTable(TABLE_IDENTIFIER.namespace().level(0), tableName);

    // check parameters are in expected state
    Map<String, String> parameters = table.getParameters();
    assertThat(parameters).isNotNull();
    assertThat(parameters.get(TABLE_TYPE_PROP)).isEqualToIgnoringCase(ICEBERG_TABLE_TYPE_VALUE);
    assertThat(table.getTableType()).isEqualToIgnoringCase("EXTERNAL_TABLE");

    // Ensure the table is pointing to empty location
    assertThat(table.getSd().getLocation()).isEqualTo(getTableLocation(tableName));

    // Ensure it is stored as unpartitioned table in hive.
    assertThat(table.getPartitionKeysSize()).isEqualTo(0);

    // Only 1 snapshotFile Should exist and no manifests should exist
    assertThat(metadataVersionFiles(tableName)).hasSize(1);
    assertThat(manifestFiles(tableName)).hasSize(0);

    final Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // Iceberg schema should match the loaded table
    assertThat(icebergTable.schema().asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  public void testRename() {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);
    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();
    assertThat(catalog.tableExists(renameTableIdentifier)).isTrue();

    Table renamed = catalog.loadTable(renameTableIdentifier);

    assertThat(renamed.schema().asStruct()).isEqualTo(original.schema().asStruct());
    assertThat(renamed.spec()).isEqualTo(original.spec());
    assertThat(renamed.location()).isEqualTo(original.location());
    assertThat(renamed.currentSnapshot()).isEqualTo(original.currentSnapshot());

    assertThat(catalog.dropTable(renameTableIdentifier)).isTrue();
  }

  @Test
  public void testDrop() {
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).as("Table should exist").isTrue();
    assertThat(catalog.dropTable(TABLE_IDENTIFIER))
        .as("Drop should return true and drop the table")
        .isTrue();
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).as("Table should not exist").isFalse();
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    String fileLocation = appendData(table, "file");

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    assertThat(catalog.dropTable(TABLE_IDENTIFIER, false /* do not delete underlying files */))
        .as("Drop should return true and drop the table")
        .isTrue();
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).as("Table should not exist").isFalse();

    assertThat(new File(fileLocation)).as("Table data files should exist").exists();
    assertThat(new File(manifestListLocation)).as("Table metadata files should exist").exists();
  }

  @Test
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records =
        Lists.newArrayList(
            recordBuilder.set("id", 1L).build(),
            recordBuilder.set("id", 2L).build(),
            recordBuilder.set("id", 3L).build());

    String location1 = table.location().replace("file:", "") + "/data/file1.avro";
    try (FileAppender<GenericData.Record> writer =
        Avro.write(Files.localOutput(location1)).schema(schema).named("test").build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    String location2 = table.location().replace("file:", "") + "/data/file2.avro";
    try (FileAppender<GenericData.Record> writer =
        Avro.write(Files.localOutput(location2)).schema(schema).named("test").build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file1 =
        DataFiles.builder(table.spec())
            .withRecordCount(3)
            .withPath(location1)
            .withFileSizeInBytes(Files.localInput(location2).getLength())
            .build();

    DataFile file2 =
        DataFiles.builder(table.spec())
            .withRecordCount(3)
            .withPath(location2)
            .withFileSizeInBytes(Files.localInput(location1).getLength())
            .build();

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());

    assertThat(catalog.dropTable(TABLE_IDENTIFIER))
        .as("Drop (table and data) should return true and drop the table")
        .isTrue();
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).as("Table should not exist").isFalse();

    assertThat(new File(location1)).as("Table data files should not exist").doesNotExist();
    assertThat(new File(location2)).as("Table data files should not exist").doesNotExist();
    assertThat(new File(manifestListLocation))
        .as("Table manifest list files should not exist")
        .doesNotExist();
    for (ManifestFile manifest : manifests) {
      assertThat(new File(manifest.path().replace("file:", "")))
          .as("Table manifest files should not exist")
          .doesNotExist();
    }
    assertThat(
            new File(
                ((HasTableOperations) table)
                    .operations()
                    .current()
                    .metadataFileLocation()
                    .replace("file:", "")))
        .as("Table metadata file should not exist")
        .doesNotExist();
  }

  @Test
  public void testExistingTableUpdate() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    assertThat(metadataVersionFiles(TABLE_NAME)).hasSize(2);
    assertThat(manifestFiles(TABLE_NAME)).hasSize(0);
    assertThat(icebergTable.schema().asStruct()).isEqualTo(altered.asStruct());

    final org.apache.hadoop.hive.metastore.api.Table table =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);
    final List<String> hiveColumns =
        table.getSd().getCols().stream().map(FieldSchema::getName).collect(Collectors.toList());
    final List<String> icebergColumns =
        altered.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
    assertThat(hiveColumns).isEqualTo(icebergColumns);
  }

  @Test
  public void testColumnTypeChangeInMetastore() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    Schema expectedSchema =
        new Schema(
            Types.StructType.of(
                    required(1, "id", Types.LongType.get()),
                    optional(2, "data", Types.LongType.get()),
                    optional(3, "string", Types.StringType.get()),
                    optional(4, "int", Types.IntegerType.get()))
                .fields());
    // Add columns with different types, then verify we could delete one column in hive metastore
    // as hive conf METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES was set to false. If this was
    // set to true,
    // an InvalidOperationException would thrown in method
    // MetaStoreUtils#throwExceptionIfIncompatibleColTypeChange()
    icebergTable
        .updateSchema()
        .addColumn("data", Types.LongType.get())
        .addColumn("string", Types.StringType.get())
        .addColumn("int", Types.IntegerType.get())
        .commit();

    assertThat(icebergTable.schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema.asStruct());

    expectedSchema =
        new Schema(
            Types.StructType.of(
                    required(1, "id", Types.LongType.get()),
                    optional(2, "data", Types.LongType.get()),
                    optional(4, "int", Types.IntegerType.get()))
                .fields());
    icebergTable.updateSchema().deleteColumn("string").commit();

    assertThat(icebergTable.schema().asStruct())
        .as("Schema should match expected")
        .isEqualTo(expectedSchema.asStruct());
  }

  @Test
  public void testFailure() throws TException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    org.apache.hadoop.hive.metastore.api.Table table =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);
    String dummyLocation = "dummylocation";
    table.getParameters().put(METADATA_LOCATION_PROP, dummyLocation);
    metastoreClient.alter_table(DB_NAME, TABLE_NAME, table);
    assertThatThrownBy(
            () -> icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessageContaining("is not same as the current table metadata location 'dummylocation'");
  }

  @Test
  public void testListTables() throws TException, IOException {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents =
        tableIdents.stream()
            .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
            .collect(Collectors.toList());

    assertThat(expectedIdents).hasSize(1);
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isTrue();

    // create a hive table
    String hiveTableName = "test_hive_table";
    org.apache.hadoop.hive.metastore.api.Table hiveTable = createHiveTable(hiveTableName);
    metastoreClient.createTable(hiveTable);

    catalog.setListAllTables(false);
    List<TableIdentifier> tableIdents1 = catalog.listTables(TABLE_IDENTIFIER.namespace());
    assertThat(tableIdents1).as("should only 1 iceberg table .").hasSize(1);

    catalog.setListAllTables(true);
    List<TableIdentifier> tableIdents2 = catalog.listTables(TABLE_IDENTIFIER.namespace());
    assertThat(tableIdents2).as("should be 2 tables in namespace .").hasSize(2);

    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isTrue();
    metastoreClient.dropTable(DB_NAME, hiveTableName);
  }

  private org.apache.hadoop.hive.metastore.api.Table createHiveTable(String hiveTableName)
      throws IOException {
    Map<String, String> parameters = Maps.newHashMap();
    parameters.put(
        serdeConstants.SERIALIZATION_CLASS, "org.apache.hadoop.hive.serde2.thrift.test.IntString");
    parameters.put(
        serdeConstants.SERIALIZATION_FORMAT, "org.apache.thrift.protocol.TBinaryProtocol");

    SerDeInfo serDeInfo =
        new SerDeInfo(null, "org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer", parameters);

    // StorageDescriptor has an empty list of fields - SerDe will report them.
    StorageDescriptor sd =
        new StorageDescriptor(
            Lists.newArrayList(),
            tempFolder.toAbsolutePath().toString(),
            "org.apache.hadoop.mapred.TextInputFormat",
            "org.apache.hadoop.mapred.TextOutputFormat",
            false,
            -1,
            serDeInfo,
            Lists.newArrayList(),
            Lists.newArrayList(),
            Maps.newHashMap());

    org.apache.hadoop.hive.metastore.api.Table hiveTable =
        new org.apache.hadoop.hive.metastore.api.Table(
            hiveTableName,
            DB_NAME,
            "test_owner",
            0,
            0,
            0,
            sd,
            Lists.newArrayList(),
            Maps.newHashMap(),
            "viewOriginalText",
            "viewExpandedText",
            TableType.EXTERNAL_TABLE.name());
    return hiveTable;
  }

  @Test
  public void testNonDefaultDatabaseLocation() throws IOException, TException {
    Namespace namespace = Namespace.of(NON_DEFAULT_DATABASE);
    // Create a new location and a non-default database / namespace for it
    File nonDefaultLocation =
        createTempDirectory(NON_DEFAULT_DATABASE, asFileAttribute(fromString("rwxrwxrwx")))
            .toFile();
    catalog.createNamespace(
        namespace, Collections.singletonMap("location", nonDefaultLocation.getPath()));
    Map<String, String> namespaceMeta = catalog.loadNamespaceMetadata(namespace);
    // Make sure that we are testing a namespace with a non default location :)
    assertThat("file:" + nonDefaultLocation.getPath()).isEqualTo(namespaceMeta.get("location"));

    TableIdentifier tableIdentifier = TableIdentifier.of(namespace, TABLE_NAME);
    catalog.createTable(tableIdentifier, schema);

    // Let's check the location loaded through the catalog
    Table table = catalog.loadTable(tableIdentifier);
    assertThat(table.location()).isEqualTo(namespaceMeta.get("location") + "/" + TABLE_NAME);

    // Drop the database and purge the files
    metastoreClient.dropDatabase(NON_DEFAULT_DATABASE, true, true, true);
  }

  @Test
  public void testRegisterTable() throws TException {
    org.apache.hadoop.hive.metastore.api.Table originalTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    Map<String, String> originalParams = originalTable.getParameters();
    assertThat(originalParams).isNotNull();
    assertThat(originalParams.get(TABLE_TYPE_PROP)).isEqualToIgnoringCase(ICEBERG_TABLE_TYPE_VALUE);
    assertThat(originalTable.getTableType()).isEqualToIgnoringCase("EXTERNAL_TABLE");

    catalog.dropTable(TABLE_IDENTIFIER, false);
    assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();

    List<String> metadataVersionFiles = metadataVersionFiles(TABLE_NAME);
    assertThat(metadataVersionFiles).hasSize(1);

    catalog.registerTable(TABLE_IDENTIFIER, "file:" + metadataVersionFiles.get(0));

    org.apache.hadoop.hive.metastore.api.Table newTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    Map<String, String> newTableParameters = newTable.getParameters();
    assertThat(newTableParameters)
        .doesNotContainKey(PREVIOUS_METADATA_LOCATION_PROP)
        .containsEntry(TABLE_TYPE_PROP, originalParams.get(TABLE_TYPE_PROP))
        .containsEntry(METADATA_LOCATION_PROP, originalParams.get(METADATA_LOCATION_PROP));
    assertThat(newTable.getSd()).isEqualTo(originalTable.getSd());
  }

  @Test
  public void testRegisterHadoopTableToHiveCatalog() throws IOException, TException {
    // create a hadoop catalog
    String tableLocation = tempFolder.toString();
    HadoopCatalog hadoopCatalog = new HadoopCatalog(new Configuration(), tableLocation);
    // create table using hadoop catalog
    TableIdentifier identifier = TableIdentifier.of(DB_NAME, "table1");
    Table table =
        hadoopCatalog.createTable(
            identifier, schema, PartitionSpec.unpartitioned(), Maps.newHashMap());
    // insert some data
    String file1Location = appendData(table, "file1");
    List<FileScanTask> tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should scan 1 file").hasSize(1);
    assertThat(file1Location).isEqualTo(tasks.get(0).file().path());

    // collect metadata file
    List<String> metadataFiles =
        Arrays.stream(new File(table.location() + "/metadata").listFiles())
            .map(File::getAbsolutePath)
            .filter(f -> f.endsWith(getFileExtension(TableMetadataParser.Codec.NONE)))
            .collect(Collectors.toList());
    assertThat(metadataFiles).hasSize(2);

    assertThatThrownBy(() -> metastoreClient.getTable(DB_NAME, "table1"))
        .isInstanceOf(NoSuchObjectException.class)
        .hasMessage("hivedb.table1 table not found");
    assertThatThrownBy(() -> catalog.loadTable(identifier))
        .isInstanceOf(NoSuchTableException.class)
        .hasMessage("Table does not exist: hivedb.table1");

    // register the table to hive catalog using the latest metadata file
    String latestMetadataFile = ((BaseTable) table).operations().current().metadataFileLocation();
    catalog.registerTable(identifier, "file:" + latestMetadataFile);
    assertThat(metastoreClient.getTable(DB_NAME, "table1")).isNotNull();

    // load the table in hive catalog
    table = catalog.loadTable(identifier);
    assertThat(table).isNotNull();

    // insert some data
    String file2Location = appendData(table, "file2");
    tasks = Lists.newArrayList(table.newScan().planFiles());
    assertThat(tasks).as("Should scan 2 files").hasSize(2);
    Set<String> files =
        tasks.stream().map(task -> task.file().path().toString()).collect(Collectors.toSet());
    assertThat(files).contains(file1Location, file2Location);
  }

  private String appendData(Table table, String fileName) throws IOException {
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records =
        Lists.newArrayList(
            recordBuilder.set("id", 1L).build(),
            recordBuilder.set("id", 2L).build(),
            recordBuilder.set("id", 3L).build());

    String fileLocation = table.location().replace("file:", "") + "/data/" + fileName + ".avro";
    try (FileAppender<GenericData.Record> writer =
        Avro.write(Files.localOutput(fileLocation)).schema(schema).named("test").build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }

    DataFile file =
        DataFiles.builder(table.spec())
            .withRecordCount(3)
            .withPath(fileLocation)
            .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
            .build();

    table.newAppend().appendFile(file).commit();

    return fileLocation;
  }

  @Test
  public void testRegisterExistingTable() throws TException {
    org.apache.hadoop.hive.metastore.api.Table originalTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    Map<String, String> originalParams = originalTable.getParameters();
    assertThat(originalParams).isNotNull();
    assertThat(originalParams.get(TABLE_TYPE_PROP)).isEqualToIgnoringCase(ICEBERG_TABLE_TYPE_VALUE);
    assertThat(originalTable.getTableType()).isEqualToIgnoringCase("EXTERNAL_TABLE");

    List<String> metadataVersionFiles = metadataVersionFiles(TABLE_NAME);
    assertThat(metadataVersionFiles).hasSize(1);

    // Try to register an existing table
    assertThatThrownBy(
            () -> catalog.registerTable(TABLE_IDENTIFIER, "file:" + metadataVersionFiles.get(0)))
        .isInstanceOf(AlreadyExistsException.class)
        .hasMessage("Table already exists: hivedb.tbl");
  }

  @Test
  public void testEngineHiveEnabledDefault() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Unset in hive-conf
    catalog.getConf().unset(ConfigProperties.ENGINE_HIVE_ENABLED);

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  @Test
  public void testEngineHiveEnabledConfig() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Enable by hive-conf
    catalog.getConf().set(ConfigProperties.ENGINE_HIVE_ENABLED, "true");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, true);

    catalog.dropTable(TABLE_IDENTIFIER);

    // Disable by hive-conf
    catalog.getConf().set(ConfigProperties.ENGINE_HIVE_ENABLED, "false");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned());
    hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  @Test
  public void testEngineHiveEnabledTableProperty() throws TException {
    // Drop the previously created table to make place for the new one
    catalog.dropTable(TABLE_IDENTIFIER);

    // Enabled by table property - also check that the hive-conf is ignored
    Map<String, String> tableProperties = Maps.newHashMap();
    tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "true");
    catalog.getConf().set(ConfigProperties.ENGINE_HIVE_ENABLED, "false");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned(), tableProperties);
    org.apache.hadoop.hive.metastore.api.Table hmsTable =
        metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, true);

    catalog.dropTable(TABLE_IDENTIFIER);

    // Disabled by table property - also check that the hive-conf is ignored
    tableProperties.put(TableProperties.ENGINE_HIVE_ENABLED, "false");
    catalog.getConf().set(ConfigProperties.ENGINE_HIVE_ENABLED, "true");

    catalog.createTable(TABLE_IDENTIFIER, schema, PartitionSpec.unpartitioned(), tableProperties);
    hmsTable = metastoreClient.getTable(DB_NAME, TABLE_NAME);

    assertHiveEnabled(hmsTable, false);
  }

  @Test
  public void testMissingMetadataWontCauseHang() {
    catalog.loadTable(TABLE_IDENTIFIER);

    File realLocation = new File(metadataLocation(TABLE_NAME));
    File fakeLocation = new File(metadataLocation(TABLE_NAME) + "_dummy");

    assertThat(realLocation.renameTo(fakeLocation)).isTrue();
    assertThatThrownBy(() -> catalog.loadTable(TABLE_IDENTIFIER))
        .isInstanceOf(NotFoundException.class)
        .hasMessageStartingWith("Failed to open input stream for file");
    assertThat(fakeLocation.renameTo(realLocation)).isTrue();
  }

  private void assertHiveEnabled(
      org.apache.hadoop.hive.metastore.api.Table hmsTable, boolean expected) {
    if (expected) {
      assertThat(hmsTable.getParameters())
          .containsEntry(
              hive_metastoreConstants.META_TABLE_STORAGE,
              "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
      assertThat(hmsTable.getSd().getSerdeInfo().getSerializationLib())
          .isEqualTo("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
      assertThat(hmsTable.getSd().getInputFormat())
          .isEqualTo("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
      assertThat(hmsTable.getSd().getOutputFormat())
          .isEqualTo("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    } else {
      assertThat(hmsTable.getParameters())
          .doesNotContainKey(hive_metastoreConstants.META_TABLE_STORAGE);
      assertThat(hmsTable.getSd().getSerdeInfo().getSerializationLib())
          .isEqualTo("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
      assertThat(hmsTable.getSd().getInputFormat())
          .isEqualTo("org.apache.hadoop.mapred.FileInputFormat");
      assertThat(hmsTable.getSd().getOutputFormat())
          .isEqualTo("org.apache.hadoop.mapred.FileOutputFormat");
    }
  }
}
