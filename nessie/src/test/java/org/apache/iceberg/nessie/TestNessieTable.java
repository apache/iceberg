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

package org.apache.iceberg.nessie;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Files;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.api.params.CommitLogParams;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentsKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.ImmutablePut;

import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

public class TestNessieTable extends BaseTestIceberg {

  private static final String BRANCH = "iceberg-table-test";

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final ContentsKey KEY = ContentsKey.of(DB_NAME, TABLE_NAME);
  private static final Schema schema = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get())).fields());
  private static final Schema altered = new Schema(Types.StructType.of(
      required(1, "id", Types.LongType.get()),
      optional(2, "data", Types.LongType.get())).fields());

  private Path tableLocation;

  public TestNessieTable() {
    super(BRANCH);
  }

  @BeforeEach
  public void beforeEach() throws IOException {
    super.beforeEach();
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());
  }

  @AfterEach
  public void afterEach() throws Exception {
    // drop the table data
    if (tableLocation != null) {
      tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
      catalog.refresh();
      catalog.dropTable(TABLE_IDENTIFIER, false);
    }

    super.afterEach();
  }

  private org.projectnessie.model.IcebergTable getTable(ContentsKey key) throws NessieNotFoundException {
    return client.getContentsApi()
        .getContents(key, BRANCH, null)
        .unwrap(IcebergTable.class).get();
  }

  @Test
  public void testCreate() throws NessieNotFoundException, IOException {
    // Table should be created in iceberg
    // Table should be renamed in iceberg
    String tableName = TABLE_IDENTIFIER.name();
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    IcebergTable table = getTable(KEY);
    // check parameters are in expected state
    String expected = (temp.toUri() + DB_NAME + "/" + tableName).replace("///", "/");
    Assertions.assertThat(getTableLocation(tableName)).isEqualTo(expected);

    // Only 1 snapshotFile Should exist and no manifests should exist
    Assertions.assertThat(metadataVersionFiles(tableName)).isNotNull().hasSize(2);
    Assertions.assertThat(manifestFiles(tableName)).isNotNull().isEmpty();

    verifyCommitMetadata();
  }

  @Test
  public void testRename() throws NessieNotFoundException {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier = TableIdentifier.of(
        TABLE_IDENTIFIER.namespace(),
        renamedTableName);

    Table original = catalog.loadTable(TABLE_IDENTIFIER);

    catalog.renameTable(TABLE_IDENTIFIER, renameTableIdentifier);
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();
    Assertions.assertThat(catalog.tableExists(renameTableIdentifier)).isTrue();

    Table renamed = catalog.loadTable(renameTableIdentifier);

    Assertions.assertThat(original.schema().asStruct()).isEqualTo(renamed.schema().asStruct());
    Assertions.assertThat(original.spec()).isEqualTo(renamed.spec());
    Assertions.assertThat(original.location()).isEqualTo(renamed.location());
    Assertions.assertThat(original.currentSnapshot()).isEqualTo(renamed.currentSnapshot());

    Assertions.assertThat(catalog.dropTable(renameTableIdentifier)).isTrue();

    verifyCommitMetadata();
  }

  private void verifyCommitMetadata() throws NessieNotFoundException {
    // check that the author is properly set
    List<CommitMeta> log = tree.getCommitLog(BRANCH, CommitLogParams.empty()).getOperations();
    Assertions.assertThat(log).isNotNull().isNotEmpty();
    log.forEach(x -> {
      Assertions.assertThat(x.getAuthor()).isNotNull().isNotEmpty();
      Assertions.assertThat(System.getProperty("user.name")).isEqualTo(x.getAuthor());
      Assertions.assertThat("iceberg").isEqualTo(x.getProperties().get(NessieUtil.APPLICATION_TYPE));
    });
  }

  @Test
  public void testDrop() throws NessieNotFoundException {
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.dropTable(TABLE_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();
    verifyCommitMetadata();
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    String fileLocation = addRecordsToFile(table, "file");

    DataFile file = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(fileLocation)
        .withFileSizeInBytes(Files.localInput(fileLocation).getLength())
        .build();

    table.newAppend().appendFile(file).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    Assertions.assertThat(catalog.dropTable(TABLE_IDENTIFIER, false)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();

    Assertions.assertThat(new File(fileLocation)).exists();
    Assertions.assertThat(new File(manifestListLocation)).exists();
  }

  @Test
  public void testDropTable() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String location1 = addRecordsToFile(table, "file1");
    String location2 = addRecordsToFile(table, "file2");

    DataFile file1 = DataFiles.builder(table.spec())
        .withRecordCount(3)
        .withPath(location1)
        .withFileSizeInBytes(Files.localInput(location2).getLength())
        .build();

    DataFile file2 = DataFiles.builder(table.spec())
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

    List<ManifestFile> manifests = table.currentSnapshot().allManifests();

    Assertions.assertThat(catalog.dropTable(TABLE_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();

    Assertions.assertThat(new File(location1)).exists();
    Assertions.assertThat(new File(location2)).exists();
    Assertions.assertThat(new File(manifestListLocation)).exists();
    for (ManifestFile manifest : manifests) {
      Assertions.assertThat(new File(manifest.path().replace("file:", ""))).exists();
    }
    Assertions.assertThat(new File(
        ((HasTableOperations) table).operations()
            .current()
            .metadataFileLocation()
            .replace("file:", "")))
        .exists();

    verifyCommitMetadata();
  }

  @Test
  public void testExistingTableUpdate() {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit();

    icebergTable = catalog.loadTable(TABLE_IDENTIFIER);

    // Only 2 snapshotFile Should exist and no manifests should exist
    Assertions.assertThat(metadataVersionFiles(TABLE_NAME)).isNotNull().hasSize(2);
    Assertions.assertThat(manifestFiles(TABLE_NAME)).isNotNull().isEmpty();
    Assertions.assertThat(altered.asStruct()).isEqualTo(icebergTable.schema().asStruct());
  }

  @Test
  public void testFailure() throws NessieNotFoundException, NessieConflictException {
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    Branch branch = (Branch) client.getTreeApi().getReferenceByName(BRANCH);

    IcebergTable table = client.getContentsApi().getContents(KEY, BRANCH, null).unwrap(IcebergTable.class).get();

    client.getTreeApi().commitMultipleOperations(branch.getName(), branch.getHash(),
        ImmutableOperations.builder().addOperations(
            ImmutablePut.builder().key(KEY).contents(IcebergTable.of("dummytable.metadata.json"))
                .build()).commitMeta(CommitMeta.fromMessage("")).build());

    Assertions.assertThatThrownBy(() -> icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "Commit failed: Reference hash is out of date. Update the reference iceberg-table-test and try again");
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents = tableIdents.stream()
        .filter(t -> t.namespace()
            .level(0)
            .equals(DB_NAME) &&
            t.name().equals(TABLE_NAME))
        .collect(Collectors.toList());

    Assertions.assertThat(expectedIdents).hasSize(1);
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isTrue();
  }

  private String getTableBasePath(String tableName) {
    String databasePath = temp.toString() + "/" + DB_NAME;
    return Paths.get(databasePath, tableName).toAbsolutePath().toString();
  }

  protected Path getTableLocationPath(String tableName) {
    return new Path("file", null, Paths.get(getTableBasePath(tableName)).toString());
  }

  protected String getTableLocation(String tableName) {
    return getTableLocationPath(tableName).toString();
  }

  private String metadataLocation(String tableName) {
    return Paths.get(getTableBasePath(tableName), "metadata").toString();
  }

  private List<String> metadataFiles(String tableName) {
    return Arrays.stream(new File(metadataLocation(tableName)).listFiles())
        .map(File::getAbsolutePath)
        .collect(Collectors.toList());
  }

  protected List<String> metadataVersionFiles(String tableName) {
    return filterByExtension(tableName, getFileExtension(TableMetadataParser.Codec.NONE));
  }

  protected List<String> manifestFiles(String tableName) {
    return filterByExtension(tableName, ".avro");
  }

  private List<String> filterByExtension(String tableName, String extension) {
    return metadataFiles(tableName)
        .stream()
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }

  private static String addRecordsToFile(Table table, String filename) throws IOException {
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = new ArrayList<>();
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    String fileLocation = table.location().replace("file:", "") +
        String.format("/data/%s.avro", filename);
    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(fileLocation))
        .schema(schema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : records) {
        writer.add(rec);
      }
    }
    return fileLocation;
  }
}
