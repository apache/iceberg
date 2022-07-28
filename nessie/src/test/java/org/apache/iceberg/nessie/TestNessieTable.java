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

import static org.apache.iceberg.TableMetadataParser.getFileExtension;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.jaxrs.ext.NessieUri;
import org.projectnessie.model.Branch;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.ImmutableTableReference;
import org.projectnessie.model.LogResponse.LogEntry;
import org.projectnessie.model.Operation;

public class TestNessieTable extends BaseTestIceberg {

  private static final String BRANCH = "iceberg-table-test";

  private static final String DB_NAME = "db";
  private static final String TABLE_NAME = "tbl";
  private static final TableIdentifier TABLE_IDENTIFIER = TableIdentifier.of(DB_NAME, TABLE_NAME);
  private static final ContentKey KEY = ContentKey.of(DB_NAME, TABLE_NAME);
  private static final Schema schema =
      new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
  private static final Schema altered =
      new Schema(
          Types.StructType.of(
                  required(1, "id", Types.LongType.get()),
                  optional(2, "data", Types.LongType.get()))
              .fields());

  private Path tableLocation;

  public TestNessieTable() {
    super(BRANCH);
  }

  @Override
  @BeforeEach
  public void beforeEach(@NessieUri URI uri) throws IOException {
    super.beforeEach(uri);
    this.tableLocation = new Path(catalog.createTable(TABLE_IDENTIFIER, schema).location());
  }

  @Override
  @AfterEach
  public void afterEach() throws Exception {
    // drop the table data
    if (tableLocation != null) {
      tableLocation.getFileSystem(hadoopConfig).delete(tableLocation, true);
      catalog.dropTable(TABLE_IDENTIFIER, false);
    }

    super.afterEach();
  }

  private IcebergTable getTable(ContentKey key) throws NessieNotFoundException {
    return getTable(BRANCH, key);
  }

  private IcebergTable getTable(String ref, ContentKey key) throws NessieNotFoundException {
    return api.getContent().key(key).refName(ref).get().get(key).unwrap(IcebergTable.class).get();
  }

  /** Verify that Nessie always returns the globally-current global-content w/ only DMLs. */
  @Test
  public void verifyStateMovesForDML() throws Exception {
    //  1. initialize table
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    icebergTable.updateSchema().addColumn("initial_column", Types.LongType.get()).commit();

    //  2. create 2nd branch
    String testCaseBranch = "verify-global-moving";
    api.createReference()
        .sourceRefName(BRANCH)
        .reference(Branch.of(testCaseBranch, catalog.currentHash()))
        .create();
    try (NessieCatalog ignore = initCatalog(testCaseBranch)) {

      IcebergTable contentInitialMain = getTable(BRANCH, KEY);
      IcebergTable contentInitialBranch = getTable(testCaseBranch, KEY);
      Table tableInitialMain = catalog.loadTable(TABLE_IDENTIFIER);

      // verify table-metadata-location + snapshot-id
      Assertions.assertThat(contentInitialMain)
          .as("global-contents + snapshot-id equal on both branches in Nessie")
          .isEqualTo(contentInitialBranch);
      Assertions.assertThat(tableInitialMain.currentSnapshot()).isNull();

      //  3. modify table in "main" branch (add some data)

      DataFile file1 = makeDataFile(icebergTable, addRecordsToFile(icebergTable, "file1"));
      icebergTable.newAppend().appendFile(file1).commit();

      IcebergTable contentsAfter1Main = getTable(KEY);
      IcebergTable contentsAfter1Branch = getTable(testCaseBranch, KEY);
      Table tableAfter1Main = catalog.loadTable(TABLE_IDENTIFIER);

      //  --> assert getValue() against both branches returns the updated metadata-location
      // verify table-metadata-location
      Assertions.assertThat(contentInitialMain.getMetadataLocation())
          .describedAs("metadata-location must change on %s", BRANCH)
          .isNotEqualTo(contentsAfter1Main.getMetadataLocation());
      Assertions.assertThat(contentInitialBranch.getMetadataLocation())
          .describedAs("metadata-location must not change on %s", testCaseBranch)
          .isEqualTo(contentsAfter1Branch.getMetadataLocation());
      Assertions.assertThat(contentsAfter1Main)
          .extracting(IcebergTable::getSchemaId)
          .describedAs("on-reference-state must not be equal on both branches")
          .isEqualTo(contentsAfter1Branch.getSchemaId());
      // verify manifests
      Assertions.assertThat(tableAfter1Main.currentSnapshot().allManifests(tableAfter1Main.io()))
          .describedAs("verify number of manifests on 'main'")
          .hasSize(1);

      //  4. modify table in "main" branch (add some data) again

      DataFile file2 = makeDataFile(icebergTable, addRecordsToFile(icebergTable, "file2"));
      icebergTable.newAppend().appendFile(file2).commit();

      IcebergTable contentsAfter2Main = getTable(KEY);
      IcebergTable contentsAfter2Branch = getTable(testCaseBranch, KEY);
      Table tableAfter2Main = catalog.loadTable(TABLE_IDENTIFIER);

      //  --> assert getValue() against both branches returns the updated metadata-location
      // verify table-metadata-location
      Assertions.assertThat(contentsAfter2Main.getMetadataLocation())
          .describedAs("metadata-location must change on %s", BRANCH)
          .isNotEqualTo(contentsAfter1Main.getMetadataLocation());
      Assertions.assertThat(contentsAfter2Branch.getMetadataLocation())
          .describedAs("on-reference-state must not change on %s", testCaseBranch)
          .isEqualTo(contentsAfter1Branch.getMetadataLocation());
      // verify manifests
      Assertions.assertThat(tableAfter2Main.currentSnapshot().allManifests(tableAfter2Main.io()))
          .describedAs("verify number of manifests on 'main'")
          .hasSize(2);
    }
  }

  @Test
  public void testCreate() throws IOException {
    // Table should be created in iceberg
    // Table should be renamed in iceberg
    String tableName = TABLE_IDENTIFIER.name();
    Table icebergTable = catalog.loadTable(TABLE_IDENTIFIER);
    // add a column
    icebergTable.updateSchema().addColumn("mother", Types.LongType.get()).commit();
    getTable(KEY); // sanity, check table exists
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
    TableIdentifier renameTableIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);

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

  @Test
  public void testRenameWithTableReference() throws NessieNotFoundException {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);

    ImmutableTableReference fromTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(TABLE_IDENTIFIER.name())
            .build();
    ImmutableTableReference toTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(renameTableIdentifier.name())
            .build();
    TableIdentifier fromIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), toTableReference.toString());

    Table original = catalog.loadTable(fromIdentifier);

    catalog.renameTable(fromIdentifier, toIdentifier);
    Assertions.assertThat(catalog.tableExists(fromIdentifier)).isFalse();
    Assertions.assertThat(catalog.tableExists(toIdentifier)).isTrue();

    Table renamed = catalog.loadTable(toIdentifier);

    Assertions.assertThat(original.schema().asStruct()).isEqualTo(renamed.schema().asStruct());
    Assertions.assertThat(original.spec()).isEqualTo(renamed.spec());
    Assertions.assertThat(original.location()).isEqualTo(renamed.location());
    Assertions.assertThat(original.currentSnapshot()).isEqualTo(renamed.currentSnapshot());

    Assertions.assertThat(catalog.dropTable(toIdentifier)).isTrue();

    verifyCommitMetadata();
  }

  @Test
  public void testRenameWithTableReferenceInvalidCase() throws NessieNotFoundException {
    String renamedTableName = "rename_table_name";
    TableIdentifier renameTableIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), renamedTableName);

    ImmutableTableReference fromTableReference =
        ImmutableTableReference.builder()
            .reference("Something")
            .name(TABLE_IDENTIFIER.name())
            .build();
    ImmutableTableReference toTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(renameTableIdentifier.name())
            .build();
    TableIdentifier fromIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), toTableReference.toString());

    Assertions.assertThatThrownBy(() -> catalog.renameTable(fromIdentifier, toIdentifier))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("from: Something and to: iceberg-table-test reference name must be same");

    fromTableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(TABLE_IDENTIFIER.name())
            .build();
    toTableReference =
        ImmutableTableReference.builder()
            .reference("Something")
            .name(renameTableIdentifier.name())
            .build();
    TableIdentifier fromIdentifierNew =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), fromTableReference.toString());
    TableIdentifier toIdentifierNew =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), toTableReference.toString());

    Assertions.assertThatThrownBy(() -> catalog.renameTable(fromIdentifierNew, toIdentifierNew))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("from: iceberg-table-test and to: Something reference name must be same");
  }

  private void verifyCommitMetadata() throws NessieNotFoundException {
    // check that the author is properly set
    List<LogEntry> log = api.getCommitLog().refName(BRANCH).get().getLogEntries();
    Assertions.assertThat(log)
        .isNotNull()
        .isNotEmpty()
        .allSatisfy(
            logEntry -> {
              CommitMeta commit = logEntry.getCommitMeta();
              Assertions.assertThat(commit.getAuthor()).isNotNull().isNotEmpty();
              Assertions.assertThat(commit.getAuthor()).isEqualTo(System.getProperty("user.name"));
              Assertions.assertThat(commit.getProperties().get(NessieUtil.APPLICATION_TYPE))
                  .isEqualTo("iceberg");
              Assertions.assertThat(commit.getMessage()).startsWith("Iceberg");
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
  public void testDropWithTableReference() throws NessieNotFoundException {
    ImmutableTableReference tableReference =
        ImmutableTableReference.builder()
            .reference(catalog.currentRefName())
            .name(TABLE_IDENTIFIER.name())
            .build();
    TableIdentifier identifier =
        TableIdentifier.of(TABLE_IDENTIFIER.namespace(), tableReference.toString());
    Assertions.assertThat(catalog.tableExists(identifier)).isTrue();
    Assertions.assertThat(catalog.dropTable(identifier)).isTrue();
    Assertions.assertThat(catalog.tableExists(identifier)).isFalse();
    verifyCommitMetadata();
  }

  @Test
  public void testDropWithoutPurgeLeavesTableData() throws IOException {
    Table table = catalog.loadTable(TABLE_IDENTIFIER);

    String fileLocation = addRecordsToFile(table, "file");

    DataFile file = makeDataFile(table, fileLocation);

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

    String location1 = addRecordsToFile(table, "file1");
    String location2 = addRecordsToFile(table, "file2");

    DataFile file1 = makeDataFile(table, location1);
    DataFile file2 = makeDataFile(table, location2);

    // add both data files
    table.newAppend().appendFile(file1).appendFile(file2).commit();

    // delete file2
    table.newDelete().deleteFile(file2.path()).commit();

    String manifestListLocation =
        table.currentSnapshot().manifestListLocation().replace("file:", "");

    List<ManifestFile> manifests = table.currentSnapshot().allManifests(table.io());

    Assertions.assertThat(catalog.dropTable(TABLE_IDENTIFIER)).isTrue();
    Assertions.assertThat(catalog.tableExists(TABLE_IDENTIFIER)).isFalse();

    Assertions.assertThat(new File(location1)).exists();
    Assertions.assertThat(new File(location2)).exists();
    Assertions.assertThat(new File(manifestListLocation)).exists();
    for (ManifestFile manifest : manifests) {
      Assertions.assertThat(new File(manifest.path().replace("file:", ""))).exists();
    }
    TableOperations ops = ((HasTableOperations) table).operations();
    String metadataLocation = ((NessieTableOperations) ops).currentMetadataLocation();
    Assertions.assertThat(new File(metadataLocation.replace("file:", ""))).exists();

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
    Branch branch = (Branch) api.getReference().refName(BRANCH).get();

    IcebergTable table = getTable(BRANCH, KEY);

    IcebergTable value = IcebergTable.of("dummytable.metadata.json", 42, 42, 42, 42, "cid");
    api.commitMultipleOperations()
        .branch(branch)
        .operation(Operation.Put.of(KEY, value))
        .commitMeta(CommitMeta.fromMessage(""))
        .commit();

    Assertions.assertThatThrownBy(
            () -> icebergTable.updateSchema().addColumn("data", Types.LongType.get()).commit())
        .isInstanceOf(CommitFailedException.class)
        .hasMessage(
            "Cannot commit: Reference hash is out of date. Update the reference 'iceberg-table-test' and try again");
  }

  @Test
  public void testListTables() {
    List<TableIdentifier> tableIdents = catalog.listTables(TABLE_IDENTIFIER.namespace());
    List<TableIdentifier> expectedIdents =
        tableIdents.stream()
            .filter(t -> t.namespace().level(0).equals(DB_NAME) && t.name().equals(TABLE_NAME))
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

  @SuppressWarnings(
      "RegexpSinglelineJava") // respecting this rule requires a lot more lines of code
  private List<String> metadataFiles(String tableName) {
    return Arrays.stream(Objects.requireNonNull(new File(metadataLocation(tableName)).listFiles()))
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
    return metadataFiles(tableName).stream()
        .filter(f -> f.endsWith(extension))
        .collect(Collectors.toList());
  }

  private static String addRecordsToFile(Table table, String filename) throws IOException {
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(schema, "test"));
    List<GenericData.Record> records = Lists.newArrayListWithCapacity(3);
    records.add(recordBuilder.set("id", 1L).build());
    records.add(recordBuilder.set("id", 2L).build());
    records.add(recordBuilder.set("id", 3L).build());

    return writeRecordsToFile(table, schema, filename, records);
  }
}
