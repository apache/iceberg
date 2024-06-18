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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.view.BaseView;
import org.apache.iceberg.view.View;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;
import org.projectnessie.model.Branch;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Reference;

public class TestBranchVisibility extends BaseTestIceberg {

  private final TableIdentifier tableIdentifier1 = TableIdentifier.of("test-ns", "table1");
  private final TableIdentifier tableIdentifier2 = TableIdentifier.of("test-ns", "table2");
  private NessieCatalog testCatalog;
  private int schemaCounter = 1;

  public TestBranchVisibility() {
    super("main");
  }

  @BeforeEach
  public void before() throws NessieNotFoundException, NessieConflictException {
    createTable(tableIdentifier1, 1); // table 1
    createTable(tableIdentifier2, 1); // table 2
    createBranch("test");
    testCatalog = initCatalog("test");
  }

  @AfterEach
  public void after() throws NessieNotFoundException, NessieConflictException {
    catalog.dropTable(tableIdentifier1);
    catalog.dropTable(tableIdentifier2);
    for (Reference reference : api.getAllReferences().get().getReferences()) {
      if (!reference.getName().equals("main")) {
        api.deleteBranch().branch((Branch) reference).delete();
      }
    }
    testCatalog = null;
  }

  @Test
  public void testBranchNoChange() {
    testCatalogEquality(catalog, testCatalog, true, true, () -> {});
  }

  /** Ensure catalogs can't see each others updates. */
  @Test
  public void testUpdateCatalogs() {
    testCatalogEquality(
        catalog, testCatalog, false, true, () -> updateSchema(catalog, tableIdentifier1));

    testCatalogEquality(
        catalog, testCatalog, false, false, () -> updateSchema(catalog, tableIdentifier2));
  }

  @Test
  public void testCatalogOnReference() {
    updateSchema(catalog, tableIdentifier1);
    updateSchema(testCatalog, tableIdentifier2);

    // catalog created with ref points to same catalog as above
    NessieCatalog refCatalog = initCatalog("test");
    testCatalogEquality(refCatalog, testCatalog, true, true, () -> {});

    // catalog created with hash points to same catalog as above
    NessieCatalog refHashCatalog = initCatalog("main");
    testCatalogEquality(refHashCatalog, catalog, true, true, () -> {});
  }

  @Test
  public void testCatalogWithTableNames() {
    updateSchema(testCatalog, tableIdentifier2);

    String mainName = "main";

    // asking for table@branch gives expected regardless of catalog
    assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@test")))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));

    // Asking for table@branch gives expected regardless of catalog.
    // Earlier versions used "table1@" + tree.getReferenceByName("main").getHash() before, but since
    // Nessie 0.8.2 the branch name became mandatory and specifying a hash within a branch is not
    // possible.
    assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@" + mainName)))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));
  }

  @Test
  public void testConcurrentChanges() {
    NessieCatalog emptyTestCatalog = initCatalog("test");
    updateSchema(testCatalog, tableIdentifier1);
    // Updating table with out of date hash. We expect this to succeed because of retry despite the
    // conflict.
    updateSchema(emptyTestCatalog, tableIdentifier1);
  }

  @Test
  public void testSchemaSnapshot() throws Exception {

    String branchTest = "test";
    String branch1 = "branch-1";
    String branch2 = "branch-2";

    NessieCatalog catalog = initCatalog(branchTest);
    String metadataOnTest =
        addRow(catalog, tableIdentifier1, "initial-data", ImmutableMap.of("id0", 4L));
    long snapshotIdOnTest = snapshotIdFromMetadata(catalog, metadataOnTest);

    String hashOnTest = catalog.currentHash();
    createBranch(branch1, hashOnTest, branchTest);
    createBranch(branch2, hashOnTest, branchTest);

    String metadataOnTest2 =
        addRow(catalog, tableIdentifier1, "added-data-on-test", ImmutableMap.of("id0", 5L));
    assertThat(metadataOnTest2).isNotEqualTo(metadataOnTest);
    long snapshotIdOnTest2 = snapshotIdFromMetadata(catalog, metadataOnTest2);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest2, 0);

    NessieCatalog catalogBranch1 = initCatalog(branch1);
    updateSchema(catalogBranch1, tableIdentifier1, Types.StringType.get());
    verifyRefState(catalogBranch1, tableIdentifier1, snapshotIdOnTest, 1);
    String metadataOn1 =
        addRow(
            catalogBranch1,
            tableIdentifier1,
            "testSchemaSnapshot-in-1",
            ImmutableMap.of("id0", 42L, "id1", "world"));
    assertThat(metadataOn1).isNotEqualTo(metadataOnTest).isNotEqualTo(metadataOnTest2);

    NessieCatalog catalogBranch2 = initCatalog(branch2);
    updateSchema(catalogBranch2, tableIdentifier1, Types.IntegerType.get());
    verifyRefState(catalogBranch2, tableIdentifier1, snapshotIdOnTest, 1);
    String metadataOn2 =
        addRow(
            catalogBranch2,
            tableIdentifier1,
            "testSchemaSnapshot-in-2",
            ImmutableMap.of("id0", 43L, "id2", 666));
    assertThat(metadataOn2).isNotEqualTo(metadataOnTest).isNotEqualTo(metadataOnTest2);
  }

  @Test
  public void testMetadataLocation() throws Exception {
    String branch1 = "test";
    String branch2 = "branch-2";

    // commit on tableIdentifier1 on branch1
    NessieCatalog catalog = initCatalog(branch1);
    String metadataLocationOfCommit1 =
        addRow(catalog, tableIdentifier1, "initial-data", ImmutableMap.of("id0", 4L));

    createBranch(branch2, catalog.currentHash(), branch1);
    // commit on tableIdentifier1 on branch2
    catalog = initCatalog(branch2);
    String metadataLocationOfCommit2 =
        addRow(catalog, tableIdentifier1, "some-more-data", ImmutableMap.of("id0", 42L));
    assertThat(metadataLocationOfCommit2).isNotNull().isNotEqualTo(metadataLocationOfCommit1);

    catalog = initCatalog(branch1);
    // load tableIdentifier1 on branch1
    BaseTable table = (BaseTable) catalog.loadTable(tableIdentifier1);
    // branch1's tableIdentifier1's metadata location must not have changed
    assertThat(table.operations().current().metadataFileLocation())
        .isNotNull()
        .isNotEqualTo(metadataLocationOfCommit2);
  }

  /**
   * Complex-ish test case that verifies that both the snapshot-ID and schema-ID are properly set
   * and retained when working with a mixture of DDLs and DMLs across multiple branches.
   */
  @Test
  public void testStateTrackingOnMultipleBranches() throws Exception {

    String branchTest = "test";
    String branchA = "branch_a";
    String branchB = "branch_b";

    NessieCatalog catalog = initCatalog(branchTest);
    verifySchema(catalog, tableIdentifier1, Types.LongType.get());
    String initialLocation = metadataLocation(catalog, tableIdentifier1);

    // Verify last-column-id
    verifyRefState(catalog, tableIdentifier1, -1L, 0);

    // Add a row and verify that the
    String metadataOnTest =
        addRow(catalog, tableIdentifier1, "initial-data", Collections.singletonMap("id0", 1L));
    assertThat(metadataOnTest).isNotEqualTo(initialLocation);
    long snapshotIdOnTest = snapshotIdFromMetadata(catalog, metadataOnTest);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String hashOnTest = catalog.currentHash();
    createBranch(branchA, hashOnTest, branchTest);
    createBranch(branchB, hashOnTest, branchTest);

    NessieCatalog catalogBranchA = initCatalog(branchA);
    // branchA hasn't been modified yet, so it must be "equal" to branch "test"
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnTest, 0);
    // updateSchema updates the schema on branch "branch_a", but not on branch "test"
    updateSchema(catalogBranchA, tableIdentifier1, Types.StringType.get());
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnTest, 1);
    verifySchema(catalogBranchA, tableIdentifier1, Types.LongType.get(), Types.StringType.get());
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String metadataOnA1 =
        addRow(
            catalogBranchA,
            tableIdentifier1,
            "branch-a-1",
            ImmutableMap.of("id0", 2L, "id1", "hello"));
    // addRow() must produce a new metadata
    assertThat(metadataOnA1).isNotEqualTo(metadataOnTest);
    long snapshotIdOnA1 = snapshotIdFromMetadata(catalogBranchA, metadataOnA1);
    assertThat(snapshotIdOnA1).isNotEqualTo(snapshotIdOnTest);
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnA1, 1);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    NessieCatalog catalogBranchB = initCatalog(branchB);
    long snapshotIdOnB = snapshotIdFromNessie(catalogBranchB, tableIdentifier1);
    assertThat(snapshotIdOnB).isEqualTo(snapshotIdOnTest);
    // branchB hasn't been modified yet, so it must be "equal" to branch "test"
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB, 0);
    // updateSchema should use schema-id 1, because it's not tracked globally
    updateSchema(catalogBranchB, tableIdentifier1, Types.LongType.get());
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB, 1);
    verifySchema(catalogBranchB, tableIdentifier1, Types.LongType.get(), Types.LongType.get());
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    String metadataOnB1 =
        addRow(
            catalogBranchB, tableIdentifier1, "branch-b-1", ImmutableMap.of("id0", 3L, "id2", 42L));
    long snapshotIdOnB1 = snapshotIdFromMetadata(catalogBranchB, metadataOnB1);
    // addRow() must produce a new metadata
    assertThat(metadataOnB1).isNotEqualTo(metadataOnA1).isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB1, 1);
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);

    // repeat addRow() against branchA
    catalogBranchA = initCatalog(branchA);
    verifySchema(catalogBranchA, tableIdentifier1, Types.LongType.get(), Types.StringType.get());
    String metadataOnA2 =
        addRow(
            catalogBranchA,
            tableIdentifier1,
            "branch-a-2",
            ImmutableMap.of("id0", 4L, "id1", "hello"));
    long snapshotIdOnA2 = snapshotIdFromMetadata(catalogBranchA, metadataOnA2);
    assertThat(metadataOnA2)
        .isNotEqualTo(metadataOnA1)
        .isNotEqualTo(metadataOnB1)
        .isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchA, tableIdentifier1, snapshotIdOnA2, 1);

    // repeat addRow() against branchB
    catalogBranchB = initCatalog(branchB);
    verifySchema(catalogBranchB, tableIdentifier1, Types.LongType.get(), Types.LongType.get());
    String metadataOnB2 =
        addRow(
            catalogBranchB,
            tableIdentifier1,
            "branch-b-2",
            ImmutableMap.of("id0", 5L, "id2", 666L));
    long snapshotIdOnB2 = snapshotIdFromMetadata(catalogBranchA, metadataOnB2);
    assertThat(metadataOnB2)
        .isNotEqualTo(metadataOnA1)
        .isNotEqualTo(metadataOnA2)
        .isNotEqualTo(metadataOnB1)
        .isNotEqualTo(metadataOnTest);
    verifyRefState(catalogBranchB, tableIdentifier1, snapshotIdOnB2, 1);

    // sanity check, branch "test" must not have changed
    verifyRefState(catalog, tableIdentifier1, snapshotIdOnTest, 0);
  }

  private void verifyRefState(
      NessieCatalog catalog, TableIdentifier identifier, long snapshotId, int schemaId)
      throws Exception {
    IcebergTable icebergTable = loadIcebergTable(catalog, identifier);
    assertThat(icebergTable)
        .extracting(IcebergTable::getSnapshotId, IcebergTable::getSchemaId)
        .containsExactly(snapshotId, schemaId);
  }

  private long snapshotIdFromNessie(NessieCatalog catalog, TableIdentifier identifier)
      throws Exception {
    IcebergTable icebergTable = loadIcebergTable(catalog, identifier);
    return icebergTable.getSnapshotId();
  }

  private long snapshotIdFromMetadata(NessieCatalog catalog, String metadataLocation) {
    Snapshot snapshot =
        TableMetadataParser.read(catalog.fileIO(), metadataLocation).currentSnapshot();
    return snapshot != null ? snapshot.snapshotId() : -1;
  }

  private IcebergTable loadIcebergTable(NessieCatalog catalog, TableIdentifier identifier)
      throws NessieNotFoundException {
    ContentKey key = NessieUtil.toKey(identifier);
    return api.getContent()
        .refName(catalog.currentRefName())
        .key(key)
        .get()
        .get(key)
        .unwrap(IcebergTable.class)
        .orElseThrow(NullPointerException::new);
  }

  private String addRow(
      NessieCatalog catalog, TableIdentifier identifier, String fileName, Map<String, Object> data)
      throws Exception {
    Table table = catalog.loadTable(identifier);
    GenericRecordBuilder recordBuilder =
        new GenericRecordBuilder(AvroSchemaUtil.convert(table.schema(), table.name()));
    data.forEach(recordBuilder::set);

    String fileLocation =
        writeRecordsToFile(
            table, table.schema(), fileName, Collections.singletonList(recordBuilder.build()));
    DataFile dataFile = makeDataFile(table, fileLocation);

    // Run via `Transaction` to exercise the whole code path ran via Spark (Spark SQL)
    Transaction tx = table.newTransaction();
    tx.newAppend().appendFile(dataFile).commit();
    tx.commitTransaction();

    return metadataLocation(catalog, identifier);
  }

  private void verifySchema(NessieCatalog catalog, TableIdentifier identifier, Type... types) {
    assertThat(catalog.loadTable(identifier))
        .extracting(t -> t.schema().columns().stream().map(NestedField::type))
        .asInstanceOf(InstanceOfAssertFactories.stream(Type.class))
        .containsExactly(types);
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier) {
    updateSchema(catalog, identifier, Types.LongType.get());
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier, Type type) {
    // Run via `Transaction` to exercise the whole code path ran via Spark (Spark SQL)
    Transaction tx = catalog.loadTable(identifier).newTransaction();
    tx.updateSchema().addColumn("id" + schemaCounter++, type).commit();
    tx.commitTransaction();
  }

  private void testCatalogEquality(
      NessieCatalog catalog,
      NessieCatalog compareCatalog,
      boolean table1Equal,
      boolean table2Equal,
      ThrowingCallable callable) {
    String testTable1 = metadataLocation(compareCatalog, tableIdentifier1);
    String testTable2 = metadataLocation(compareCatalog, tableIdentifier2);

    try {
      callable.call();
    } catch (RuntimeException e) {
      throw e;
    } catch (Throwable e) {
      throw new RuntimeException(e);
    }

    String table1 = metadataLocation(catalog, tableIdentifier1);
    String table2 = metadataLocation(catalog, tableIdentifier2);

    AbstractStringAssert<?> assertion =
        assertThat(table1)
            .describedAs(
                "Table %s on ref %s should%s be equal to table %s on ref %s",
                tableIdentifier1.name(),
                catalog.currentRefName(),
                table1Equal ? "" : " not",
                tableIdentifier1.name(),
                compareCatalog.currentRefName());
    if (table1Equal) {
      assertion.isEqualTo(testTable1);
    } else {
      assertion.isNotEqualTo(testTable1);
    }

    assertion =
        assertThat(table2)
            .describedAs(
                "Table %s on ref %s should%s be equal to table %s on ref %s",
                tableIdentifier2.name(),
                catalog.currentRefName(),
                table2Equal ? "" : " not",
                tableIdentifier2.name(),
                compareCatalog.currentRefName());
    if (table2Equal) {
      assertion.isEqualTo(testTable2);
    } else {
      assertion.isNotEqualTo(testTable2);
    }
  }

  @Test
  public void testWithRefAndHash() throws NessieConflictException, NessieNotFoundException {
    String testBranch = "testBranch";
    createBranch(testBranch);
    Schema schema =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());

    NessieCatalog nessieCatalog = initCatalog(testBranch);
    String hashBeforeNamespaceCreation = api.getReference().refName(testBranch).get().getHash();
    Namespace namespaceA = Namespace.of("a");
    Namespace namespaceAB = Namespace.of("a", "b");
    assertThat(nessieCatalog.listNamespaces(namespaceAB)).isEmpty();

    createMissingNamespaces(
        nessieCatalog, Namespace.of(Arrays.copyOf(namespaceAB.levels(), namespaceAB.length() - 1)));
    nessieCatalog.createNamespace(namespaceAB);
    assertThat(nessieCatalog.listNamespaces(namespaceAB)).isEmpty();
    assertThat(nessieCatalog.listNamespaces(namespaceA)).containsExactly(namespaceAB);
    assertThat(nessieCatalog.listTables(namespaceAB)).isEmpty();

    NessieCatalog catalogAtHash1 = initCatalog(testBranch, hashBeforeNamespaceCreation);
    assertThat(catalogAtHash1.listNamespaces(namespaceAB)).isEmpty();
    assertThat(catalogAtHash1.listTables(namespaceAB)).isEmpty();

    TableIdentifier identifier = TableIdentifier.of(namespaceAB, "table");
    String hashBeforeTableCreation = nessieCatalog.currentHash();
    nessieCatalog.createTable(identifier, schema);
    assertThat(nessieCatalog.listTables(namespaceAB)).hasSize(1);

    NessieCatalog catalogAtHash2 = initCatalog(testBranch, hashBeforeTableCreation);
    assertThat(catalogAtHash2.listNamespaces(namespaceAB)).isEmpty();
    assertThat(catalogAtHash2.listNamespaces(namespaceA)).containsExactly(namespaceAB);
    assertThat(catalogAtHash2.listTables(namespaceAB)).isEmpty();

    // updates should not be possible
    assertThatThrownBy(() -> catalogAtHash2.createTable(identifier, schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(
            "You can only mutate tables/views when using a branch without a hash or timestamp.");
    assertThat(catalogAtHash2.listTables(namespaceAB)).isEmpty();

    // updates should be still possible here
    nessieCatalog = initCatalog(testBranch);
    TableIdentifier identifier2 = TableIdentifier.of(namespaceAB, "table2");
    nessieCatalog.createTable(identifier2, schema);
    assertThat(nessieCatalog.listTables(namespaceAB)).hasSize(2);
  }

  @Test
  public void testDifferentTableSameName() throws NessieConflictException, NessieNotFoundException {
    String branch1 = "branch1";
    String branch2 = "branch2";
    createBranch(branch1);
    createBranch(branch2);
    Schema schema1 =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    Schema schema2 =
        new Schema(
            Types.StructType.of(
                    required(1, "file_count", Types.IntegerType.get()),
                    required(2, "record_count", Types.LongType.get()))
                .fields());

    TableIdentifier identifier = TableIdentifier.of("db", "table1");

    NessieCatalog nessieCatalog = initCatalog(branch1);

    createMissingNamespaces(nessieCatalog, identifier);
    Table table1 = nessieCatalog.createTable(identifier, schema1);
    assertThat(table1.schema().asStruct()).isEqualTo(schema1.asStruct());

    nessieCatalog = initCatalog(branch2);
    createMissingNamespaces(nessieCatalog, identifier);
    Table table2 = nessieCatalog.createTable(identifier, schema2);
    assertThat(table2.schema().asStruct()).isEqualTo(schema2.asStruct());

    assertThat(table1.location()).isNotEqualTo(table2.location());
  }

  @Test
  public void testViewMetadataLocation() throws Exception {
    String branch1 = "branch-1";
    String branch2 = "branch-2";

    TableIdentifier viewIdentifier = TableIdentifier.of("test-ns", "view1");
    createView(catalog, viewIdentifier);

    createBranch(branch1, catalog.currentHash());
    // commit on viewIdentifier on branch1
    NessieCatalog catalog = initCatalog(branch1);
    String metadataLocationOfCommit1 =
        ((BaseView) replaceView(catalog, viewIdentifier))
            .operations()
            .current()
            .metadataFileLocation();

    createBranch(branch2, catalog.currentHash(), branch1);
    // commit on viewIdentifier on branch2
    catalog = initCatalog(branch2);
    String metadataLocationOfCommit2 =
        ((BaseView) replaceView(catalog, viewIdentifier))
            .operations()
            .current()
            .metadataFileLocation();

    assertThat(metadataLocationOfCommit2).isNotNull().isNotEqualTo(metadataLocationOfCommit1);

    catalog = initCatalog(branch1);
    // load viewIdentifier on branch1
    BaseView view = (BaseView) catalog.loadView(viewIdentifier);
    // branch1's viewIdentifier's metadata location must not have changed
    assertThat(view.operations().current().metadataFileLocation())
        .isNotNull()
        .isNotEqualTo(metadataLocationOfCommit2);

    catalog.dropView(viewIdentifier);
  }

  @Test
  public void testDifferentViewSameName() throws NessieConflictException, NessieNotFoundException {
    String branch1 = "branch1";
    String branch2 = "branch2";
    createBranch(branch1);
    createBranch(branch2);
    Schema schema1 =
        new Schema(Types.StructType.of(required(1, "id", Types.LongType.get())).fields());
    Schema schema2 =
        new Schema(
            Types.StructType.of(
                    required(1, "file_count", Types.IntegerType.get()),
                    required(2, "record_count", Types.LongType.get()))
                .fields());

    TableIdentifier identifier = TableIdentifier.of("db", "view1");

    NessieCatalog nessieCatalog = initCatalog(branch1);

    createMissingNamespaces(nessieCatalog, identifier);
    View view1 = createView(nessieCatalog, identifier, schema1);
    assertThat(view1.schema().asStruct()).isEqualTo(schema1.asStruct());

    nessieCatalog = initCatalog(branch2);
    createMissingNamespaces(nessieCatalog, identifier);
    View view2 = createView(nessieCatalog, identifier, schema2);
    assertThat(view2.schema().asStruct()).isEqualTo(schema2.asStruct());

    assertThat(view1.location()).isNotEqualTo(view2.location());
  }
}
