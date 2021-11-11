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

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.projectnessie.error.NessieConflictException;
import org.projectnessie.error.NessieNotFoundException;

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
    catalog.refresh();
    createBranch("test", catalog.currentHash());
    testCatalog = initCatalog("test");
  }

  @AfterEach
  public void after() throws NessieNotFoundException, NessieConflictException {
    catalog.dropTable(tableIdentifier1);
    catalog.dropTable(tableIdentifier2);
    catalog.refresh();
    catalog.getTreeApi().deleteBranch("test", catalog.getTreeApi().getReferenceByName("test").getHash());
    testCatalog = null;
  }

  @Test
  public void testBranchNoChange() {
    testCatalogEquality(catalog, testCatalog, true, true);
  }

  @Test
  public void testUpdateCatalogs() {
    // ensure catalogs can't see each others updates
    updateSchema(catalog, tableIdentifier1);

    testCatalogEquality(catalog, testCatalog, false, true);

    String initialMetadataLocation = metadataLocation(testCatalog, tableIdentifier2);
    updateSchema(testCatalog, tableIdentifier2);

    testCatalogEquality(catalog, testCatalog, false, false);

    // points to the previous metadata location
    Assertions.assertThat(initialMetadataLocation).isEqualTo(metadataLocation(catalog, tableIdentifier2));
  }

  @Test
  public void testCatalogOnReference() {
    updateSchema(catalog, tableIdentifier1);
    updateSchema(testCatalog, tableIdentifier2);

    // catalog created with ref points to same catalog as above
    NessieCatalog refCatalog = initCatalog("test");
    testCatalogEquality(refCatalog, testCatalog, true, true);

    // catalog created with hash points to same catalog as above
    NessieCatalog refHashCatalog = initCatalog("main");
    testCatalogEquality(refHashCatalog, catalog, true, true);
  }

  @Test
  public void testCatalogWithTableNames() {
    updateSchema(testCatalog, tableIdentifier2);

    String mainName = "main";

    // asking for table@branch gives expected regardless of catalog
    Assertions.assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@test")))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));

    // Asking for table@branch gives expected regardless of catalog.
    // Earlier versions used "table1@" + tree.getReferenceByName("main").getHash() before, but since
    // Nessie 0.8.2 the branch name became mandatory and specifying a hash within a branch is not
    // possible.
    Assertions.assertThat(metadataLocation(catalog, TableIdentifier.of("test-ns", "table1@" + mainName)))
        .isEqualTo(metadataLocation(testCatalog, tableIdentifier1));
  }

  @Test
  public void testConcurrentChanges() {
    NessieCatalog emptyTestCatalog = initCatalog("test");
    updateSchema(testCatalog, tableIdentifier1);
    // Updating table with out of date hash. We expect this to succeed because of retry despite the conflict.
    updateSchema(emptyTestCatalog, tableIdentifier1);
  }

  private void updateSchema(NessieCatalog catalog, TableIdentifier identifier) {
    catalog.loadTable(identifier).updateSchema().addColumn("id" + schemaCounter++, Types.LongType.get()).commit();
  }

  private void testCatalogEquality(
      NessieCatalog catalog, NessieCatalog compareCatalog, boolean table1Equal, boolean table2Equal) {
    String testTable1 = metadataLocation(compareCatalog, tableIdentifier1);
    String table1 = metadataLocation(catalog, tableIdentifier1);
    String testTable2 = metadataLocation(compareCatalog, tableIdentifier2);
    String table2 = metadataLocation(catalog, tableIdentifier2);

    Assertions.assertThat(table1.equals(testTable1))
        .withFailMessage(() -> String.format(
            "Table %s on ref %s should%s equal table %s on ref %s",
            tableIdentifier1.name(),
            tableIdentifier2.name(),
            table1Equal ? "" : " not",
            catalog.currentRefName(),
            testCatalog.currentRefName()))
        .isEqualTo(table1Equal);

    Assertions.assertThat(table2.equals(testTable2))
        .withFailMessage(() -> String.format(
            "Table %s on ref %s should%s equal table %s on ref %s",
            tableIdentifier1.name(),
            tableIdentifier2.name(),
            table1Equal ? "" : " not",
            catalog.currentRefName(),
            testCatalog.currentRefName()))
        .isEqualTo(table2Equal);
  }
}
