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
package org.apache.iceberg;

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Regression tests for GH-16232.
 *
 * <p>Replace transactions must fail on concurrent table metadata changes rather than silently
 * overwriting committed updates.
 */
public class TestReplaceTableSafety {

  private static final Namespace NS = Namespace.of("db");
  private static final TableIdentifier TABLE = TableIdentifier.of(NS, "tbl");

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final Schema SCHEMA_WITH_EXTRA_COL =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          NestedField.optional(3, "extra", Types.StringType.get()));

  private static final DataFile FILE_A =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .build();

  private static final DataFile FILE_B =
      DataFiles.builder(PartitionSpec.unpartitioned())
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withRecordCount(2)
          .build();

  private InMemoryCatalog catalog;

  @BeforeEach
  public void before(@TempDir Path temp) {
    catalog = new InMemoryCatalog();
    catalog.initialize(
        "in-memory",
        ImmutableMap.of(
            CatalogProperties.WAREHOUSE_LOCATION,
            temp.toAbsolutePath().toString(),
            CatalogProperties.FILE_IO_IMPL,
            "org.apache.iceberg.inmemory.InMemoryFileIO"));
    catalog.createNamespace(NS);
  }

  @Test
  public void replaceVsSchemaUpdateFailsAndPreservesSchema() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent schema update
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.schema().asStruct()).isEqualTo(SCHEMA_WITH_EXTRA_COL.asStruct());
  }

  @Test
  public void replaceVsPropertyWriteFailsAndPreservesProperty() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent property update
    table.updateProperties().set("k1", "v1").commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.properties()).containsEntry("k1", "v1");
  }

  @Test
  public void replaceVsAppendFailsAndPreservesCommittedData() {
    catalog.buildTable(TABLE, SCHEMA).create();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent append
    catalog.loadTable(TABLE).newFastAppend().appendFile(FILE_B).commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    // concurrent data remains reachable
    assertThat(after.currentSnapshot()).isNotNull();
    assertThat(after.newScan().planFiles()).hasSize(1);
  }

  @Test
  public void replaceVsReplaceFailsSecondCommitAndPreservesFirst() {
    catalog.buildTable(TABLE, SCHEMA).create();

    Transaction secondReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    secondReplace.newFastAppend().appendFile(FILE_A).commit();

    Transaction firstReplace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    firstReplace.newFastAppend().appendFile(FILE_B).commit();
    firstReplace.commitTransaction();

    assertThatThrownBy(secondReplace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");
  }

  @Test
  public void replaceVsExpireSnapshotsFailsAndDoesNotResurrectExpiredSnapshot() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();
    table.newFastAppend().appendFile(FILE_A).commit();
    long snapshotId = table.currentSnapshot().snapshotId();
    table.newFastAppend().appendFile(FILE_B).commit();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(FILE_B).commit();

    // concurrent expire
    table.expireSnapshots().expireSnapshotId(snapshotId).commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.snapshot(snapshotId)).isNull();
  }

  @Test
  public void replaceVsMultipleConcurrentChangesFailsAndPreservesAll() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction replace = catalog.buildTable(TABLE, SCHEMA).replaceTransaction();
    replace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent changes
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();
    table.updateProperties().set("k1", "v1").commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.schema().asStruct()).isEqualTo(SCHEMA_WITH_EXTRA_COL.asStruct());
    assertThat(after.properties()).containsEntry("k1", "v1");
  }

  @Test
  public void createOrReplaceVsSchemaUpdateFailsAndPreservesSchema() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();
    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent schema update
    table.updateSchema().addColumn("extra", Types.StringType.get()).commit();

    assertThatThrownBy(createOrReplace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.schema().asStruct()).isEqualTo(SCHEMA_WITH_EXTRA_COL.asStruct());
  }

  @Test
  public void createOrReplaceNewTableSucceeds() {
    Transaction createOrReplace =
        catalog.buildTable(TableIdentifier.of(NS, "new_tbl"), SCHEMA).createOrReplaceTransaction();
    createOrReplace.newFastAppend().appendFile(FILE_A).commit();
    createOrReplace.commitTransaction();

    Table created = catalog.loadTable(TableIdentifier.of(NS, "new_tbl"));
    assertThat(created).isNotNull();
    assertThat(created.currentSnapshot()).isNotNull();
  }

  @Test
  public void createOrReplaceVsPropertyWriteFailsAndPreservesProperty() {
    Table table = catalog.buildTable(TABLE, SCHEMA).create();

    Transaction createOrReplace = catalog.buildTable(TABLE, SCHEMA).createOrReplaceTransaction();
    createOrReplace.newFastAppend().appendFile(FILE_A).commit();

    // concurrent property update
    table.updateProperties().set("k1", "v1").commit();

    assertThatThrownBy(createOrReplace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");

    Table after = catalog.loadTable(TABLE);
    assertThat(after.properties()).containsEntry("k1", "v1");
  }

  @Test
  public void replaceV3TableVsConcurrentAppendFails() {
    catalog.buildTable(TABLE, SCHEMA).withProperty(TableProperties.FORMAT_VERSION, "3").create();

    Table table = catalog.loadTable(TABLE);
    table.newFastAppend().appendFile(FILE_A).commit();

    Transaction replace =
        catalog
            .buildTable(TABLE, SCHEMA)
            .withProperty(TableProperties.FORMAT_VERSION, "3")
            .replaceTransaction();
    replace.newFastAppend().appendFile(FILE_B).commit();

    // concurrent append on v3 table advances next-row-id
    table = catalog.loadTable(TABLE);
    table.newFastAppend().appendFile(FILE_A).commit();

    assertThatThrownBy(replace::commitTransaction)
        .isInstanceOf(org.apache.iceberg.exceptions.CommitFailedException.class)
        .hasMessageContaining("replace transaction");
  }
}
