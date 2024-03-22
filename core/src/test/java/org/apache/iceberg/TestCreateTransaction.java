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

import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(ParameterizedTestExtension.class)
public class TestCreateTransaction extends TestBase {
  @Parameters(name = "formatVersion = {0}")
  protected static List<Object> parameters() {
    return Arrays.asList(1, 2);
  }

  @TestTemplate
  public void testCreateTransaction() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_create", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_create")).isNull();
    assertThat(TestTables.metadataVersion("test_create")).isNull();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_create");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_create")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).isEmpty();

    assertThat(meta.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).isEmpty();
  }

  @TestTemplate
  public void testCreateTransactionAndUpdateSchema() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_create", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_create")).isNull();
    assertThat(TestTables.metadataVersion("test_create")).isNull();

    txn.updateSchema()
        .allowIncompatibleChanges()
        .addRequiredColumn("col", Types.StringType.get())
        .setIdentifierFields("id", "col")
        .commit();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_create");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_create")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).isEmpty();

    Schema resultSchema =
        new Schema(
            Lists.newArrayList(
                required(1, "id", Types.IntegerType.get()),
                required(2, "data", Types.StringType.get()),
                required(3, "col", Types.StringType.get())),
            Sets.newHashSet(1, 3));

    assertThat(meta.schema().asStruct()).isEqualTo(resultSchema.asStruct());
    assertThat(meta.schema().identifierFieldIds()).isEqualTo(resultSchema.identifierFieldIds());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).isEmpty();
  }

  @TestTemplate
  public void testCreateAndAppendWithTransaction() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_append", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_append")).isNull();

    txn.newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_append")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).hasSize(1);

    assertThat(meta.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).hasSize(1);

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  @TestTemplate
  public void testCreateAndAppendWithTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_append", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_append"))
        .isEqualTo(TestTables.readMetadata("test_append"));
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    assertThat(txn.table()).isInstanceOf(BaseTransaction.TransactionTable.class);

    txn.table().newAppend().appendFile(FILE_A).appendFile(FILE_B).commit();

    assertThat(TestTables.readMetadata("test_append")).isNull();
    assertThat(TestTables.metadataVersion("test_append")).isNull();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_append");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_append")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).hasSize(1);

    assertThat(meta.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).hasSize(1);

    validateSnapshot(null, meta.currentSnapshot(), FILE_A, FILE_B);
  }

  @TestTemplate
  public void testCreateAndUpdatePropertiesWithTransaction() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_properties", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_properties")).isNull();
    assertThat(TestTables.metadataVersion("test_properties")).isNull();

    txn.updateProperties().set("test-property", "test-value").commit();

    assertThat(TestTables.readMetadata("test_properties")).isNull();
    assertThat(TestTables.metadataVersion("test_properties")).isNull();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_properties");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_properties")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).isEmpty();

    assertThat(meta.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).isEmpty();
    assertThat(meta.properties()).hasSize(1).containsEntry("test-property", "test-value");
  }

  @TestTemplate
  public void testCreateAndUpdatePropertiesWithTable() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_properties", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("test_properties")).isNull();
    assertThat(TestTables.metadataVersion("test_properties")).isNull();

    assertThat(txn.table()).isInstanceOf(BaseTransaction.TransactionTable.class);

    txn.table().updateProperties().set("test-property", "test-value").commit();

    assertThat(TestTables.readMetadata("test_properties")).isNull();
    assertThat(TestTables.metadataVersion("test_properties")).isNull();

    txn.commitTransaction();

    TableMetadata meta = TestTables.readMetadata("test_properties");
    assertThat(meta).isNotNull();
    assertThat(TestTables.metadataVersion("test_properties")).isEqualTo(0);
    assertThat(listManifestFiles(tableDir)).hasSize(0);

    assertThat(meta.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(meta.spec()).isEqualTo(unpartitioned());
    assertThat(meta.snapshots()).isEmpty();
    assertThat(meta.properties()).hasSize(1).containsEntry("test-property", "test-value");
  }

  @TestTemplate
  public void testCreateDetectsUncommittedChange() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn =
        TestTables.beginCreate(tableDir, "uncommitted_change", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("uncommitted_change")).isNull();
    assertThat(TestTables.metadataVersion("uncommitted_change")).isNull();

    txn.updateProperties().set("test-property", "test-value"); // not committed

    assertThatThrownBy(txn::newDelete)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot create new DeleteFiles: last operation has not committed");
  }

  @TestTemplate
  public void testCreateDetectsUncommittedChangeOnCommit() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn =
        TestTables.beginCreate(tableDir, "uncommitted_change", SCHEMA, unpartitioned());

    assertThat(TestTables.readMetadata("uncommitted_change")).isNull();
    assertThat(TestTables.metadataVersion("uncommitted_change")).isNull();

    txn.updateProperties().set("test-property", "test-value"); // not committed

    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Cannot commit transaction: last operation has not committed");
  }

  @TestTemplate
  public void testCreateTransactionConflict() throws IOException {
    File tableDir = Files.createTempDirectory(temp, "junit").toFile();
    assertThat(tableDir.delete()).isTrue();

    Transaction txn = TestTables.beginCreate(tableDir, "test_conflict", SCHEMA, SPEC);

    // append in the transaction to ensure a manifest file is created
    txn.newAppend().appendFile(FILE_A).commit();

    assertThat(TestTables.readMetadata("test_conflict")).isNull();
    assertThat(TestTables.metadataVersion("test_conflict")).isNull();

    Table conflict =
        TestTables.create(tableDir, "test_conflict", SCHEMA, unpartitioned(), formatVersion);

    assertThat(conflict.schema().asStruct())
        .isEqualTo(TypeUtil.assignIncreasingFreshIds(SCHEMA).asStruct());
    assertThat(conflict.spec()).isEqualTo(unpartitioned());
    assertThat(conflict.snapshots()).isEmpty();

    assertThatThrownBy(txn::commitTransaction)
        .isInstanceOf(CommitFailedException.class)
        .hasMessageStartingWith("Commit failed: table was updated");

    assertThat(listManifestFiles(tableDir)).isEmpty();
  }
}
