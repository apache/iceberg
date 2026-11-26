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
package org.apache.iceberg.data;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import java.io.File;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Files;
import org.apache.iceberg.Parameter;
import org.apache.iceberg.ParameterizedTestExtension;
import org.apache.iceberg.Parameters;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

/** Tests that a reader decrypts the data and delete files of an encrypted table. */
@ExtendWith(ParameterizedTestExtension.class)
public abstract class EncryptedTableReadTestBase {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  @Parameter(index = 0)
  protected FileFormat format;

  @Parameter(index = 1)
  protected int formatVersion;

  @TempDir private File tableDir;

  protected Table table;

  @Parameters(name = "format = {0}, formatVersion = {1}")
  protected static List<Object[]> parameters() {
    return List.of(
        new Object[] {FileFormat.PARQUET, 2},
        new Object[] {FileFormat.PARQUET, 3},
        new Object[] {FileFormat.AVRO, 2},
        new Object[] {FileFormat.AVRO, 3});
  }

  /** Reads all rows of the given table using the reader under test. */
  protected abstract StructLikeSet read(Table encryptedTable) throws Exception;

  protected boolean supportsDeletes() {
    return true;
  }

  @BeforeEach
  void createTable() {
    this.table =
        TestTables.createEncrypted(
            tableDir,
            "encrypted",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            formatVersion,
            ImmutableMap.of(
                TableProperties.DEFAULT_FILE_FORMAT,
                format.name(),
                TableProperties.DELETE_DEFAULT_FILE_FORMAT,
                format.name()),
            EncryptionTestHelpers.createEncryptionManager());
  }

  @AfterEach
  void dropTable() {
    TestTables.clearTables();
  }

  @TestTemplate
  void readEncryptedDataFiles() throws Exception {
    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile firstFile =
        FileHelpers.writeDataFile(table, outputFile("first"), List.of(first, second));
    DataFile secondFile = FileHelpers.writeDataFile(table, outputFile("second"), List.of(third));
    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();

    assertThat(firstFile.keyMetadata()).isNotNull();
    assertThat(secondFile.keyMetadata()).isNotNull();

    assertThat(read(table)).isEqualTo(rowSet(first, second, third));
  }

  @TestTemplate
  protected void readMixedPlaintextAndEncryptedDataFiles() throws Exception {
    // This table is needed so that we can create a plain text data file through the helper.
    Table plaintextTable =
        TestTables.create(
            new File(tableDir, "plaintext"),
            "plaintext",
            table.schema(),
            table.spec(),
            formatVersion,
            table.properties());

    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile plaintextFile =
        FileHelpers.writeDataFile(plaintextTable, outputFile("plaintext"), List.of(first, second));
    DataFile encryptedFile =
        FileHelpers.writeDataFile(table, outputFile("encrypted"), List.of(third));
    table.newAppend().appendFile(plaintextFile).appendFile(encryptedFile).commit();

    assertThat(plaintextFile.keyMetadata()).isNull();
    assertThat(encryptedFile.keyMetadata()).isNotNull();

    assertThat(read(table)).isEqualTo(rowSet(first, second, third));
  }

  @TestTemplate
  void readEncryptedDataFilesWithPositionDeletes() throws Exception {
    assumeThat(supportsDeletes()).isTrue();

    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile firstFile =
        FileHelpers.writeDataFile(table, outputFile("first"), List.of(first, second));
    DataFile secondFile = FileHelpers.writeDataFile(table, outputFile("second"), List.of(third));
    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();

    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(
                table,
                outputFile("pos-deletes"),
                List.of(Pair.of(firstFile.location(), 0L)),
                formatVersion)
            .first();
    table.newRowDelta().addDeletes(deleteFile).commit();

    assertThat(firstFile.keyMetadata()).isNotNull();
    assertThat(secondFile.keyMetadata()).isNotNull();
    assertThat(deleteFile.keyMetadata()).isNotNull();
    assertThat(deleteFile.format()).isEqualTo(formatVersion >= 3 ? FileFormat.PUFFIN : format);

    assertThat(read(table)).isEqualTo(rowSet(second, third));
  }

  @TestTemplate
  void readEncryptedDataFilesWithEqualityDeletes() throws Exception {
    assumeThat(supportsDeletes()).isTrue();

    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile firstFile =
        FileHelpers.writeDataFile(table, outputFile("first"), List.of(first, second));
    DataFile secondFile = FileHelpers.writeDataFile(table, outputFile("second"), List.of(third));
    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();

    Schema deleteSchema = table.schema().select("data");
    Record delete = GenericRecord.create(deleteSchema).copy("data", "c");
    DeleteFile deleteFile =
        FileHelpers.writeDeleteFile(table, outputFile("eq-deletes"), List.of(delete), deleteSchema);
    table.newRowDelta().addDeletes(deleteFile).commit();

    assertThat(firstFile.keyMetadata()).isNotNull();
    assertThat(secondFile.keyMetadata()).isNotNull();
    assertThat(deleteFile.keyMetadata()).isNotNull();

    assertThat(read(table)).isEqualTo(rowSet(first, second));
  }

  protected Record record(long id, String data) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private StructLikeSet rowSet(Record... rows) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    set.addAll(List.of(rows));
    return set;
  }

  protected OutputFile outputFile(String name) {
    return Files.localOutput(new File(tableDir, format.addExtension(name)));
  }
}
