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

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.encryption.EncryptingFileIO;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestGenericReaderEncryption {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));

  @TempDir private File tableDir;

  private Table table;

  @BeforeEach
  void createTable() {
    EncryptionManager encryptionManager = EncryptionTestHelpers.createEncryptionManager();
    TestTables.TestTableOperations ops =
        new TestTables.TestTableOperations(
            "encrypted",
            tableDir,
            EncryptingFileIO.combine(new TestTables.LocalFileIO(), encryptionManager)) {
          @Override
          public EncryptionManager encryption() {
            return encryptionManager;
          }
        };

    this.table =
        TestTables.create(
            tableDir,
            "encrypted",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            SortOrder.unsorted(),
            3,
            ops);
  }

  @AfterEach
  void dropTable() {
    TestTables.clearTables();
  }

  @Test
  void readEncryptedDataFilesWithDV() throws IOException {
    Record first = record(1L, "a");
    Record second = record(2L, "b");
    Record third = record(3L, "c");
    DataFile firstFile =
        FileHelpers.writeDataFile(table, outputFile("first.parquet"), List.of(first, second));
    DataFile secondFile =
        FileHelpers.writeDataFile(table, outputFile("second.parquet"), List.of(third));
    table.newAppend().appendFile(firstFile).appendFile(secondFile).commit();

    DeleteFile dv =
        FileHelpers.writeDeleteFile(table, null, List.of(Pair.of(firstFile.location(), 0L)), 3)
            .first();
    table.newRowDelta().addDeletes(dv).commit();

    assertThat(firstFile.keyMetadata()).isNotNull();
    assertThat(secondFile.keyMetadata()).isNotNull();
    assertThat(dv.keyMetadata()).isNotNull();

    StructLikeSet expected = StructLikeSet.create(SCHEMA.asStruct());
    expected.add(second);
    expected.add(third);

    StructLikeSet actual = StructLikeSet.create(SCHEMA.asStruct());
    try (CloseableIterable<Record> rows = IcebergGenerics.read(table).build()) {
      rows.forEach(actual::add);
    }

    assertThat(actual).isEqualTo(expected);
  }

  private static Record record(long id, String data) {
    Record record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private OutputFile outputFile(String fileName) {
    return Files.localOutput(new File(tableDir, fileName));
  }
}
