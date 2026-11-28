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
package org.apache.iceberg.flink.maintenance.operator;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.util.List;
import java.util.Set;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.ProcessFunctionTestHarnesses;
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
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionTestHelpers;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(ParameterizedTestExtension.class)
class TestEqualityConvertReaderEncryption {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "data", Types.StringType.get()));
  private static final Set<Integer> EQUALITY_FIELD_IDS = Set.of(1);

  @Parameter private FileFormat format;

  @TempDir private File tableDir;

  private Table table;

  @Parameters(name = "format = {0}")
  private static List<Object> parameters() {
    return List.of(FileFormat.PARQUET, FileFormat.AVRO);
  }

  @BeforeEach
  void createTable() {
    this.table =
        TestTables.createEncrypted(
            tableDir,
            "encrypted",
            SCHEMA,
            PartitionSpec.unpartitioned(),
            3,
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
  void readEncryptedDataFileWithDV() throws Exception {
    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, outputFile("data"), List.of(record(1L, "a"), record(2L, "b")));
    DeleteFile dv =
        FileHelpers.writeDeleteFile(
                table, outputFile("dv"), List.of(Pair.of(dataFile.location(), 0L)), 3)
            .first();

    assertThat(dataFile.keyMetadata()).isNotNull();
    assertThat(dv.keyMetadata()).isNotNull();

    ReadCommand command =
        ReadCommand.dataFile(
            new FlinkAddedRowsScanTask(dataFile, table.spec(), List.of(dv)), 0L, 0L, 0L);
    List<IndexCommand> output = process(command);

    assertThat(output)
        .singleElement()
        .satisfies(
            row -> {
              assertThat(row.type()).isEqualTo(IndexCommand.Type.ADD_DATA_ROW);
              assertThat(row.rowPosition().dataFilePath()).isEqualTo(dataFile.location());
              assertThat(row.rowPosition().position()).isEqualTo(1L);
            });
  }

  @TestTemplate
  void readEncryptedEqualityDeleteFile() throws Exception {
    Schema deleteSchema = table.schema().select("id");
    Record delete = GenericRecord.create(deleteSchema);
    DeleteFile eqDeletes =
        FileHelpers.writeDeleteFile(
            table,
            outputFile("eq-deletes"),
            List.of(delete.copy("id", 1L), delete.copy("id", 2L)),
            deleteSchema);

    assertThat(eqDeletes.keyMetadata()).isNotNull();

    List<IndexCommand> output =
        process(ReadCommand.eqDeleteFile(eqDeletes, table.spec(), 0L, 0L, 0L));

    assertThat(output)
        .hasSize(2)
        .allSatisfy(row -> assertThat(row.type()).isEqualTo(IndexCommand.Type.RESOLVE_DELETE));
  }

  private List<IndexCommand> process(ReadCommand command) throws Exception {
    EqualityConvertReader reader =
        new EqualityConvertReader(new StaticTableLoader(table), EQUALITY_FIELD_IDS, false);
    try (OneInputStreamOperatorTestHarness<ReadCommand, IndexCommand> harness =
        ProcessFunctionTestHarnesses.forProcessFunction(reader)) {
      harness.open();
      harness.processElement(command, 0);

      assertThat(harness.getSideOutput(TaskResultAggregator.ERROR_STREAM)).isNullOrEmpty();
      return harness.extractOutputValues();
    }
  }

  private Record record(long id, String data) {
    Record record = GenericRecord.create(table.schema());
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }

  private OutputFile outputFile(String name) {
    return Files.localOutput(new File(tableDir, format.addExtension(name)));
  }

  private static class StaticTableLoader implements TableLoader {
    private final Table table;

    private StaticTableLoader(Table table) {
      this.table = table;
    }

    @Override
    public void open() {}

    @Override
    public boolean isOpen() {
      return true;
    }

    @Override
    public Table loadTable() {
      return table;
    }

    @Override
    @SuppressWarnings({"checkstyle:NoClone", "checkstyle:SuperClone"})
    public TableLoader clone() {
      return this;
    }

    @Override
    public void close() {}
  }
}
