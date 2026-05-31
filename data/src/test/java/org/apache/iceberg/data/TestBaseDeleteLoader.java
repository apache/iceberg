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
import org.apache.iceberg.Table;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.Pair;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestBaseDeleteLoader {

  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()),
          required(2, "data", Types.StringType.get()),
          optional(3, "binaryData", Types.BinaryType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  @TempDir private File tableDir;

  @Test
  public void testLoadParquetPositionDeletesWithReadProperties() throws IOException {
    Table table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);

    GenericRecord record = GenericRecord.create(table.schema());
    List<Record> records =
        Lists.newArrayList(
            record.copy("id", 29, "data", "a"),
            record.copy("id", 43, "data", "b"),
            record.copy("id", 89, "data", "d"));

    DataFile dataFile =
        FileHelpers.writeDataFile(
            table, Files.localOutput(new File(tableDir, "data.parquet")), Row.of(0), records);

    List<Pair<CharSequence, Long>> deletes =
        Lists.newArrayList(
            Pair.of(dataFile.location(), 0L), // id = 29
            Pair.of(dataFile.location(), 2L) // id = 89
            );

    Pair<DeleteFile, ?> posDeletes =
        FileHelpers.writeDeleteFile(
            table, Files.localOutput(new File(tableDir, "deletes.parquet")), Row.of(0), deletes, 2);

    DeleteFile deleteFile = posDeletes.first();
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(
            file -> table.io().newInputFile(file.location()),
            ImmutableMap.of(ParquetInputFormat.HADOOP_VECTORED_IO_ENABLED, "false"));

    PositionDeleteIndex index =
        deleteLoader.loadPositionDeletes(List.of(deleteFile), dataFile.location());

    assertThat(index.isDeleted(0)).isTrue();
    assertThat(index.isDeleted(1)).isFalse();
    assertThat(index.isDeleted(2)).isTrue();
  }
}
