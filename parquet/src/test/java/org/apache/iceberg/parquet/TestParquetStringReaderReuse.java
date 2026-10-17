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
package org.apache.iceberg.parquet;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestParquetStringReaderReuse {

  private static final Schema STRINGS =
      new Schema(Types.NestedField.optional(1, "s", Types.StringType.get()));

  @TempDir private File temp;

  @Test
  public void positionDeletePathsAreDecodedOncePerDictionaryEntry() throws IOException {
    Schema deleteSchema =
        new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS);
    List<String> paths = Lists.newArrayList();
    for (int f = 0; f < 4; f += 1) {
      paths.add("s3://bucket/warehouse/db/table/data/00000-" + f + "-data-file.parquet");
    }

    OutputFile out = Files.localOutput(new File(temp, "pos-deletes.parquet"));
    PositionDeleteWriter<Void> deleteWriter =
        Parquet.writeDeletes(out)
            .createWriterFunc(GenericParquetWriter::create)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .buildPositionWriter();
    PositionDelete<Void> delete = PositionDelete.create();
    try (PositionDeleteWriter<Void> writer = deleteWriter) {
      for (String path : paths) {
        for (long pos = 0; pos < 1000; pos += 1) {
          writer.write(delete.set(path, pos));
        }
      }
    }

    List<Record> read = read(out, deleteSchema);
    assertThat(read).hasSize(4000);
    for (int i = 0; i < read.size(); i += 1) {
      assertThat(read.get(i).getField("file_path")).isEqualTo(paths.get(i / 1000));
      assertThat(read.get(i).getField("pos")).isEqualTo((long) (i % 1000));
      if (i % 1000 != 0) {
        // the same dictionary entry as the previous row: the same String, not a new copy
        assertThat(read.get(i).getField("file_path"))
            .isSameAs(read.get(i - 1).getField("file_path"));
      }
    }
  }

  @Test
  public void plainEncodedStringsAreReadCorrectly() throws IOException {
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < 5000; i += 1) {
      // repeated values in a row, which a plain page still decodes one by one
      values.add(i % 7 == 0 ? null : "value-" + (i / 3));
    }

    List<Record> read = writeAndRead(values, ImmutableMap.of("parquet.enable.dictionary", "false"));
    assertThat(Lists.transform(read, r -> r.getField("s"))).isEqualTo(values);
  }

  @Test
  public void dictionaryFallbackAndRowGroupsAreReadCorrectly() throws IOException {
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < 20000; i += 1) {
      // a few repeating values first, then distinct ones that overflow the dictionary
      values.add(i < 5000 ? "repeat-" + (i % 3) : "distinct-" + i);
    }

    List<Record> read =
        writeAndRead(
            values,
            ImmutableMap.of(
                TableProperties.PARQUET_DICT_SIZE_BYTES, "4096",
                TableProperties.PARQUET_ROW_GROUP_SIZE_BYTES, "65536",
                TableProperties.PARQUET_PAGE_SIZE_BYTES, "8192"));
    assertThat(Lists.transform(read, r -> r.getField("s"))).isEqualTo(values);
  }

  private List<Record> writeAndRead(List<String> values, Map<String, String> properties)
      throws IOException {
    OutputFile out = Files.localOutput(new File(temp, "strings.parquet"));
    GenericRecord template = GenericRecord.create(STRINGS);
    try (FileAppender<Record> appender =
        Parquet.write(out)
            .schema(STRINGS)
            .createWriterFunc(GenericParquetWriter::create)
            .setAll(properties)
            .overwrite()
            .build()) {
      for (String value : values) {
        GenericRecord record = template.copy();
        record.setField("s", value);
        appender.add(record);
      }
    }

    return read(out, STRINGS);
  }

  private static List<Record> read(OutputFile out, Schema schema) throws IOException {
    try (CloseableIterable<Record> reader =
        Parquet.read(out.toInputFile())
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      List<Record> records = Lists.newArrayList();
      for (Record record : reader) {
        records.add(record.copy());
      }

      return records;
    }
  }
}
