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
package org.apache.iceberg.orc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.orc.GenericOrcReader;
import org.apache.iceberg.data.orc.GenericOrcWriter;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestOrcStructWithoutProjectedFields {
  private static final Types.NestedField ID =
      Types.NestedField.required(1, "id", Types.LongType.get());

  @TempDir private File temp;

  @Test
  public void testStructWithoutProjectedFields() throws IOException {
    Types.StructType writeStruct =
        Types.StructType.of(Types.NestedField.optional(3, "a", Types.IntegerType.get()));
    Schema writeSchema = new Schema(ID, Types.NestedField.optional(2, "s", writeStruct));

    Record present = GenericRecord.create(writeSchema);
    present.setField("id", 1L);
    present.setField("s", GenericRecord.create(writeStruct).copy("a", 10));
    Record nullStruct = GenericRecord.create(writeSchema);
    nullStruct.setField("id", 2L);

    Types.StructType readStruct = Types.StructType.of();
    Schema readSchema = new Schema(ID, Types.NestedField.optional(2, "s", readStruct));

    List<Record> rows = writeAndRead(writeSchema, readSchema, List.of(present, nullStruct));

    assertThat(rows).hasSize(2);
    assertThat(rows.get(0).getField("s")).isEqualTo(GenericRecord.create(readStruct));
    assertThat(rows.get(1).getField("s")).isNull();
  }

  @Test
  public void testListElementStructWithoutProjectedFields() throws IOException {
    Types.StructType writeElement =
        Types.StructType.of(Types.NestedField.optional(4, "a", Types.IntegerType.get()));
    Schema writeSchema =
        new Schema(
            ID, Types.NestedField.optional(2, "arr", Types.ListType.ofOptional(3, writeElement)));

    Record withElements = GenericRecord.create(writeSchema);
    withElements.setField("id", 1L);
    withElements.setField(
        "arr", Lists.newArrayList(GenericRecord.create(writeElement).copy("a", 10), null));
    Record nullList = GenericRecord.create(writeSchema);
    nullList.setField("id", 2L);
    Record emptyList = GenericRecord.create(writeSchema);
    emptyList.setField("id", 3L);
    emptyList.setField("arr", Lists.newArrayList());

    Types.StructType readElement = Types.StructType.of();
    Schema readSchema =
        new Schema(
            ID, Types.NestedField.optional(2, "arr", Types.ListType.ofOptional(3, readElement)));

    List<Record> rows =
        writeAndRead(writeSchema, readSchema, List.of(withElements, nullList, emptyList));

    assertThat(rows).hasSize(3);
    assertThat(rows.get(0).getField("arr"))
        .isEqualTo(Lists.newArrayList(GenericRecord.create(readElement), null));
    assertThat(rows.get(1).getField("arr")).isNull();
    assertThat((List<?>) rows.get(2).getField("arr")).isEmpty();
  }

  @Test
  public void testMapValueStructWithoutProjectedFields() throws IOException {
    Types.StructType writeValue =
        Types.StructType.of(Types.NestedField.optional(5, "a", Types.IntegerType.get()));
    Schema writeSchema =
        new Schema(
            ID,
            Types.NestedField.optional(
                2, "map", Types.MapType.ofOptional(3, 4, Types.StringType.get(), writeValue)));

    Map<String, Record> writtenMap = Maps.newLinkedHashMap();
    writtenMap.put("k1", GenericRecord.create(writeValue).copy("a", 10));
    writtenMap.put("k2", null);
    Record row = GenericRecord.create(writeSchema);
    row.setField("id", 1L);
    row.setField("map", writtenMap);

    Types.StructType readValue = Types.StructType.of();
    Schema readSchema =
        new Schema(
            ID,
            Types.NestedField.optional(
                2, "map", Types.MapType.ofOptional(3, 4, Types.StringType.get(), readValue)));

    List<Record> rows = writeAndRead(writeSchema, readSchema, List.of(row));

    Map<String, Record> expectedMap = Maps.newHashMap();
    expectedMap.put("k1", GenericRecord.create(readValue));
    expectedMap.put("k2", null);
    assertThat(rows).hasSize(1);
    assertThat(rows.get(0).getField("map")).isEqualTo(expectedMap);
  }

  private List<Record> writeAndRead(Schema writeSchema, Schema readSchema, List<Record> records)
      throws IOException {
    OutputFile out = Files.localOutput(new File(temp, "test.orc"));
    try (FileAppender<Record> appender =
        ORC.write(out)
            .schema(writeSchema)
            .createWriterFunc(GenericOrcWriter::buildWriter)
            .build()) {
      appender.addAll(records);
    }

    try (CloseableIterable<Record> reader =
        ORC.read(out.toInputFile())
            .project(readSchema)
            .createReaderFunc(fileSchema -> GenericOrcReader.buildReader(readSchema, fileSchema))
            .build()) {
      return Lists.newArrayList(reader);
    }
  }
}
