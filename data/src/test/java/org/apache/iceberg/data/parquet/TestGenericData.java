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
package org.apache.iceberg.data.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;

public class TestGenericData extends DataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomGenericData.generate(schema, 100, 0L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

    try (FileAppender<Record> appender =
        Parquet.write(Files.localOutput(testFile))
            .schema(schema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .build()) {
      appender.addAll(expected);
    }

    List<Record> rows;
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }

    // test reuseContainers
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      CloseableIterator it = reader.iterator();
      int idx = 0;
      while (it.hasNext()) {
        GenericRecord actualRecord = (GenericRecord) it.next();
        DataTestHelpers.assertEquals(schema.asStruct(), expected.get(idx), actualRecord);
        idx++;
      }
    }
  }

  @Test
  public void testTwoLevelList() throws IOException {
    Schema schema =
        new Schema(
            optional(1, "arraybytes", Types.ListType.ofRequired(3, Types.BinaryType.get())),
            optional(2, "topbytes", Types.BinaryType.get()));
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema.asStruct());

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

    ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
        AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(new Path(testFile.toURI()))
            .withDataModel(GenericData.get())
            .withSchema(avroSchema)
            .config("parquet.avro.add-list-element-records", "true")
            .config("parquet.avro.write-old-list-structure", "true")
            .build();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    List<ByteBuffer> expectedByteList = new ArrayList();
    byte[] expectedByte = {0x00, 0x01};
    ByteBuffer expectedBinary = ByteBuffer.wrap(expectedByte);
    expectedByteList.add(expectedBinary);
    recordBuilder.set("arraybytes", expectedByteList);
    recordBuilder.set("topbytes", expectedBinary);
    GenericData.Record expectedRecord = recordBuilder.build();

    writer.write(expectedRecord);
    writer.close();

    // test reuseContainers
    try (CloseableIterable<Record> reader =
        Parquet.read(Files.localInput(testFile))
            .project(schema)
            .reuseContainers()
            .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
            .build()) {
      CloseableIterator it = reader.iterator();
      assertThat(it.hasNext()).isTrue();
      while (it.hasNext()) {
        GenericRecord actualRecord = (GenericRecord) it.next();
        assertThat(actualRecord.get(0, ArrayList.class)).first().isEqualTo(expectedBinary);
        assertThat(actualRecord.get(1, ByteBuffer.class)).isEqualTo(expectedBinary);
        assertThat(it.hasNext()).isFalse();
      }
    }
  }
}
