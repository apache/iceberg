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
package org.apache.iceberg.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.List;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.DataTestBase;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class TestGenericAvro extends DataTestBase {
  @Override
  protected boolean supportsUnknown() {
    return true;
  }

  @Override
  protected boolean supportsTimestampNanos() {
    return true;
  }

  @Override
  protected boolean supportsVariant() {
    return true;
  }

  @ParameterizedTest(name = "{index} {0}")
  @MethodSource("org.apache.iceberg.avro.RandomAvroData#localTimestampRecords")
  public void testReadLocalTimestampType(Record avroRecord, Schema readSchema, long expectedValue)
      throws IOException {
    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (PositionOutputStream out = outputFile.createOrOverwrite();
        DataFileWriter<Record> writer =
            new DataFileWriter<>(new GenericDatumWriter<>(avroRecord.getSchema()))) {
      writer.create(avroRecord.getSchema(), out);
      writer.append(avroRecord);
    }

    try (AvroIterable<Record> reader =
        Avro.read(outputFile.toInputFile()).project(readSchema).build()) {
      assertThat(Iterables.getOnlyElement(reader).get("ts")).isEqualTo(expectedValue);
    }
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    List<Record> expected = RandomAvroData.generate(schema, 100, 0L);

    OutputFile outputFile = new InMemoryOutputFile();
    try (FileAppender<Record> writer =
        Avro.write(outputFile).schema(schema).named("test").build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (AvroIterable<Record> reader =
        Avro.read(outputFile.toInputFile()).project(schema).build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      AvroTestHelpers.assertEquals(schema.asStruct(), expected.get(i), rows.get(i));
    }
  }
}
