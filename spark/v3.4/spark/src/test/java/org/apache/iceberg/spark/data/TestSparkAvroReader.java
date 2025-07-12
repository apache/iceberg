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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;

public class TestSparkAvroReader extends AvroDataTestBase {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(
      Schema writeSchema, Schema expectedSchema, List<org.apache.iceberg.data.Record> records)
      throws IOException {
    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (DataWriter<Record> dataWriter =
        Avro.writeData(Files.localOutput(testFile))
            .schema(writeSchema)
            .createWriterFunc(org.apache.iceberg.data.avro.DataWriter::create)
            .withSpec(PartitionSpec.unpartitioned())
            .build()) {
      for (org.apache.iceberg.data.Record rec : records) {
        dataWriter.write(rec);
      }
    }

    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader =
        Avro.read(Files.localInput(testFile))
            .createResolvingReader(schema -> SparkPlannedAvroReader.create(schema, ID_TO_CONSTANT))
            .project(expectedSchema)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < records.size(); i += 1) {
      GenericsHelpers.assertEqualsUnsafe(
          expectedSchema.asStruct(), records.get(i), rows.get(i), ID_TO_CONSTANT, i);
    }
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> expected = RandomGenericData.generate(writeSchema, 100, 0L);
    writeAndValidate(writeSchema, expectedSchema, expected);
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
  }
}
