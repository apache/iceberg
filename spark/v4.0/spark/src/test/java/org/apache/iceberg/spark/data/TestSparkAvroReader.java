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
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;

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

  @Test
  public void testReadLocalTimestampType() throws IOException {
    org.apache.avro.Schema avroSchema =
        org.apache.avro.Schema.createRecord(
            "r1",
            null,
            null,
            false,
            List.of(
                localTimestampField("ts_millis", 1, LogicalTypes.localTimestampMillis()),
                localTimestampField("ts_micros", 2, LogicalTypes.localTimestampMicros())));

    Schema readSchema =
        new Schema(
            Types.NestedField.optional(1, "ts_millis", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(2, "ts_micros", Types.TimestampType.withoutZone()));

    long millisValue = 1_000L;
    long microsValue = 2_000_000L;

    GenericData.Record record = new GenericData.Record(avroSchema);
    record.put("ts_millis", millisValue);
    record.put("ts_micros", microsValue);

    InMemoryOutputFile outputFile = new InMemoryOutputFile();
    try (DataFileWriter<GenericData.Record> writer =
        new DataFileWriter<>(new GenericDatumWriter<>(avroSchema))) {
      writer.create(avroSchema, outputFile.createOrOverwrite());
      writer.append(record);
    }

    try (AvroIterable<InternalRow> reader =
        Avro.read(outputFile.toInputFile())
            .project(readSchema)
            .createResolvingReader(schema -> SparkPlannedAvroReader.create(schema, Map.of()))
            .build()) {
      InternalRow row = Iterables.getOnlyElement(reader);
      assertThat(row.getLong(0)).isEqualTo(millisValue * 1_000L);
      assertThat(row.getLong(1)).isEqualTo(microsValue);
    }
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Override
  protected boolean supportsRowLineage() {
    return true;
  }

  private static org.apache.avro.Schema.Field localTimestampField(
      String name, int id, LogicalType logicalType) {
    org.apache.avro.Schema type =
        logicalType.addToSchema(org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG));
    org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(name, type);
    field.addProp(AvroSchemaUtil.FIELD_ID_PROP, id);
    return field;
  }
}
