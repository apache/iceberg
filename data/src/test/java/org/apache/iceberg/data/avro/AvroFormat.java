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
package org.apache.iceberg.data.avro;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.avro.AvroTestHelpers;
import org.apache.iceberg.data.FileFormatTestSupport;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;

public class AvroFormat implements FileFormatTestSupport {
  @Override
  public FileFormat format() {
    return FileFormat.AVRO;
  }

  @Override
  public void writeRecordsWithoutFieldIds(
      OutputFile outputFile, Schema schema, List<Record> records) throws IOException {
    org.apache.avro.Schema avroSchemaWithoutIds = AvroTestHelpers.removeIds(schema);
    DatumWriter<GenericData.Record> datumWriter = new GenericDatumWriter<>(avroSchemaWithoutIds);
    try (OutputStream out = outputFile.create();
        DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(datumWriter)) {
      writer.create(avroSchemaWithoutIds, out);
      for (Record record : records) {
        GenericData.Record avroRecord = new GenericData.Record(avroSchemaWithoutIds);
        for (Types.NestedField field : schema.columns()) {
          avroRecord.put(field.name(), record.getField(field.name()));
        }

        writer.append(avroRecord);
      }
    }

    try (DataFileStream<GenericData.Record> reader =
        new DataFileStream<>(outputFile.toInputFile().newStream(), new GenericDatumReader<>())) {
      assertThat(AvroTestHelpers.hasIds(reader.getSchema())).isFalse();
    }
  }

  @Override
  public Map<String, String> testPropertiesToSet() {
    // Avro currently has only two configurable writer properties: compression codec and
    // compression level.
    return Map.of(
        TableProperties.AVRO_COMPRESSION_LEVEL, "1", TableProperties.AVRO_COMPRESSION, "snappy");
  }

  @Override
  public boolean checkTestProperties(InputFile inputFile) throws IOException {
    // Only the compression codec round-trips into the file metadata; the compression level is not
    // persisted and cannot be read back.
    return "snappy".equals(metadataValue(inputFile, "avro.codec"));
  }

  @Override
  public String metadataValue(InputFile inputFile, String key) throws IOException {
    try (DataFileStream<GenericData.Record> reader =
        new DataFileStream<>(inputFile.newStream(), new GenericDatumReader<>())) {
      return reader.getMetaString(key);
    }
  }

  @Override
  public String splitSizeProperty() {
    throw new UnsupportedOperationException(
        "No split size property defined for format: " + format());
  }
}
