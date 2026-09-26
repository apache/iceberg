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
package org.apache.iceberg.index;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

/**
 * Writes a tracking file in Avro format.
 *
 * <p>The tracking file is a metadata file (like an Iceberg manifest) that lists all leaf files
 * belonging to an index snapshot, with transform-value bounds per leaf file used for planning-time
 * pruning.
 *
 * <p>Uses Avro's {@link DataFileWriter} writing to Iceberg's {@link OutputFile}, following the
 * same pattern as Iceberg's own manifest writers.
 */
public class TrackingFileWriter implements AutoCloseable {

  static final Schema AVRO_SCHEMA =
      new Schema.Parser()
          .parse(
              "{"
                  + "\"type\":\"record\","
                  + "\"name\":\"tracking_file_entry\","
                  + "\"namespace\":\"org.apache.iceberg.index\","
                  + "\"fields\":["
                  + "  {\"name\":\"location\",\"type\":\"string\",\"field-id\":100},"
                  + "  {\"name\":\"file_format\",\"type\":\"string\",\"field-id\":101},"
                  + "  {\"name\":\"record_count\",\"type\":\"long\",\"field-id\":103},"
                  + "  {\"name\":\"file_size_in_bytes\",\"type\":\"long\",\"field-id\":104},"
                  + "  {\"name\":\"transform_value_lower_bound\",\"type\":\"long\",\"field-id\":200},"
                  + "  {\"name\":\"transform_value_upper_bound\",\"type\":\"long\",\"field-id\":201},"
                  + "  {\"name\":\"key_metadata\",\"type\":[\"null\",\"bytes\"],\"default\":null,\"field-id\":131}"
                  + "]"
                  + "}");

  private final DataFileWriter<GenericRecord> writer;
  private final OutputStream stream;
  private int entryCount = 0;

  public TrackingFileWriter(OutputFile outputFile) {
    Preconditions.checkNotNull(outputFile, "outputFile is required");
    try {
      this.stream = outputFile.createOrOverwrite();
      DataFileWriter<GenericRecord> dfw =
          new DataFileWriter<GenericRecord>(new GenericDatumWriter<GenericRecord>(AVRO_SCHEMA))
              .setCodec(CodecFactory.snappyCodec());
      dfw.create(AVRO_SCHEMA, stream);
      this.writer = dfw;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to create tracking file writer", e);
    }
  }

  /** Write a single tracking file entry. */
  public void add(TrackingFileEntry entry) {
    Preconditions.checkNotNull(entry, "entry is required");
    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
    record.put("location", entry.location());
    record.put("file_format", entry.fileFormat());
    record.put("record_count", entry.recordCount());
    record.put("file_size_in_bytes", entry.fileSizeInBytes());
    record.put("transform_value_lower_bound", entry.transformValueLowerBound());
    record.put("transform_value_upper_bound", entry.transformValueUpperBound());
    record.put(
        "key_metadata",
        entry.keyMetadata() != null ? entry.keyMetadata() : null);
    try {
      writer.append(record);
      entryCount++;
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to write tracking file entry", e);
    }
  }

  /** Write all entries from a list. */
  public void addAll(List<TrackingFileEntry> entries) {
    entries.forEach(this::add);
  }

  public int entryCount() {
    return entryCount;
  }

  @Override
  public void close() {
    try {
      writer.close();
    } catch (IOException e) {
      throw new UncheckedIOException("Failed to close tracking file writer", e);
    }
  }
}
