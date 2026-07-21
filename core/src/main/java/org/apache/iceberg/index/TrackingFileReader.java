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
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Reads a tracking file written by {@link TrackingFileWriter}.
 *
 * <p>Uses Avro's {@link DataFileStream} for sequential streaming reads from Iceberg's
 * {@link InputFile} — no random access required, which keeps compatibility with object stores
 * where seeking is expensive.
 */
public class TrackingFileReader {

  private TrackingFileReader() {}

  /**
   * Read all entries from a tracking file.
   *
   * @param inputFile the tracking file to read
   * @return list of all tracking file entries
   */
  public static List<TrackingFileEntry> readAll(InputFile inputFile) {
    Preconditions.checkNotNull(inputFile, "inputFile is required");
    List<TrackingFileEntry> entries = Lists.newArrayList();
    try (DataFileStream<GenericRecord> stream =
        new DataFileStream<>(
            inputFile.newStream(), new GenericDatumReader<>(TrackingFileWriter.AVRO_SCHEMA))) {
      while (stream.hasNext()) {
        entries.add(fromRecord(stream.next()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read tracking file: " + inputFile.location(), e);
    }
    return entries;
  }

  /**
   * Read only the entries whose transform-value range overlaps with {@code [queryMin, queryMax]}.
   *
   * <p>This is the primary planning-time operation: given a predicate that maps to a transform
   * value range, return only the leaf files that could contain matching entries.
   *
   * @param inputFile the tracking file to read
   * @param queryMin minimum transform value for the query (inclusive)
   * @param queryMax maximum transform value for the query (inclusive)
   * @return list of entries whose transform-value range overlaps the query range
   */
  public static List<TrackingFileEntry> readMatching(
      InputFile inputFile, long queryMin, long queryMax) {
    Preconditions.checkNotNull(inputFile, "inputFile is required");
    List<TrackingFileEntry> matches = Lists.newArrayList();
    try (DataFileStream<GenericRecord> stream =
        new DataFileStream<>(
            inputFile.newStream(), new GenericDatumReader<>(TrackingFileWriter.AVRO_SCHEMA))) {
      while (stream.hasNext()) {
        TrackingFileEntry entry = fromRecord(stream.next());
        // Overlap check: entry range [lower, upper] overlaps [queryMin, queryMax]
        // iff lower <= queryMax AND upper >= queryMin
        if (entry.transformValueLowerBound() <= queryMax
            && entry.transformValueUpperBound() >= queryMin) {
          matches.add(entry);
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to read tracking file: " + inputFile.location(), e);
    }
    return matches;
  }

  private static TrackingFileEntry fromRecord(GenericRecord record) {
    TrackingFileEntry.Builder builder =
        TrackingFileEntry.builder()
            .location(record.get("location").toString())
            .fileFormat(record.get("file_format").toString())
            .recordCount((Long) record.get("record_count"))
            .fileSizeInBytes((Long) record.get("file_size_in_bytes"))
            .transformValueLowerBound((Long) record.get("transform_value_lower_bound"))
            .transformValueUpperBound((Long) record.get("transform_value_upper_bound"));

    Object keyMeta = record.get("key_metadata");
    if (keyMeta != null) {
      builder.keyMetadata((ByteBuffer) keyMeta);
    }
    return builder.build();
  }
}
