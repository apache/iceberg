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
package org.apache.iceberg;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.IOException;
import java.util.Locale;
import java.util.UUID;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class PartitionStatistics {

  /**
   * Reads partition statistics from the specified {@link InputFile} using given schema.
   *
   * @param schema The {@link Schema} of the partition statistics file.
   * @param inputFile An {@link InputFile} pointing to the partition stats file.
   */
  public CloseableIterable<PartitionStats> read(Schema schema, InputFile inputFile) {
    Preconditions.checkArgument(schema != null, "Invalid schema: null");
    Preconditions.checkArgument(inputFile != null, "Invalid input file: null");

    FileFormat fileFormat = FileFormat.fromFileName(inputFile.location());
    Preconditions.checkArgument(
        fileFormat != null, "Unable to determine format of file: %s", inputFile.location());

    CloseableIterable<StructLike> records =
        InternalData.read(fileFormat, inputFile).project(schema).build();
    return CloseableIterable.transform(records, PartitionStatsHandler::recordToPartitionStats);
  }

  /**
   * Writes partition statistics using given schema.
   *
   * @param table The {@link Table} for which the partition statistics is computed.
   * @param schema The {@link Schema} of the partition statistics file.
   * @param records The records to write to the partition stats file.
   */
  public PartitionStatisticsFile write(
      Table table, long snapshotId, Schema schema, Iterable<PartitionStats> records)
      throws IOException {
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));

    OutputFile outputFile = newPartitionStatsFile(table, fileFormat, snapshotId);

    try (FileAppender<StructLike> writer =
        InternalData.write(fileFormat, outputFile).schema(schema).build()) {
      records.iterator().forEachRemaining(writer::add);
    }

    return ImmutableGenericPartitionStatisticsFile.builder()
        .snapshotId(snapshotId)
        .path(outputFile.location())
        .fileSizeInBytes(outputFile.toInputFile().getLength())
        .build();
  }

  private static OutputFile newPartitionStatsFile(
      Table table, FileFormat fileFormat, long snapshotId) {
    Preconditions.checkArgument(
        table instanceof HasTableOperations,
        "Table must have operations to retrieve metadata location");

    return table
        .io()
        .newOutputFile(
            ((HasTableOperations) table)
                .operations()
                .metadataFileLocation(
                    fileFormat.addExtension(
                        String.format(
                            Locale.ROOT, "partition-stats-%d-%s", snapshotId, UUID.randomUUID()))));
  }
}
