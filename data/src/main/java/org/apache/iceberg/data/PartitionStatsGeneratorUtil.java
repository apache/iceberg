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
package org.apache.iceberg.data;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatisticsFile;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.encryption.EncryptedFiles;
import org.apache.iceberg.encryption.EncryptionKeyMetadata;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.FileWriterFactory;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;

/**
 * Util to write and read the {@link PartitionStatisticsFile}. Uses generic readers and writes to
 * support writing and reading of the stats in table default format.
 */
public final class PartitionStatsGeneratorUtil {

  private PartitionStatsGeneratorUtil() {}

  /**
   * Creates a new {@link OutputFile} for storing partition statistics for the specified table. The
   * output file extension will be same as table's default format.
   *
   * @param table The {@link Table} for which the partition statistics file is being created.
   * @param snapshotId The ID of the snapshot associated with the partition statistics.
   * @return A new {@link OutputFile} that can be used to write partition statistics.
   */
  public static OutputFile newPartitionStatsFile(Table table, long snapshotId) {
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    return table
        .io()
        .newOutputFile(
            ((HasTableOperations) table)
                .operations()
                .metadataFileLocation(
                    fileFormat.addExtension(String.format("partition-stats-%d", snapshotId))));
  }

  /**
   * Writes partition statistics to the specified {@link OutputFile} for the given table.
   *
   * @param table The {@link Table} for which the partition statistics are being written.
   * @param records An {@link Iterator} of {@link Record} to be written into the file.
   * @param outputFile The {@link OutputFile} where the partition statistics will be written. Should
   *     include the file extension.
   */
  public static void writePartitionStatsFile(
      Table table, Iterator<Record> records, OutputFile outputFile) {
    Schema dataSchema = PartitionStatsUtil.schema(Partitioning.partitionType(table));
    FileFormat fileFormat =
        FileFormat.fromString(
            outputFile.location().substring(outputFile.location().lastIndexOf(".") + 1));
    FileWriterFactory<Record> factory =
        GenericFileWriterFactory.builderFor(table)
            .dataSchema(dataSchema)
            .dataFileFormat(fileFormat)
            .build();
    DataWriter<Record> writer =
        factory.newDataWriter(
            EncryptedFiles.encryptedOutput(outputFile, EncryptionKeyMetadata.EMPTY),
            PartitionSpec.unpartitioned(),
            null);
    try (Closeable toClose = writer) {
      records.forEachRemaining(writer::write);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Reads partition statistics from the specified {@link InputFile} using given schema.
   *
   * @param schema The {@link Schema} of the partition statistics file.
   * @param inputFile An {@link InputFile} pointing to the partition stats file.
   */
  public static CloseableIterable<Record> readPartitionStatsFile(
      Schema schema, InputFile inputFile) {
    // TODO: support other formats or introduce GenericFileReader
    return Parquet.read(inputFile)
        .project(schema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
        .build();
  }
}
