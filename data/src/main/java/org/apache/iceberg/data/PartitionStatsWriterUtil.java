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
import java.util.List;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionStatsUtil;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
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
import org.apache.iceberg.types.Types;

public final class PartitionStatsWriterUtil {

  private PartitionStatsWriterUtil() {}

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

  public static void writePartitionStatsFile(
      Table table, Iterator<Record> records, OutputFile outputFile) {
    Schema dataSchema = PartitionStatsUtil.schema(Partitioning.partitionType(table));
    FileFormat fileFormat =
        FileFormat.fromString(
            table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT));
    FileWriterFactory<Record> factory =
        GenericFileWriterFactory.builderFor(table)
            .dataSchema(dataSchema)
            .dataFileFormat(fileFormat)
            .dataSortOrder(sortOrder(dataSchema))
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

  public static CloseableIterable<Record> readPartitionStatsFile(
      Schema schema, InputFile inputFile) {
    // TODO: support other formats or introduce GenericFileReader
    return Parquet.read(inputFile)
        .project(schema)
        .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
        .build();
  }

  private static SortOrder sortOrder(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    List<Types.NestedField> partitionFields =
        ((Types.StructType) schema.asStruct().fields().get(0).type()).fields();
    partitionFields.forEach(
        field -> builder.asc(PartitionStatsUtil.Column.PARTITION_DATA.name() + "." + field.name()));
    return builder.build();
  }
}
