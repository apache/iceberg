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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionEntry;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetAvroValueReaders;
import org.apache.iceberg.parquet.ParquetAvroWriter;
import org.apache.iceberg.types.Types;

public final class PartitionStatsUtil {

  private PartitionStatsUtil() {}

  private static final String PARQUET_SUFFIX = ".parquet";

  public static OutputFile newPartitionStatsFile(
      TableOperations ops, long snapshotId, FileFormat format) {
    // TODO: UUID is temp, remove it.
    return ops.io()
        .newOutputFile(
            ops.metadataFileLocation(
                format.addExtension(
                    String.format("partition-stats-%s-%d", UUID.randomUUID(), snapshotId))));
  }

  public static void writePartitionStatsFile(
      Iterator<PartitionEntry> partitions, OutputFile outputFile, Collection<PartitionSpec> specs) {
    validateFormat(outputFile.location());
    writeAsParquetFile(
        PartitionEntry.icebergSchema(Partitioning.partitionType(specs)), partitions, outputFile);
  }

  private static void validateFormat(String filePath) {
    if (!filePath.toLowerCase().endsWith(PARQUET_SUFFIX)) {
      throw new UnsupportedOperationException("Unsupported format : " + filePath);
    }
  }

  public static CloseableIterable<PartitionEntry> readPartitionStatsFile(
      Schema schema, InputFile inputFile) {
    validateFormat(inputFile.location());
    // schema of partition column during read could be different from
    // what is used for writing due to partition evolution.
    // While reading, ParquetAvroValueReaders fills the data as per latest schema.
    CloseableIterable<GenericData.Record> records =
        Parquet.read(inputFile)
            .project(schema)
            .createReaderFunc(fileSchema -> ParquetAvroValueReaders.buildReader(schema, fileSchema))
            .build();

    return CloseableIterable.transform(records, record -> toPartition(schema, record));
  }

  private static PartitionEntry toPartition(Schema schema, GenericData.Record record) {
    PartitionEntry partition = PartitionEntry.builder().newInstance();
    partition.put(
        PartitionEntry.Column.PARTITION_DATA.ordinal(),
        extractPartitionDataFromRecord(schema, record));

    int recordCount = record.getSchema().getFields().size();
    for (int columnIndex = 1; columnIndex < recordCount; columnIndex++) {
      partition.put(columnIndex, record.get(columnIndex));
    }

    return partition;
  }

  private static PartitionData extractPartitionDataFromRecord(
      Schema schema, GenericData.Record record) {
    int partitionDataCount =
        record
            .getSchema()
            .getField(PartitionEntry.Column.PARTITION_DATA.name())
            .schema()
            .getFields()
            .size();
    PartitionData partitionData =
        new PartitionData(
            (Types.StructType)
                schema.findField(PartitionEntry.Column.PARTITION_DATA.name()).type());
    for (int partitionColIndex = 0; partitionColIndex < partitionDataCount; partitionColIndex++) {
      partitionData.set(
          partitionColIndex,
          ((GenericData.Record) record.get(PartitionEntry.Column.PARTITION_DATA.ordinal()))
              .get(partitionColIndex));
    }

    return partitionData;
  }

  private static void writeAsParquetFile(
      Schema schema, Iterator<PartitionEntry> records, OutputFile outputFile) {
    try (DataWriter<PartitionEntry> dataWriter =
        Parquet.writeData(outputFile)
            .schema(schema)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .overwrite()
            .withSpec(PartitionSpec.unpartitioned())
            .withSortOrder(sortOrder(schema))
            .build()) {
      records.forEachRemaining(dataWriter::write);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static SortOrder sortOrder(Schema schema) {
    SortOrder.Builder builder = SortOrder.builderFor(schema);
    List<Types.NestedField> partitionFields =
        ((Types.StructType) schema.asStruct().fields().get(0).type()).fields();
    partitionFields.forEach(
        field -> builder.asc(PartitionEntry.Column.PARTITION_DATA.name() + "." + field.name()));

    return builder.build();
  }
}
