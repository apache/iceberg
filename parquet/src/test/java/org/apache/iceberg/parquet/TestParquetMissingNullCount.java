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
package org.apache.iceberg.parquet;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.ManifestReader;
import org.apache.iceberg.ManifestWriter;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.InclusiveMetricsEvaluator;
import org.apache.iceberg.inmemory.InMemoryFileIO;
import org.apache.iceberg.inmemory.InMemoryOutputFile;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types.IntegerType;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;

/**
 * Tests metrics for files whose column chunk stats omit {@code null_count}, which is allowed by the
 * format. Parquet reports a missing count as -1 from {@link Statistics#getNumNulls()}, so it must
 * not be summed into a total.
 */
public class TestParquetMissingNullCount {

  private static final MessageType PARQUET_SCHEMA =
      Types.buildMessage()
          .addField(
              Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, Type.Repetition.OPTIONAL)
                  .id(1)
                  .named("id"))
          .named("table");

  private static final PrimitiveType ID_TYPE = PARQUET_SCHEMA.getType("id").asPrimitiveType();

  private static final Schema SCHEMA = new Schema(optional(1, "id", IntegerType.get()));
  private static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA).build();

  @Test
  public void missingNullCountInSingleRowGroup() {
    Metrics metrics = metrics(block(statsWithoutNullCount(1, 10), 10));

    assertThat(metrics.nullValueCounts()).doesNotContainKey(1);
    assertThat(metrics.valueCounts()).containsEntry(1, 10L);
    // a missing null count does not invalidate the bounds
    assertThat(metrics.lowerBounds()).containsKey(1);
    assertThat(metrics.upperBounds()).containsKey(1);
  }

  @Test
  public void missingNullCountInOneOfTwoRowGroups() {
    // without accounting for the -1 sentinel, this sums to an incorrect null count of 0
    Metrics metrics = metrics(block(statsWithoutNullCount(1, 10), 10), block(stats(20, 30, 1), 10));

    assertThat(metrics.nullValueCounts()).doesNotContainKey(1);
    assertThat(metrics.valueCounts()).containsEntry(1, 20L);
  }

  @Test
  public void missingNullCountAfterKnownNullCount() {
    // the -1 sentinel must also be detected when it is not the first row group
    Metrics metrics = metrics(block(stats(20, 30, 5), 10), block(statsWithoutNullCount(1, 10), 10));

    assertThat(metrics.nullValueCounts()).doesNotContainKey(1);
  }

  @Test
  public void missingNullCountWithCountsMode() {
    Metrics metrics =
        metrics(
            MetricsConfig.fromProperties(
                Collections.singletonMap("write.metadata.metrics.default", "counts")),
            block(statsWithoutNullCount(1, 10), 10),
            block(stats(20, 30, 1), 10));

    assertThat(metrics.nullValueCounts()).doesNotContainKey(1);
    assertThat(metrics.valueCounts()).containsEntry(1, 20L);
  }

  @Test
  public void knownNullCountsAreSummed() {
    Metrics metrics = metrics(block(stats(1, 10, 2), 10), block(stats(20, 30, 3), 10));

    assertThat(metrics.nullValueCounts()).containsEntry(1, 5L);
    assertThat(metrics.valueCounts()).containsEntry(1, 20L);
  }

  @Test
  public void missingNullCountIsNotWrittenToManifest() throws IOException {
    // ensure the null count is not written into manifest when there's missing
    // stats for a column chunk.
    DataFile file =
        DataFiles.builder(SPEC)
            .withPath("/path/to/data.parquet")
            .withFormat(FileFormat.PARQUET)
            .withFileSizeInBytes(1024)
            .withMetrics(
                metrics(block(statsWithoutNullCount(1, 10), 10), block(stats(20, 30, 1), 10)))
            .build();

    InMemoryOutputFile outputFile = new InMemoryOutputFile("manifest.avro");
    ManifestWriter<DataFile> writer = ManifestFiles.write(2, SPEC, outputFile, 100L);
    try {
      writer.add(file);
    } finally {
      writer.close();
    }

    InMemoryFileIO io = new InMemoryFileIO();
    io.addFile(outputFile.location(), outputFile.toByteArray());

    DataFile read;
    try (ManifestReader<DataFile> reader =
        ManifestFiles.read(writer.toManifestFile(), io, ImmutableMap.of(SPEC.specId(), SPEC))) {
      read = Iterables.getOnlyElement(reader);
    }

    assertThat(read.nullValueCounts()).doesNotContainKey(1);
    assertThat(read.valueCounts()).containsEntry(1, 20L);
    // the file is not pruned from a scan that looks for nulls
    assertThat(new InclusiveMetricsEvaluator(SCHEMA, Expressions.isNull("id")).eval(read)).isTrue();
  }

  private static Statistics<?> statsWithoutNullCount(int min, int max) {
    return Statistics.getBuilderForReading(ID_TYPE)
        .withMin(toLittleEndian(min))
        .withMax(toLittleEndian(max))
        .build();
  }

  private static Statistics<?> stats(int min, int max, long numNulls) {
    return Statistics.getBuilderForReading(ID_TYPE)
        .withMin(toLittleEndian(min))
        .withMax(toLittleEndian(max))
        .withNumNulls(numNulls)
        .build();
  }

  private static byte[] toLittleEndian(int value) {
    return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(value).array();
  }

  private static BlockMetaData block(Statistics<?> stats, long valueCount) {
    ColumnChunkMetaData column =
        ColumnChunkMetaData.get(
            ColumnPath.get("id"),
            ID_TYPE,
            CompressionCodecName.UNCOMPRESSED,
            null /* encodingStats */,
            ImmutableSet.of(Encoding.PLAIN),
            stats,
            4L /* firstDataPage */,
            0L /* dictionaryPageOffset */,
            valueCount,
            100L /* totalSize */,
            100L /* totalUncompressedSize */);

    BlockMetaData block = new BlockMetaData();
    block.setRowCount(valueCount);
    block.setTotalByteSize(100L);
    block.addColumn(column);
    return block;
  }

  private static Metrics metrics(BlockMetaData... blocks) {
    return metrics(MetricsConfig.getDefault(), blocks);
  }

  private static Metrics metrics(MetricsConfig config, BlockMetaData... blocks) {
    List<BlockMetaData> blockList = Lists.newArrayList(blocks);
    FileMetaData fileMetaData =
        new FileMetaData(PARQUET_SCHEMA, Collections.emptyMap(), "test-writer");
    return ParquetUtil.footerMetrics(
        new ParquetMetadata(fileMetaData, blockList), Stream.empty(), config);
  }
}
