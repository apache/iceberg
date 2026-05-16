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

import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX;
import static org.apache.iceberg.TableProperties.PARQUET_BLOOM_FILTER_MAX_BYTES;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for the adaptive bloom filter sizing TBLPROPERTY ({@code
 * write.parquet.bloom-filter-adaptive-enabled}).
 *
 * <p>Without adaptive sizing, parquet-mr's {@code ColumnValueCollector.initBloomFilter} allocates a
 * fixed {@code bloom-filter-max-bytes} buffer per bloom-enabled column regardless of actual NDV
 * (the {@code !ndv.isPresent() && !adaptive} branch calls {@code new BlockSplitBloomFilter(
 * maxBloomFilterSize, maxBloomFilterSize)}). For low-row-count writes this produces a file
 * dominated by an empty bloom filter buffer. The adaptive flag (PARQUET-2326) makes parquet-mr pick
 * the smallest of N candidate filter sizes that satisfies actual NDV at FPP.
 *
 * <p>These tests use the createWriterFunc code path because that is the path Iceberg uses for Spark
 * and Flink data writes; the legacy ParquetWriteBuilder path goes through parquet-mr directly
 * without consulting Iceberg-specific properties.
 */
public class TestParquetAdaptiveBloomFilter {

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.LongType.get()));

  @TempDir private Path temp;

  /**
   * Writes a parquet file via the createWriterFunc code path with bloom filter enabled on the
   * {@code id} column and a 4 MiB max-bytes cap. Returns the resulting file size.
   */
  private long writeAndMeasure(boolean adaptive) throws IOException {
    File file = new File(temp.toFile(), "test_" + adaptive + "_" + System.nanoTime() + ".parquet");
    OutputFile outFile = Files.localOutput(file);

    Parquet.WriteBuilder writer =
        Parquet.write(outFile)
            .schema(SCHEMA)
            .createWriterFunc(ParquetAvroWriter::buildWriter)
            .set(PARQUET_BLOOM_FILTER_COLUMN_ENABLED_PREFIX + "id", "true")
            .set(PARQUET_BLOOM_FILTER_MAX_BYTES, "4194304");

    if (adaptive) {
      writer = writer.set(PARQUET_BLOOM_FILTER_ADAPTIVE_ENABLED, "true");
    }

    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(SCHEMA.asStruct());
    try (FileAppender<GenericData.Record> appender = writer.build()) {
      for (int i = 0; i < 5; i++) {
        appender.add(new GenericRecordBuilder(avroSchema).set("id", (long) i).build());
      }
    }

    return file.length();
  }

  @Test
  public void testAdaptiveSizingShrinksFile() throws IOException {
    // Without adaptive, parquet-mr writes the full bloom-filter-max-bytes buffer (~4 MiB).
    long sizeWithoutAdaptive = writeAndMeasure(false);

    // With adaptive, parquet-mr picks the smallest of N candidate filter sizes that fits actual
    // NDV (5 values). The exact candidate depends on parquet-mr's sizing strategy; for very low
    // NDV the smallest candidate is selected.
    long sizeWithAdaptive = writeAndMeasure(true);

    assertThat(sizeWithoutAdaptive)
        .as("non-adaptive file should pad bloom buffer close to bloom-filter-max-bytes (~4 MiB)")
        .isGreaterThan(3_500_000L);
    assertThat(sizeWithAdaptive)
        .as("adaptive file should be at least 2x smaller than the non-adaptive file")
        .isLessThan(sizeWithoutAdaptive / 2);
  }

  @Test
  public void testAdaptiveDisabledByDefault() throws IOException {
    // When the property is not set, behavior should match the legacy (non-adaptive) write path
    // — i.e., the full max-bytes buffer is allocated. This guards backward compatibility.
    long size = writeAndMeasure(false);

    assertThat(size)
        .as("default behavior (no adaptive property) should pad to bloom-filter-max-bytes")
        .isGreaterThan(3_500_000L);
  }
}
