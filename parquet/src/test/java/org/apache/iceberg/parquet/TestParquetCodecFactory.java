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

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.jupiter.api.Test;

final class TestParquetCodecFactory {

  private static final int PAGE_SIZE = 1024 * 1024;

  @Test
  void testCachesCodecsByLevelWithoutHadoopConfiguration() {
    PlainParquetConfiguration level3 = new PlainParquetConfiguration();
    level3.set("parquet.compression.codec.zstd.level", "3");
    PlainParquetConfiguration level5 = new PlainParquetConfiguration();
    level5.set("parquet.compression.codec.zstd.level", "5");

    CompressionCodec codecAtLevel3 = codec(level3, CompressionCodecName.ZSTD);
    CompressionCodec codecAtLevel5 = codec(level5, CompressionCodecName.ZSTD);

    assertThat(codecAtLevel3).isNotNull();
    assertThat(codecAtLevel5).isNotNull().isNotSameAs(codecAtLevel3);
    assertThat(codec(level3, CompressionCodecName.ZSTD)).isSameAs(codecAtLevel3);
  }

  @Test
  void testLegacyZstdLevelPropertyIsPartOfTheCacheKey() {
    PlainParquetConfiguration legacy = new PlainParquetConfiguration();
    legacy.set("io.compression.codec.zstd.level", "7");
    PlainParquetConfiguration current = new PlainParquetConfiguration();
    current.set("parquet.compression.codec.zstd.level", "7");

    assertThat(codec(legacy, CompressionCodecName.ZSTD))
        .isSameAs(codec(current, CompressionCodecName.ZSTD));
  }

  @Test
  @SuppressWarnings("deprecation")
  void testHadoopConfigurationStillAccepted() {
    Configuration conf = new Configuration(false);
    conf.set("zlib.compress.level", "BEST_COMPRESSION");
    ParquetCodecFactory factory = new ParquetCodecFactory(conf, PAGE_SIZE);

    assertThat(factory.getCompressor(CompressionCodecName.GZIP)).isNotNull();
    assertThat(factory.getCodec(CompressionCodecName.UNCOMPRESSED)).isNull();
  }

  private static CompressionCodec codec(ParquetConfiguration conf, CompressionCodecName name) {
    return new ParquetCodecFactory(conf, PAGE_SIZE).getCodec(name);
  }
}
