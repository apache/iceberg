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
package org.apache.iceberg.spark;

import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.METADATA;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.iceberg.FileFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.internal.config.package$;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

public class TestSparkCompressionUtil {

  private SparkSession spark;
  private SparkConf sparkConf;

  @Before
  public void initSpark() {
    this.spark = mock(SparkSession.class);
    this.sparkConf = mock(SparkConf.class);

    SparkContext sparkContext = mock(SparkContext.class);

    when(spark.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.conf()).thenReturn(sparkConf);
  }

  @Test
  public void testParquetCompressionRatios() {
    configureShuffle("lz4", true);

    double ratio1 = shuffleCompressionRatio(PARQUET, "zstd");
    assertThat(ratio1).isEqualTo(3.0);

    double ratio2 = shuffleCompressionRatio(PARQUET, "gzip");
    assertThat(ratio2).isEqualTo(3.0);

    double ratio3 = shuffleCompressionRatio(PARQUET, "snappy");
    assertThat(ratio3).isEqualTo(2.0);
  }

  @Test
  public void testOrcCompressionRatios() {
    configureShuffle("lz4", true);

    double ratio1 = shuffleCompressionRatio(ORC, "zlib");
    assertThat(ratio1).isEqualTo(3.0);

    double ratio2 = shuffleCompressionRatio(ORC, "lz4");
    assertThat(ratio2).isEqualTo(2.0);
  }

  @Test
  public void testAvroCompressionRatios() {
    configureShuffle("lz4", true);

    double ratio1 = shuffleCompressionRatio(AVRO, "gzip");
    assertThat(ratio1).isEqualTo(1.5);

    double ratio2 = shuffleCompressionRatio(AVRO, "zstd");
    assertThat(ratio2).isEqualTo(1.5);
  }

  @Test
  public void testCodecNameNormalization() {
    configureShuffle("zStD", true);
    double ratio = shuffleCompressionRatio(PARQUET, "ZstD");
    assertThat(ratio).isEqualTo(2.0);
  }

  @Test
  public void testUnknownCodecNames() {
    configureShuffle("SOME_SPARK_CODEC", true);

    double ratio1 = shuffleCompressionRatio(PARQUET, "SOME_PARQUET_CODEC");
    assertThat(ratio1).isEqualTo(2.0);

    double ratio2 = shuffleCompressionRatio(ORC, "SOME_ORC_CODEC");
    assertThat(ratio2).isEqualTo(2.0);

    double ratio3 = shuffleCompressionRatio(AVRO, "SOME_AVRO_CODEC");
    assertThat(ratio3).isEqualTo(1.0);
  }

  @Test
  public void testOtherFileFormats() {
    configureShuffle("lz4", true);
    double ratio = shuffleCompressionRatio(METADATA, "zstd");
    assertThat(ratio).isEqualTo(1.0);
  }

  @Test
  public void testNullFileCodec() {
    configureShuffle("lz4", true);

    double ratio1 = shuffleCompressionRatio(PARQUET, null);
    assertThat(ratio1).isEqualTo(2.0);

    double ratio2 = shuffleCompressionRatio(ORC, null);
    assertThat(ratio2).isEqualTo(2.0);

    double ratio3 = shuffleCompressionRatio(AVRO, null);
    assertThat(ratio3).isEqualTo(1.0);
  }

  @Test
  public void testUncompressedShuffles() {
    configureShuffle("zstd", false);

    double ratio1 = shuffleCompressionRatio(PARQUET, "zstd");
    assertThat(ratio1).isEqualTo(4.0);

    double ratio2 = shuffleCompressionRatio(ORC, "zlib");
    assertThat(ratio2).isEqualTo(4.0);

    double ratio3 = shuffleCompressionRatio(AVRO, "gzip");
    assertThat(ratio3).isEqualTo(2.0);
  }

  @Test
  public void testSparkDefaults() {
    assertThat(package$.MODULE$.SHUFFLE_COMPRESS().defaultValueString()).isEqualTo("true");
    assertThat(package$.MODULE$.IO_COMPRESSION_CODEC().defaultValueString()).isEqualTo("lz4");
  }

  private void configureShuffle(String codec, boolean compress) {
    when(sparkConf.getBoolean(eq("spark.shuffle.compress"), anyBoolean())).thenReturn(compress);
    when(sparkConf.get(eq("spark.io.compression.codec"), anyString())).thenReturn(codec);
  }

  private double shuffleCompressionRatio(FileFormat fileFormat, String codec) {
    return SparkCompressionUtil.shuffleCompressionRatio(spark, fileFormat, codec);
  }
}
