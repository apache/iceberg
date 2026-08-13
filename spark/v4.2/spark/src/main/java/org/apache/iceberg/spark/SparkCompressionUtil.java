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

import java.util.Locale;
import java.util.Map;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.util.Pair;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

class SparkCompressionUtil {

  private static final String LZ4 = "lz4";
  private static final String ZSTD = "zstd";
  private static final String GZIP = "gzip";
  private static final String ZLIB = "zlib";
  private static final String SNAPPY = "snappy";
  private static final String NONE = "none";

  // an internal Spark config that controls whether shuffle data is compressed
  private static final String SHUFFLE_COMPRESSION_ENABLED = "spark.shuffle.compress";
  private static final boolean SHUFFLE_COMPRESSION_ENABLED_DEFAULT = true;

  // an internal Spark config that controls what compression codec is used
  private static final String SPARK_COMPRESSION_CODEC = "spark.io.compression.codec";
  private static final String SPARK_COMPRESSION_CODEC_DEFAULT = "lz4";

  private static final double DEFAULT_COLUMNAR_COMPRESSION = 2;
  private static final Map<Pair<String, String>, Double> COLUMNAR_COMPRESSIONS =
      initColumnarCompressions();

  private static final double DEFAULT_ROW_BASED_COMPRESSION = 1;
  private static final Map<Pair<String, String>, Double> ROW_BASED_COMPRESSIONS =
      initRowBasedCompressions();

  private SparkCompressionUtil() {}

  /**
   * Estimates how much the data in shuffle map files will compress once it is written to disk using
   * a particular file format and codec.
   */
  public static double shuffleCompressionRatio(
      SparkSession spark, FileFormat outputFileFormat, String outputCodec) {
    if (outputFileFormat == FileFormat.ORC || outputFileFormat == FileFormat.PARQUET) {
      return columnarCompression(shuffleCodec(spark), outputCodec);
    } else if (outputFileFormat == FileFormat.AVRO) {
      return rowBasedCompression(shuffleCodec(spark), outputCodec);
    } else {
      return 1.0;
    }
  }

  private static String shuffleCodec(SparkSession spark) {
    SparkConf sparkConf = spark.sparkContext().conf();
    return shuffleCompressionEnabled(sparkConf) ? sparkCodec(sparkConf) : NONE;
  }

  private static boolean shuffleCompressionEnabled(SparkConf sparkConf) {
    return sparkConf.getBoolean(SHUFFLE_COMPRESSION_ENABLED, SHUFFLE_COMPRESSION_ENABLED_DEFAULT);
  }

  private static String sparkCodec(SparkConf sparkConf) {
    return sparkConf.get(SPARK_COMPRESSION_CODEC, SPARK_COMPRESSION_CODEC_DEFAULT);
  }

  private static double columnarCompression(String shuffleCodec, String outputCodec) {
    Pair<String, String> key = Pair.of(normalize(shuffleCodec), normalize(outputCodec));
    return COLUMNAR_COMPRESSIONS.getOrDefault(key, DEFAULT_COLUMNAR_COMPRESSION);
  }

  private static double rowBasedCompression(String shuffleCodec, String outputCodec) {
    Pair<String, String> key = Pair.of(normalize(shuffleCodec), normalize(outputCodec));
    return ROW_BASED_COMPRESSIONS.getOrDefault(key, DEFAULT_ROW_BASED_COMPRESSION);
  }

  private static String normalize(String value) {
    return value != null ? value.toLowerCase(Locale.ROOT) : null;
  }

  private static Map<Pair<String, String>, Double> initColumnarCompressions() {
    Map<Pair<String, String>, Double> compressions = Maps.newHashMap();

    compressions.put(Pair.of(NONE, ZSTD), 4.0);
    compressions.put(Pair.of(NONE, GZIP), 4.0);
    compressions.put(Pair.of(NONE, ZLIB), 4.0);
    compressions.put(Pair.of(NONE, SNAPPY), 3.0);
    compressions.put(Pair.of(NONE, LZ4), 3.0);

    compressions.put(Pair.of(ZSTD, ZSTD), 2.0);
    compressions.put(Pair.of(ZSTD, GZIP), 2.0);
    compressions.put(Pair.of(ZSTD, ZLIB), 2.0);
    compressions.put(Pair.of(ZSTD, SNAPPY), 1.5);
    compressions.put(Pair.of(ZSTD, LZ4), 1.5);

    compressions.put(Pair.of(SNAPPY, ZSTD), 3.0);
    compressions.put(Pair.of(SNAPPY, GZIP), 3.0);
    compressions.put(Pair.of(SNAPPY, ZLIB), 3.0);
    compressions.put(Pair.of(SNAPPY, SNAPPY), 2.0);
    compressions.put(Pair.of(SNAPPY, LZ4), 2.);

    compressions.put(Pair.of(LZ4, ZSTD), 3.0);
    compressions.put(Pair.of(LZ4, GZIP), 3.0);
    compressions.put(Pair.of(LZ4, ZLIB), 3.0);
    compressions.put(Pair.of(LZ4, SNAPPY), 2.0);
    compressions.put(Pair.of(LZ4, LZ4), 2.0);

    return compressions;
  }

  private static Map<Pair<String, String>, Double> initRowBasedCompressions() {
    Map<Pair<String, String>, Double> compressions = Maps.newHashMap();

    compressions.put(Pair.of(NONE, ZSTD), 2.0);
    compressions.put(Pair.of(NONE, GZIP), 2.0);
    compressions.put(Pair.of(NONE, ZLIB), 2.0);

    compressions.put(Pair.of(ZSTD, SNAPPY), 0.5);
    compressions.put(Pair.of(ZSTD, LZ4), 0.5);

    compressions.put(Pair.of(SNAPPY, ZSTD), 1.5);
    compressions.put(Pair.of(SNAPPY, GZIP), 1.5);
    compressions.put(Pair.of(SNAPPY, ZLIB), 1.5);

    compressions.put(Pair.of(LZ4, ZSTD), 1.5);
    compressions.put(Pair.of(LZ4, GZIP), 1.5);
    compressions.put(Pair.of(LZ4, ZLIB), 1.5);

    return compressions;
  }
}
