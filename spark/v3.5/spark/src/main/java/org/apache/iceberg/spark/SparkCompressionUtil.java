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

  private static final String SHUFFLE_COMPRESSION_ENABLED = "spark.shuffle.compress";
  private static final boolean SHUFFLE_COMPRESSION_ENABLED_DEFAULT = true;

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
    return shuffleCompressionEnabled(sparkConf) ? sparkCodec(sparkConf) : "none";
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

    compressions.put(Pair.of("none", "zstd"), 4.0);
    compressions.put(Pair.of("none", "gzip"), 4.0);
    compressions.put(Pair.of("none", "zlib"), 4.0);
    compressions.put(Pair.of("none", "snappy"), 3.0);
    compressions.put(Pair.of("none", "lz4"), 3.0);

    compressions.put(Pair.of("zstd", "zstd"), 2.0);
    compressions.put(Pair.of("zstd", "gzip"), 2.0);
    compressions.put(Pair.of("zstd", "zlib"), 2.0);
    compressions.put(Pair.of("zstd", "snappy"), 1.5);
    compressions.put(Pair.of("zstd", "lz4"), 1.5);

    compressions.put(Pair.of("snappy", "zstd"), 3.0);
    compressions.put(Pair.of("snappy", "gzip"), 3.0);
    compressions.put(Pair.of("snappy", "zlib"), 3.0);
    compressions.put(Pair.of("snappy", "snappy"), 2.0);
    compressions.put(Pair.of("snappy", "lz4"), 2.);

    compressions.put(Pair.of("lz4", "zstd"), 3.0);
    compressions.put(Pair.of("lz4", "gzip"), 3.0);
    compressions.put(Pair.of("lz4", "zlib"), 3.0);
    compressions.put(Pair.of("lz4", "snappy"), 2.0);
    compressions.put(Pair.of("lz4", "lz4"), 2.0);

    return compressions;
  }

  private static Map<Pair<String, String>, Double> initRowBasedCompressions() {
    Map<Pair<String, String>, Double> compressions = Maps.newHashMap();

    compressions.put(Pair.of("none", "zstd"), 2.0);
    compressions.put(Pair.of("none", "gzip"), 2.0);

    compressions.put(Pair.of("lz4", "zstd"), 1.5);
    compressions.put(Pair.of("lz4", "gzip"), 1.5);

    return compressions;
  }
}
