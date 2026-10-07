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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.conf.HadoopParquetConfiguration;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

/**
 * A codec factory that caches codecs by compression level as well as by name, so that requests for
 * the same codec at different levels do not share an instance.
 */
public class ParquetCodecFactory extends CodecFactory {

  /**
   * @deprecated since 1.13.0; this class is internal to {@link Parquet}
   *     and should not be constructed directly.
   */
  @Deprecated
  public ParquetCodecFactory(Configuration configuration, int pageSize) {
    this(new HadoopParquetConfiguration(configuration), pageSize);
  }

  ParquetCodecFactory(ParquetConfiguration configuration, int pageSize) {
    super(configuration, pageSize);
  }

  /**
   * This is copied from {@link CodecFactory} and modified to include the level in the cache key.
   */
  @SuppressWarnings("deprecation")
  @Override
  protected CompressionCodec getCodec(CompressionCodecName codecName) {
    String codecClassName = codecName.getHadoopCompressionCodecClassName();
    if (codecClassName == null) {
      return null;
    }
    String cacheKey = cacheKey(codecName);
    CompressionCodec codec = CODEC_BY_NAME.get(cacheKey);
    if (codec != null) {
      return codec;
    }

    try {
      Class<?> codecClass;
      try {
        codecClass = Class.forName(codecClassName);
      } catch (ClassNotFoundException e) {
        // Try to load the class using the configuration's classloader
        codecClass = conf.getClassByName(codecClassName);
      }
      // Hadoop codecs are Configurable, so instantiation needs a Hadoop Configuration
      codec =
          (CompressionCodec)
              ReflectionUtils.newInstance(
                  codecClass, ConfigurationUtil.createHadoopConfiguration(conf));
      CODEC_BY_NAME.put(cacheKey, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

  @SuppressWarnings("deprecation")
  private String cacheKey(CompressionCodecName codecName) {
    String level = null;
    switch (codecName) {
      case GZIP:
        level = conf.get("zlib.compress.level");
        break;
      case BROTLI:
        level = conf.get("compression.brotli.quality");
        break;
      case ZSTD:
        level = conf.get("parquet.compression.codec.zstd.level");
        if (level == null) {
          // keep "io.compression.codec.zstd.level" for backwards compatibility
          level = conf.get("io.compression.codec.zstd.level");
        }
        break;
      default:
        // compression level is not supported; ignore it
    }
    String codecClass = codecName.getHadoopCompressionCodecClassName();
    return level == null ? codecClass : codecClass + ":" + level;
  }
}
