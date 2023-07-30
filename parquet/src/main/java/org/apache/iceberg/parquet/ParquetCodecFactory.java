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
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * This class implements a codec factory that is used when reading from Parquet. It adds a
 * workaround to cache codecs by name and level, not just by name. This can be removed when this
 * change is made to Parquet.
 */
public class ParquetCodecFactory extends CodecFactory {

  public ParquetCodecFactory(Configuration configuration, int pageSize) {
    super(configuration, pageSize);
  }

  /**
   * This is copied from {@link CodecFactory} and modified to include the level in the cache key.
   */
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
        // Try to load the class using the job classloader
        codecClass = configuration.getClassLoader().loadClass(codecClassName);
      }
      codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, configuration);
      CODEC_BY_NAME.put(cacheKey, codec);
      return codec;
    } catch (ClassNotFoundException e) {
      throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
    }
  }

  private String cacheKey(CompressionCodecName codecName) {
    String level = null;
    switch (codecName) {
      case GZIP:
        level = configuration.get("zlib.compress.level");
        break;
      case BROTLI:
        level = configuration.get("compression.brotli.quality");
        break;
      case ZSTD:
        level = configuration.get("parquet.compression.codec.zstd.level");
        if (level == null) {
          // keep "io.compression.codec.zstd.level" for backwards compatibility
          level = configuration.get("io.compression.codec.zstd.level");
        }
        break;
      default:
        // compression level is not supported; ignore it
    }
    String codecClass = codecName.getHadoopCompressionCodecClassName();
    return level == null ? codecClass : codecClass + ":" + level;
  }
}
