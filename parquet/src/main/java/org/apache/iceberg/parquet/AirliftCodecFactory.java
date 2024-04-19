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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.parquet.compression.CompressionCodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class AirliftCodecFactory implements CompressionCodecFactory {

  private final Map<CompressionCodecName, AirliftBytesInputCompressor> compressors =
      new HashMap<>();

  private static final Set<CompressionCodecName> CODECS =
      EnumSet.of(
          CompressionCodecName.LZ4,
          CompressionCodecName.LZO,
          CompressionCodecName.SNAPPY,
          CompressionCodecName.ZSTD);

  @Override
  public BytesInputCompressor getCompressor(CompressionCodecName compressionCodecName) {
    if (CODECS.contains(compressionCodecName)) {
      return compressors.computeIfAbsent(
          compressionCodecName, c -> new AirliftBytesInputCompressor(compressionCodecName));
    } else {
      throw new UnsupportedOperationException(
          "Compression is not available: " + compressionCodecName.toString());
    }
  }

  @Override
  public BytesInputDecompressor getDecompressor(CompressionCodecName compressionCodecName) {
    if (CODECS.contains(compressionCodecName)) {
      return compressors.computeIfAbsent(
          compressionCodecName, c -> new AirliftBytesInputCompressor(compressionCodecName));
    } else {
      throw new UnsupportedOperationException(
          "Compression is not available: " + compressionCodecName.toString());
    }
  }

  @Override
  public void release() {
    compressors.values().forEach(AirliftBytesInputCompressor::release);
    compressors.clear();
  }
}
