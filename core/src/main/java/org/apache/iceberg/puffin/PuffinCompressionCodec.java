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
package org.apache.iceberg.puffin;

import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;

public enum PuffinCompressionCodec {
  /** No compression */
  NONE(null),

  /** LZ4 single compression frame with content size present */
  LZ4("lz4"),

  /** Zstandard single compression frame with content size present */
  ZSTD("zstd"),
/**/ ;

  private static final Map<String, PuffinCompressionCodec> BY_NAME =
      Stream.of(values())
          .collect(
              Collectors.toMap(
                  PuffinCompressionCodec::codecName,
                  Function.identity(),
                  (a, b) -> {
                    throw new UnsupportedOperationException("Two enum instances with same name");
                  },
                  Maps::newHashMap));

  private final String codecName;

  PuffinCompressionCodec(String codecName) {
    this.codecName = codecName;
  }

  @Nullable
  public String codecName() {
    return codecName;
  }

  public static PuffinCompressionCodec forName(@Nullable String codecName) {
    PuffinCompressionCodec codec = BY_NAME.get(codecName);
    Preconditions.checkArgument(codec != null, "Unknown codec name: %s", codecName);
    return codec;
  }
}
