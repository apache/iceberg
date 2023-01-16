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

import org.apache.iceberg.relocated.com.google.common.io.Resources;

public final class PuffinFormatTestUtil {
  private PuffinFormatTestUtil() {}

  // footer size for v1/empty-puffin-uncompressed.bin
  public static final long EMPTY_PUFFIN_UNCOMPRESSED_FOOTER_SIZE = 28;

  // footer size for v1/sample-metric-data-compressed-zstd.bin
  public static final long SAMPLE_METRIC_DATA_COMPRESSED_ZSTD_FOOTER_SIZE = 314;

  static byte[] readTestResource(String resourceName) throws Exception {
    return Resources.toByteArray(Resources.getResource(PuffinFormatTestUtil.class, resourceName));
  }
}
