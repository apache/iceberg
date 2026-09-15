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
package org.apache.iceberg.avro;

import java.util.Map;

/**
 * Interface for {@link org.apache.avro.io.DatumReader} implementations that need access to Avro
 * file header metadata before reading records. {@link AvroIterable} calls {@link
 * #setFileMetadata(Map)} after opening the file, allowing readers to adjust their behavior based on
 * per-file encoding metadata.
 */
interface AvroFileMetadataAware {
  /** Avro file metadata key that records the fixed-field encoding used when writing. */
  String FIXED_ENCODING_META_KEY = "iceberg.avro.fixed-encoding";

  /** Legacy encoding: fixed fields written with {@code encoder.writeBytes()} (length-prefixed). */
  String FIXED_ENCODING_V1 = "v1";

  /** Correct encoding: fixed fields written with {@code encoder.writeFixed()} (exact N bytes). */
  String FIXED_ENCODING_V2 = "v2";

  void setFileMetadata(Map<String, String> metadata);
}
