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
package org.apache.iceberg;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileFormatTest {

  @ParameterizedTest
  @MethodSource
  void fromFileName(String fileName, FileFormat expected) {
    FileFormat actual = FileFormat.fromFileName(fileName);

    assertThat(actual).isEqualTo(expected);
  }

  static Stream<Arguments> fromFileName() {
    return Stream.of(
        // Files with format
        arguments("file.puffin", FileFormat.PUFFIN),
        arguments("dir/file.puffin", FileFormat.PUFFIN),
        arguments("file.orc", FileFormat.ORC),
        arguments("dir/file.orc", FileFormat.ORC),
        arguments("file.parquet", FileFormat.PARQUET),
        arguments("dir/file.parquet", FileFormat.PARQUET),
        arguments("file.avro", FileFormat.AVRO),
        arguments("dir/file.avro", FileFormat.AVRO),
        arguments("v1.metadata.json", FileFormat.METADATA),
        arguments("dir/v1.metadata.json", FileFormat.METADATA),
        // Unsupported formats
        arguments("file.csv", null),
        arguments("dir/file.csv", null),
        // No format
        arguments("file", null),
        arguments("dir", null),
        // File names match format names but no extension
        arguments("puffin", null),
        arguments("orc", null),
        arguments("parquet", null),
        arguments("avro", null),
        arguments("metadata.json", null));
  }
}
