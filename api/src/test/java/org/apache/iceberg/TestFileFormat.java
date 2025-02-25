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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;

@SuppressWarnings("unused")
class TestFileFormat {

  private static final Object[][] FILE_NAMES =
      new Object[][] {
        // Files with format
        {"file.puffin", FileFormat.PUFFIN},
        {"dir/file.puffin", FileFormat.PUFFIN},
        {"file.orc", FileFormat.ORC},
        {"dir/file.orc", FileFormat.ORC},
        {"file.parquet", FileFormat.PARQUET},
        {"dir/file.parquet", FileFormat.PARQUET},
        {"file.avro", FileFormat.AVRO},
        {"dir/file.avro", FileFormat.AVRO},
        {"v1.metadata.json", FileFormat.METADATA},
        {"dir/v1.metadata.json", FileFormat.METADATA},
        // Short file names with format
        {"x.puffin", FileFormat.PUFFIN},
        {"x.orc", FileFormat.ORC},
        {"x.parquet", FileFormat.PARQUET},
        {"x.avro", FileFormat.AVRO},
        {"x.metadata.json", FileFormat.METADATA},
        // Unsupported formats
        {"file.csv", null},
        {"dir/file.csv", null},
        // No format
        {"file", null},
        {"dir", null},
        // Blank strings
        {"", null},
        {" ", null},
        {null, null},
      };

  @ParameterizedTest
  @FieldSource("FILE_NAMES")
  void fromFileName(String fileName, FileFormat expected) {
    FileFormat actual = FileFormat.fromFileName(fileName);

    assertThat(actual).isEqualTo(expected);
  }
}
