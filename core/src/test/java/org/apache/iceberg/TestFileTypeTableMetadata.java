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

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TestFileTypeTableMetadata {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void rejectsAFileColumnBeforeFormatVersion4(int formatVersion) {
    assertThatThrownBy(() -> newTableMetadata(formatVersion))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining("Invalid type for photo: file is not supported until v4");
  }

  @Test
  void keepsTheFileTypeThroughSerialization() {
    TableMetadata metadata = newTableMetadata(4);
    TableMetadata reparsed = TableMetadataParser.fromJson(TableMetadataParser.toJson(metadata));

    assertThat(reparsed.schema().findField("photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(reparsed.lastColumnId()).isEqualTo(metadata.lastColumnId());
    assertThat(reparsed.schema().asStruct()).isEqualTo(SCHEMA.asStruct());
  }

  private static TableMetadata newTableMetadata(int formatVersion) {
    return TableMetadata.newTableMetadata(
        SCHEMA,
        PartitionSpec.unpartitioned(),
        "file:/tmp/table",
        ImmutableMap.of(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)));
  }
}
