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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableSet;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class TestColumnFileStruct {

  private static final String LOCATION = "s3://bucket/data/column.parquet";
  private static final Set<Integer> FIELD_IDS = ImmutableSet.of(1, 2, 3);
  private static final FileFormat FILE_FORMAT = FileFormat.PARQUET;
  private static final long FILE_SIZE_IN_BYTES = 1024L;
  private static final ByteBuffer KEY_METADATA = ByteBuffer.wrap(new byte[] {1, 2, 3});
  private static final Comparator<StructLike> COLUMN_FILE_COMPARATOR =
      Comparators.forType(ColumnFile.schema());

  @Test
  void fieldAccess() {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fieldIds()).isEqualTo(FIELD_IDS);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  void copy() {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    ColumnFile copy = columnFile.copy();

    assertThat((StructLike) copy).usingComparator(COLUMN_FILE_COMPARATOR).isEqualTo(columnFile);

    // verify deep copy of key metadata
    assertThat(copy.keyMetadata().array()).isNotSameAs(columnFile.keyMetadata().array());
  }

  @Test
  void structLikeSize() {
    ColumnFileStruct columnFile = new ColumnFileStruct();
    assertThat(columnFile.size()).isEqualTo(5);
  }

  @Test
  void setFieldsByOrdinals() {
    ColumnFileStruct columnFile = new ColumnFileStruct();

    columnFile.set(0, LOCATION);
    columnFile.set(1, ImmutableList.copyOf(FIELD_IDS));
    columnFile.set(2, FILE_FORMAT.toString());
    columnFile.set(3, FILE_SIZE_IN_BYTES);
    columnFile.set(4, KEY_METADATA);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fieldIds()).isEqualTo(FIELD_IDS);
    assertThat(columnFile.fileFormat()).isEqualTo(FILE_FORMAT);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.keyMetadata()).isEqualTo(KEY_METADATA);
  }

  @Test
  void getFieldsByOrdinals() {
    ColumnFileStruct columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, List.class)).containsExactlyElementsOf(FIELD_IDS);
    assertThat(columnFile.get(2, String.class)).isEqualTo(FILE_FORMAT.toString());
    assertThat(columnFile.get(3, Long.class)).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.get(4, ByteBuffer.class)).isEqualTo(KEY_METADATA);
  }

  @Test
  void projectedStructLike() {
    Types.StructType projection =
        Types.StructType.of(ColumnFile.LOCATION, ColumnFile.FILE_SIZE_IN_BYTES);

    ColumnFileStruct columnFile = new ColumnFileStruct(projection);
    assertThat(columnFile.size()).isEqualTo(2);

    // projected position 0 maps to internal position of location
    // projected position 1 maps to internal position of file_size_in_bytes
    columnFile.set(0, LOCATION);
    columnFile.set(1, FILE_SIZE_IN_BYTES);

    assertThat(columnFile.location()).isEqualTo(LOCATION);
    assertThat(columnFile.fileSizeInBytes()).isEqualTo(FILE_SIZE_IN_BYTES);
    assertThat(columnFile.get(0, String.class)).isEqualTo(LOCATION);
    assertThat(columnFile.get(1, Long.class)).isEqualTo(FILE_SIZE_IN_BYTES);
  }

  @ParameterizedTest
  @MethodSource("org.apache.iceberg.TestHelpers#serializers")
  void serializationRoundTrip(TestHelpers.RoundTripSerializer<ColumnFile> roundTripSerializer)
      throws IOException, ClassNotFoundException {
    ColumnFile columnFile =
        new ColumnFileStruct(LOCATION, FIELD_IDS, FILE_FORMAT, FILE_SIZE_IN_BYTES, KEY_METADATA);

    ColumnFile deserialized = roundTripSerializer.apply(columnFile);

    assertThat((StructLike) deserialized)
        .usingComparator(COLUMN_FILE_COMPARATOR)
        .isEqualTo(columnFile);
  }
}
