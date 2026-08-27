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
package org.apache.iceberg.spark.data.vectorized;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.iceberg.spark.SparkSchemaUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;

class TestConstantColumnVector {
  private static final int BATCH_SIZE = 10;
  private static final Types.FileType PHOTO = Types.FileType.of(2);

  /**
   * A file column disables batch reads, so a constant file vector is not reachable from a scan
   * today. This pins the intended child type behavior so a change on the vectorized path cannot
   * turn it into a ClassCastException that names no column.
   */
  @Test
  void exposesTheNestedFieldsOfAFileConstantAsChildren() {
    GenericInternalRow photo =
        new GenericInternalRow(
            new Object[] {
              UTF8String.fromString("s3://bucket/photo.png"),
              0L,
              12L,
              UTF8String.fromString("image/png"),
              UTF8String.fromString("d41d8cd9"),
              new byte[] {1, 2}
            });
    ConstantColumnVector vector = new ConstantColumnVector(PHOTO, BATCH_SIZE, photo);

    for (int ordinal = 0; ordinal < PHOTO.fields().size(); ordinal += 1) {
      Types.NestedField field = PHOTO.fields().get(ordinal);
      assertThat(vector.getChild(ordinal).dataType())
          .as("Child %s should have the nested field type", field.name())
          .isEqualTo(SparkSchemaUtil.convert(field.type()));
    }

    assertThat(vector.getChild(0).getUTF8String(0).toString()).isEqualTo("s3://bucket/photo.png");
    assertThat(vector.getChild(2).getLong(0)).isEqualTo(12L);
  }
}
