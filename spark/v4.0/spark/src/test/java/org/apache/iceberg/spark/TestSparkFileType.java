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
package org.apache.iceberg.spark;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.TestHelpers.Row;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

class TestSparkFileType {
  private static final byte[] BYTES = new byte[] {1, 2};
  private static final int PHOTO_ID = 2;
  private static final Types.FileType PHOTO = Types.FileType.of(PHOTO_ID);
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(PHOTO_ID, "photo", PHOTO),
          optional(9, "data", Types.StringType.get()));

  @Test
  void convertsAFileColumnToAStructOfItsNestedFields() {
    StructType converted = SparkSchemaUtil.convert(SCHEMA);
    DataType photo = converted.apply("photo").dataType();

    assertThat(converted.apply("photo").nullable()).isTrue();
    assertThat(photo).isEqualTo(SparkSchemaUtil.convert(PHOTO.asStruct()));
    assertThat(((StructType) photo).fieldNames()).containsExactlyElementsOf(nestedFieldNames());
  }

  @Test
  void prunesToAFileWhenEveryNestedFieldIsProjected() {
    Schema pruned = SparkSchemaUtil.prune(SCHEMA, SparkSchemaUtil.convert(SCHEMA));

    assertThat(pruned.asStruct()).isEqualTo(SCHEMA.asStruct());
    assertThat(pruned.findField("photo").type()).isEqualTo(PHOTO);
  }

  @Test
  void prunesToAStructWhenOnlySomeNestedFieldsAreProjected() {
    Types.NestedField uri = PHOTO.field("uri");
    StructType requested =
        new StructType()
            .add("photo", new StructType().add(uri.name(), SparkSchemaUtil.convert(uri.type())));

    Schema pruned = SparkSchemaUtil.prune(SCHEMA, requested);

    assertThat(pruned.findField("photo").type()).isEqualTo(Types.StructType.of(uri));
  }

  @Test
  void describesAFileColumnAsAStruct() {
    assertThat(Spark3Util.describe(PHOTO))
        .isEqualTo(Spark3Util.describe(Types.StructType.of(PHOTO.fields())));
  }

  @Test
  void rejectsConvertingASparkTypeBackToAFileColumn() {
    StructType sparkType = SparkSchemaUtil.convert(SCHEMA);

    assertThatThrownBy(() -> SparkSchemaUtil.convert(SCHEMA, sparkType))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write file column photo: Spark cannot express the file type");

    assertThatThrownBy(() -> SparkSchemaUtil.convertWithFreshIds(SCHEMA, sparkType, true))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage("Cannot write file column photo: Spark cannot express the file type");
  }

  @Test
  void namesANestedFileColumnInTheWriteRejection() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()),
            optional(
                2, "attachments", Types.StructType.of(optional(3, "photo", Types.FileType.of(3)))));

    assertThatThrownBy(
            () -> SparkSchemaUtil.convert(schema, SparkSchemaUtil.convert(schema), false))
        .isInstanceOf(UnsupportedOperationException.class)
        .hasMessage(
            "Cannot write file column attachments.photo: Spark cannot express the file type");
  }

  @Test
  void convertsAWriteSchemaThatOmitsAFileColumn() {
    StructType sparkType = SparkSchemaUtil.convert(SCHEMA.select("id", "data"));

    assertThat(SparkSchemaUtil.convert(SCHEMA, sparkType).asStruct())
        .isEqualTo(SCHEMA.select("id", "data").asStruct());
  }

  @Test
  void convertsAFileValueToASparkRow() {
    StructLike photo =
        Row.of("s3://bucket/photo.png", 0L, 12L, "image/png", "d41d8cd9", ByteBuffer.wrap(BYTES));

    InternalRow converted = (InternalRow) SparkUtil.internalToSpark(PHOTO, photo);

    assertThat(converted.numFields()).isEqualTo(PHOTO.fields().size());
    assertThat(converted.getUTF8String(0).toString()).isEqualTo("s3://bucket/photo.png");
    assertThat(converted.getLong(2)).isEqualTo(12L);
    assertThat(converted.getBinary(5)).isEqualTo(BYTES);
  }

  private static List<String> nestedFieldNames() {
    return PHOTO.fields().stream().map(Types.NestedField::name).collect(Collectors.toList());
  }
}
