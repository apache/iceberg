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
package org.apache.iceberg.flink;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class TestFileTypeFlink {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.LongType.get()),
          optional(2, "photo", Types.FileType.of(2)),
          optional(9, "data", Types.StringType.get()));

  @TempDir private Path temp;

  @Test
  void convertsAFileToARowOfItsNestedFields() {
    RowType rowType = FlinkSchemaUtil.convert(SCHEMA);
    RowType photoType = (RowType) rowType.getTypeAt(rowType.getFieldIndex("photo"));

    Types.StructType expected = Types.FileType.of(2).asStruct();
    assertThat(photoType.getFieldNames())
        .containsExactlyElementsOf(Lists.transform(expected.fields(), Types.NestedField::name));
    for (Types.NestedField field : expected.fields()) {
      assertThat(photoType.getTypeAt(photoType.getFieldIndex(field.name())))
          .isEqualTo(FlinkSchemaUtil.convert(field.type()).copy(field.isOptional()));
    }
  }

  @Test
  void wrapsAFileColumnAsAStruct() {
    RowDataWrapper wrapper = new RowDataWrapper(FlinkSchemaUtil.convert(SCHEMA), SCHEMA.asStruct());
    RowData row = GenericRowData.of(1L, photoRowData(), StringData.fromString("d"));

    Types.StructType expected = Types.FileType.of(2).asStruct();
    StructLike photo = wrapper.wrap(row).get(1, StructLike.class);

    assertThat(photo.size()).isEqualTo(expected.fields().size());
    assertThat(photo.get(position(expected, "uri"), String.class)).isEqualTo("s3://bucket/photo");
    assertThat(photo.get(position(expected, "offset"), Long.class)).isEqualTo(128L);
    assertThat(photo.get(position(expected, "size"), Long.class)).isEqualTo(1024L);
    assertThat(photo.get(position(expected, "content_type"), String.class)).isEqualTo("image/png");
    assertThat(photo.get(position(expected, "checksum"), String.class)).isEqualTo("abc123");
    assertThat(photo.get(position(expected, "inline"), ByteBuffer.class)).isNull();
  }

  @Test
  void restoresTheFileTypeWhenConvertingBackFromFlink() {
    ResolvedSchema flinkSchema = FlinkSchemaUtil.toResolvedSchema(SCHEMA);

    assertThat(FlinkSchemaUtil.convert(SCHEMA, flinkSchema).asStruct())
        .isEqualTo(SCHEMA.asStruct());
  }

  @Test
  void restoresTheFileTypeForAProjection() {
    Schema projected = SCHEMA.select("id", "photo");
    ResolvedSchema flinkSchema = FlinkSchemaUtil.toResolvedSchema(projected);

    assertThat(FlinkSchemaUtil.convert(SCHEMA, flinkSchema).asStruct())
        .isEqualTo(projected.asStruct());
  }

  @Test
  void rejectsWritingAFileColumn() {
    Table table =
        new HadoopTables()
            .create(
                SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of(TableProperties.FORMAT_VERSION, "4"),
                temp.resolve("table").toUri().toString());

    assertThatThrownBy(
            () ->
                new RowDataTaskWriterFactory(
                    table,
                    FlinkSchemaUtil.convert(SCHEMA),
                    TARGET_FILE_SIZE,
                    FileFormat.PARQUET,
                    table.properties(),
                    null,
                    false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageStartingWith("Cannot write file columns from Flink: [photo]");
  }

  private static int position(Types.StructType struct, String name) {
    return struct.fields().indexOf(struct.field(name));
  }

  private static GenericRowData photoRowData() {
    return GenericRowData.of(
        StringData.fromString("s3://bucket/photo"),
        128L,
        1024L,
        StringData.fromString("image/png"),
        StringData.fromString("abc123"),
        null);
  }
}
