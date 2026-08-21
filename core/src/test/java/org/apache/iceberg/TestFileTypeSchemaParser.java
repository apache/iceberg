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

import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

class TestFileTypeSchemaParser {
  @Test
  void roundTripsAsATopLevelColumn() {
    Schema schema =
        new Schema(
            required(1, "id", Types.LongType.get()), optional(2, "photo", Types.FileType.of(2)));

    String json = SchemaParser.toJson(schema);
    assertThat(json).contains("\"name\":\"photo\",\"required\":false,\"type\":\"file\"");

    assertThat(SchemaParser.fromJson(json).asStruct()).isEqualTo(schema.asStruct());
  }

  @Test
  void roundTripsNestedInAStruct() {
    Schema schema =
        new Schema(
            optional(
                1,
                "media",
                Types.StructType.of(
                    optional(2, "photo", Types.FileType.of(2)),
                    optional(9, "caption", Types.StringType.get()))));

    Schema parsed = SchemaParser.fromJson(SchemaParser.toJson(schema));

    assertThat(parsed.asStruct()).isEqualTo(schema.asStruct());
    assertThat(parsed.findField("media.photo").type()).isEqualTo(Types.FileType.of(2));
    assertThat(parsed.findField("media.photo.uri").fieldId()).isEqualTo(3);
  }

  @Test
  void roundTripsAsAListElement() {
    Schema schema =
        new Schema(optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(2))));

    Schema parsed = SchemaParser.fromJson(SchemaParser.toJson(schema));

    assertThat(parsed.asStruct()).isEqualTo(schema.asStruct());
    assertThat(parsed.findField("photos.element").type()).isEqualTo(Types.FileType.of(2));
  }

  @Test
  void roundTripsAsAMapValue() {
    Schema schema =
        new Schema(
            optional(
                1,
                "byName",
                Types.MapType.ofOptional(2, 3, Types.StringType.get(), Types.FileType.of(3))));

    Schema parsed = SchemaParser.fromJson(SchemaParser.toJson(schema));

    assertThat(parsed.asStruct()).isEqualTo(schema.asStruct());
    assertThat(parsed.findField("byName.value").type()).isEqualTo(Types.FileType.of(3));
  }

  @Test
  void acceptsAnyCaseAndWritesTheCanonicalName() {
    String json =
        "{\"type\":\"struct\",\"schema-id\":0,\"fields\":["
            + "{\"id\":5,\"name\":\"photo\",\"required\":false,\"type\":\"FILE\"}]}";

    Schema parsed = SchemaParser.fromJson(json);

    assertThat(parsed.findField("photo").type()).isEqualTo(Types.FileType.of(5));
    assertThat(SchemaParser.toJson(parsed)).contains("\"type\":\"file\"");
  }

  @Test
  void rejectsAFileTypeWithoutAnEnclosingId() {
    assertThatThrownBy(() -> SchemaParser.fromJson("\"file\""))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Cannot parse file type without an enclosing field ID");
  }

  @Test
  void rejectsWritingUnderivedNestedIds() {
    Schema schema = new Schema(optional(5, "photo", Types.FileType.of(9)));

    assertThatThrownBy(() -> SchemaParser.toJson(schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file type: nested field IDs are derived from 5, not 9");
  }

  @Test
  void rejectsWritingUnderivedNestedIdsInAList() {
    Schema schema =
        new Schema(optional(1, "photos", Types.ListType.ofOptional(2, Types.FileType.of(9))));

    assertThatThrownBy(() -> SchemaParser.toJson(schema))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid file type: nested field IDs are derived from 2, not 9");
  }
}
