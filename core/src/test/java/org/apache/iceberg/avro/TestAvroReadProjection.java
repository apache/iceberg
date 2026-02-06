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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

public class TestAvroReadProjection extends TestReadProjection {
  @Override
  protected GenericData.Record writeAndRead(
      String desc, Schema writeSchema, Schema readSchema, GenericData.Record record)
      throws IOException {
    File file = temp.resolve(desc + ".avro").toFile();

    try (FileAppender<GenericData.Record> appender =
        Avro.write(Files.localOutput(file)).schema(writeSchema).build()) {
      appender.add(record);
    }

    Iterable<GenericData.Record> records =
        Avro.read(Files.localInput(file)).project(readSchema).build();

    return Iterables.getOnlyElement(records);
  }

  @Test
  public void testAvroArrayAsLogicalMap() throws IOException {
    Schema writeSchema =
        new Schema(
            Types.NestedField.optional(
                0,
                "map",
                Types.MapType.ofOptional(
                    2,
                    3,
                    Types.LongType.get(),
                    Types.ListType.ofRequired(1, Types.LongType.get()))));

    List<Long> values1 = ImmutableList.of(101L, 102L);
    List<Long> values2 = ImmutableList.of(201L, 202L, 203L);
    GenericData.Record record =
        new GenericData.Record(AvroSchemaUtil.convert(writeSchema, "table"));
    record.put("map", ImmutableMap.of(100L, values1, 200L, values2));

    GenericData.Record projected =
        writeAndRead("full_projection", writeSchema, writeSchema, record);
    assertThat(((Map<Long, List<Long>>) projected.get("map")).get(100L))
        .as("Should contain correct value list")
        .isEqualTo(values1);
    assertThat(((Map<Long, List<Long>>) projected.get("map")).get(200L))
        .as("Should contain correct value list")
        .isEqualTo(values2);
  }
}
