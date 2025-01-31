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
package org.apache.iceberg.data.avro;

import static org.apache.iceberg.avro.AvroSchemaUtil.convert;
import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.data.DataTest;
import org.apache.iceberg.data.DataTestHelpers;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;
import org.junit.jupiter.api.Test;

public class TestGenericData extends DataTest {
  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    writeAndValidate(schema, schema);
  }

  @Override
  protected void writeAndValidate(Schema writeSchema, Schema expectedSchema) throws IOException {
    List<Record> expected = RandomGenericData.generate(writeSchema, 100, 0L);

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).isTrue();

    try (FileAppender<Record> writer =
        Avro.write(Files.localOutput(testFile))
            .schema(writeSchema)
            .createWriterFunc(DataWriter::create)
            .named("test")
            .build()) {
      for (Record rec : expected) {
        writer.add(rec);
      }
    }

    List<Record> rows;
    try (AvroIterable<Record> reader =
        Avro.read(Files.localInput(testFile))
            .project(expectedSchema)
            .createResolvingReader(PlannedDataReader::create)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      DataTestHelpers.assertEquals(expectedSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  @Override
  protected boolean supportsDefaultValues() {
    return true;
  }

  @Test
  public void testSchemaWithTwoVariants() throws JsonProcessingException {
    final Schema schema =
        new Schema(
            required(1, "id", Types.IntegerType.get()),
            optional(2, "v1", Types.VariantType.get()),
            optional(3, "v2", Types.VariantType.get()));

    GenericRecordBuilder builder = new GenericRecordBuilder(convert(schema, "table"));
    builder.set("id", 1);

    GenericData.Record record = builder.build();
    String expectedSchema =
        "{\"type\":\"record\",\"name\":\"table\","
            + "\"fields\":[{\"name\":\"id\",\"type\":\"int\",\"field-id\":1},"
            + "{\"name\":\"v1\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"r2\","
            + "\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}]}],\"default\":null,\"field-id\":2},"
            + "{\"name\":\"v2\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"r3\","
            + "\"fields\":[{\"name\":\"metadata\",\"type\":\"bytes\"},"
            + "{\"name\":\"value\",\"type\":\"bytes\"}]}],\"default\":null,\"field-id\":3}]}";
    assertThat(JsonUtil.mapper().readValue(record.getSchema().toString(), JsonNode.class))
        .isEqualTo(JsonUtil.mapper().readValue(expectedSchema, JsonNode.class));
  }
}
