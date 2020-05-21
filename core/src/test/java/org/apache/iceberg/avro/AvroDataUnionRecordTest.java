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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroDataUnionRecordTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected void writeAndValidate(
      List<GenericData.Record> actualWrite,
      List<GenericData.Record> expectedRead,
      Schema icebergSchema) throws IOException {
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Avro.write(Files.localOutput(testFile))
        .schema(icebergSchema)
        .named("test")
        .build()) {
      for (GenericData.Record rec : actualWrite) {
        writer.add(rec);
      }
    }

    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader = Avro.read(Files.localInput(testFile))
        .project(icebergSchema)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expectedRead.size(); i += 1) {
      AvroTestHelpers.assertEquals(icebergSchema.asStruct(), expectedRead.get(i), rows.get(i));
    }
  }

  @Test
  public void testMapOfUnionValues() throws IOException {
    String schema1 = "{\n" +
        "  \"name\": \"MapOfUnion\",\n" +
        "  \"type\": \"record\",\n" +
        "  \"fields\": [\n" +
        "    {\n" +
        "      \"name\": \"map\",\n" +
        "      \"type\": [\n" +
        "        \"null\",\n" +
        "        {\n" +
        "          \"type\": \"map\",\n" +
        "          \"values\": [\n" +
        "            \"null\",\n" +
        "            \"boolean\",\n" +
        "            \"int\",\n" +
        "            \"long\",\n" +
        "            \"float\",\n" +
        "            \"double\",\n" +
        "            \"bytes\",\n" +
        "            \"string\"\n" +
        "          ]\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ]\n" +
        "}";
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schema1);
    org.apache.iceberg.Schema icebergSchema = AvroSchemaUtil.toIceberg(avroSchema);
    org.apache.avro.Schema avroSchemaUnionRecord = AvroSchemaUtil.convert(icebergSchema, "test");
    org.apache.avro.Schema unionRecordSchema =
        avroSchemaUnionRecord.getFields().get(0).schema().getTypes().get(1).getValueType().getTypes().get(1);

    List<GenericData.Record> expectedRead = new ArrayList<>();
    List<GenericData.Record> actualWrite = new ArrayList<>();

    for (long i = 0; i < 10; i++) {
      Map<String, Object> map = new HashMap<>();
      Map<String, Object> mapRead = new HashMap<>();
      updateMapsForUnionSchema(unionRecordSchema, map, mapRead, i);
      GenericData.Record recordRead = new GenericRecordBuilder(avroSchema)
          .set("map", mapRead)
          .build();
      GenericData.Record record = new GenericRecordBuilder(avroSchema)
          .set("map", map)
          .build();
      actualWrite.add(record);
      expectedRead.add(recordRead);
    }
    writeAndValidate(actualWrite, expectedRead, icebergSchema);
  }

  private void updateMapsForUnionSchema(
      org.apache.avro.Schema unionRecordSchema,
      Map<String, Object> map,
      Map<String, Object> mapRead,
      Long index) {
    map.put("boolean", index % 2 == 0);
    map.put("int", index.intValue());
    map.put("long", index);
    map.put("float", index.floatValue());
    map.put("double", index.doubleValue());
    map.put("bytes", ByteBuffer.wrap(("bytes_" + index).getBytes()));
    map.put("string", "string_" + index);

    map.entrySet().stream().forEach(e -> {
      String key = e.getKey();
      GenericData.Record record = getGenericRecordForUnionType(unionRecordSchema, map, key);
      mapRead.put(key, record);
    });
  }

  private GenericData.Record getGenericRecordForUnionType(
      org.apache.avro.Schema unionRecordSchema,
      Map<String, Object> map,
      String key) {
    GenericRecordBuilder rec = new GenericRecordBuilder(unionRecordSchema);
    switch (key) {
      case "boolean":
        return rec
            .set("member1", map.get(key))
            .build();
      case "int":
        return rec
            .set("member2", map.get(key))
            .build();
      case "long":
        return rec
            .set("member3", map.get(key))
            .build();
      case "float":
        return rec
            .set("member4", map.get(key))
            .build();
      case "double":
        return rec
            .set("member5", map.get(key))
            .build();
      case "bytes":
        return rec
            .set("member6", map.get(key))
            .build();
      case "string":
        return rec
            .set("member7", map.get(key))
            .build();
      default:
        throw new IllegalStateException("key mapping not found for " + key);
    }
  }
}
