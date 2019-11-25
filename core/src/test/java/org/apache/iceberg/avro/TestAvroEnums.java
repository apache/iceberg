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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestAvroEnums {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void writeAndValidateEnums() throws IOException {
    org.apache.avro.Schema avroSchema = SchemaBuilder.record("root")
        .fields()
        .name("enumCol")
        .type()
        .nullable()
        .enumeration("testEnum")
        .symbols("SYMB1", "SYMB2")
        .enumDefault("SYMB2")
        .endRecord();

    org.apache.avro.Schema enumSchema = avroSchema.getField("enumCol").schema().getTypes().get(0);
    Record enumRecord1 = new GenericData.Record(avroSchema);
    enumRecord1.put("enumCol", new GenericData.EnumSymbol(enumSchema, "SYMB1"));
    Record enumRecord2 = new GenericData.Record(avroSchema);
    enumRecord2.put("enumCol", new GenericData.EnumSymbol(enumSchema, "SYMB2"));
    Record enumRecord3 = new GenericData.Record(avroSchema); // null enum
    List<Record> expected = ImmutableList.of(enumRecord1, enumRecord2, enumRecord3);

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(enumRecord1);
      writer.append(enumRecord2);
      writer.append(enumRecord3);
    }

    Schema schema = new Schema(AvroSchemaUtil.convert(avroSchema).asStructType().fields());
    List<GenericData.Record> rows;
    try (AvroIterable<GenericData.Record> reader = Avro.read(Files.localInput(testFile)).project(schema).build()) {
      rows = Lists.newArrayList(reader);
    }

    // Iceberg will return enums as strings, so compare String value of enum field instead of comparing Record objects
    for (int i = 0; i < expected.size(); i += 1) {
      String expectedEnumString =
          expected.get(i).get("enumCol") == null ? null : expected.get(i).get("enumCol").toString();
      Assert.assertEquals(expectedEnumString, rows.get(i).get("enumCol"));
    }
  }
}
