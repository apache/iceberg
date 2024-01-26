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
package org.apache.iceberg.spark.data;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestSparkAvroEnums {

  @TempDir private Path temp;

  @Test
  public void writeAndValidateEnums() throws IOException {
    org.apache.avro.Schema avroSchema =
        SchemaBuilder.record("root")
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

    File testFile = File.createTempFile("junit", null, temp.toFile());
    assertThat(testFile.delete()).as("Delete should succeed").isTrue();

    try (DataFileWriter<Record> writer = new DataFileWriter<>(new GenericDatumWriter<>())) {
      writer.create(avroSchema, testFile);
      writer.append(enumRecord1);
      writer.append(enumRecord2);
      writer.append(enumRecord3);
    }

    Schema schema = new Schema(AvroSchemaUtil.convert(avroSchema).asStructType().fields());
    List<InternalRow> rows;
    try (AvroIterable<InternalRow> reader =
        Avro.read(Files.localInput(testFile))
            .createReaderFunc(SparkAvroReader::new)
            .project(schema)
            .build()) {
      rows = Lists.newArrayList(reader);
    }

    // Iceberg will return enums as strings, so we compare string values for the enum field
    for (int i = 0; i < expected.size(); i += 1) {
      String expectedEnumString =
          expected.get(i).get("enumCol") == null ? null : expected.get(i).get("enumCol").toString();
      String sparkString =
          rows.get(i).getUTF8String(0) == null ? null : rows.get(i).getUTF8String(0).toString();
      assertThat(sparkString).isEqualTo(expectedEnumString);
    }
  }
}
