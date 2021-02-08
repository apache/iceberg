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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.catalyst.InternalRow;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestMalformedParquetFromAvro {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();


  @Test
  public void testWriteReadAvroBinary() throws IOException {
    String schema = "{" +
        "\"type\":\"record\"," +
        "\"name\":\"DbRecord\"," +
        "\"namespace\":\"com.iceberg\"," +
        "\"fields\":[" +
          "{\"name\":\"arraybytes\", " +
            "\"type\":[ \"null\", { \"type\":\"array\", \"items\":\"bytes\"}], \"default\":null}," +
          "{\"name\":\"topbytes\", \"type\":\"bytes\"}" +
        "]" +
        "}";

    org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
    org.apache.avro.Schema avroSchema = parser.parse(schema);
    AvroSchemaConverter converter = new AvroSchemaConverter();
    MessageType parquetScehma = converter.convert(avroSchema);
    Schema icebergSchema = ParquetSchemaUtil.convert(parquetScehma);

    File testFile = temp.newFile();
    Assert.assertTrue(testFile.delete());

    ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(testFile.toURI()))
        .withDataModel(GenericData.get())
        .withSchema(avroSchema)
        .build();

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(avroSchema);
    List<ByteBuffer> expectedByteList = new ArrayList();
    byte[] expectedByte = {0x00, 0x01};
    expectedByteList.add(ByteBuffer.wrap(expectedByte));

    recordBuilder.set("arraybytes", expectedByteList);
    recordBuilder.set("topbytes", ByteBuffer.wrap(expectedByte));
    GenericData.Record record = recordBuilder.build();
    writer.write(record);
    writer.close();

    List<InternalRow> rows;
    try (CloseableIterable<InternalRow> reader =
        Parquet.read(Files.localInput(testFile))
          .project(icebergSchema)
          .createReaderFunc(type -> SparkParquetReaders.buildReader(icebergSchema, type))
          .build()) {
      rows = Lists.newArrayList(reader);
    }

    InternalRow row = rows.get(0);
    Assert.assertArrayEquals(row.getArray(0).getBinary(0), expectedByte);
    Assert.assertArrayEquals(row.getBinary(1), expectedByte);
  }

}
