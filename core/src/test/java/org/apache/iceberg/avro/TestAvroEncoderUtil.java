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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.types.Type;
import org.junit.Assert;

public class TestAvroEncoderUtil extends AvroDataTest {

  @Override
  protected void writeAndValidate(org.apache.iceberg.Schema schema) throws IOException {
    List<GenericData.Record> expected = RandomAvroData.generate(schema, 100, 1990L);
    Map<Type, Schema> typeToSchema = AvroSchemaUtil.convertTypes(schema.asStruct(), "test");
    Schema avroSchema = typeToSchema.get(schema.asStruct());

    for (GenericData.Record record : expected) {
      byte[] serializedData = AvroEncoderUtil.encode(record, avroSchema);
      GenericData.Record expectedRecord = AvroEncoderUtil.decode(serializedData);

      // Fallback to compare the record's string, because its equals implementation will depend on
      // the avro schema.
      // While the avro schema will convert the 'map' type to be a list of key/value pairs for
      // non-string keys, it
      // would be failing to read the 'array' from a 'map'.
      Assert.assertEquals(expectedRecord.toString(), record.toString());

      byte[] serializedData2 = AvroEncoderUtil.encode(expectedRecord, avroSchema);
      Assert.assertArrayEquals(serializedData2, serializedData);

      expectedRecord = AvroEncoderUtil.decode(serializedData2);
      Assert.assertEquals(expectedRecord.toString(), record.toString());
    }
  }
}
