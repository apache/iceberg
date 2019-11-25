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
import java.util.List;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;

import static org.apache.iceberg.types.Types.NestedField.required;

public class TestSparkParquetFallbackToDictionaryEncodingForVectorizedReader extends TestSparkParquetVectorizedReader {

  @Override
  protected Types.StructType getSupportedPrimitives() {
    return Types.StructType.of(
        required(100, "id", Types.LongType.get()),
        required(101, "data", Types.StringType.get()));
  }

  @Override
  protected void writeAndValidate(Schema schema) throws IOException {
    // Write test data
    Assume.assumeTrue("Parquet Avro cannot write non-string map keys", null == TypeUtil.find(
        schema,
        type -> type.isMapType() && type.asMapType().keyType() != Types.StringType.get()));

    List<GenericData.Record> expected =
        RandomData.generateListWithFallBackDictionaryEncodingForStrings(schema, 1000000, 0L, 0.5f);

    // write a test parquet file using iceberg writer
    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<GenericData.Record> writer = Parquet.write(Files.localOutput(testFile))
        .schema(schema)
        .named("test")
        .build()) {
      writer.addAll(expected);
    }
    assertRecordsMatch(schema, expected, testFile);
  }
}
