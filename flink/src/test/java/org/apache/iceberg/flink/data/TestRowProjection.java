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

package org.apache.iceberg.flink.data;

import java.io.File;
import java.io.IOException;
import org.apache.flink.types.Row;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.relocated.com.google.common.collect.Iterables;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestRowProjection {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  protected Row writeAndRead(String desc, Schema writeSchema, Schema readSchema, Row row) throws IOException {
    File file = temp.newFile(desc + ".avro");
    Assert.assertTrue(file.delete());

    try (FileAppender<Row> appender = Avro.write(Files.localOutput(file))
        .schema(writeSchema)
        .createWriterFunc(FlinkAvroWriter::new)
        .build()) {
      appender.add(row);
    }

    Iterable<Row> records = Avro.read(Files.localInput(file))
        .project(readSchema)
        .createReaderFunc(FlinkAvroReader::new)
        .build();

    return Iterables.getOnlyElement(records);
  }

  @Test
  public void testFullProjection() throws Exception {
    Schema schema = new Schema(
        Types.NestedField.required(0, "id", Types.LongType.get()),
        Types.NestedField.optional(1, "data", Types.StringType.get())
    );

    Row row = Row.of(34L, "test");

    Row projected = writeAndRead("full_projection", schema, schema, row);

    Assert.assertEquals("Should contain the correct id value", 34L, (long) projected.getField(0));

    int cmp = Comparators.charSequences()
        .compare("test", (CharSequence) projected.getField(1));
    Assert.assertEquals("Should contain the correct data value", cmp, 0);
  }
}
