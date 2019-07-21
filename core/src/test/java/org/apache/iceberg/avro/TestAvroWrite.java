/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.avro;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.avro.generic.GenericData;
import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

public class TestAvroWrite {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testWriteCreate() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.AVRO_WRITE_MODE, "create");

    File file = write(10, properties, null);

    thrown.expect(AlreadyExistsException.class);
    write(5, properties, file);
  }

  @Test
  public void testWriteOverwrite() throws IOException {
    Map<String, String> properties = ImmutableMap.of(TableProperties.AVRO_WRITE_MODE, "overwrite");

    File file = write(10, properties, null);
    Assert.assertEquals(10, Lists.newArrayList(Avro.read(Files.localInput(file))
        .project(schema())
        .build()
    ).size());

    write(5, properties, file);
    Assert.assertEquals(5, Lists.newArrayList(Avro.read(Files.localInput(file))
        .project(schema())
        .build()
    ).size());
  }

  @Test
  public void testUnsupportedWriteMode() throws IOException {
    thrown.expect(IllegalArgumentException.class);
    write(10, ImmutableMap.of(TableProperties.AVRO_WRITE_MODE, "abc"), null);
  }

  private Schema schema() {
    return new Schema(
        Types.NestedField.required(0, "col_int", Types.IntegerType.get())
    );
  }

  private File write(int recordsNumber, Map<String, String> properties, File file) throws IOException {
    Schema schema = schema();
    org.apache.avro.Schema avroSchema = AvroSchemaUtil.convert(schema, "table");

    List<GenericData.Record> records = IntStream.rangeClosed(1, recordsNumber)
        .mapToObj(index -> {
          GenericData.Record record = new GenericData.Record(avroSchema);
          record.put("col_int", index);
          return record;
        })
        .collect(Collectors.toList());

    File outputFile = file;
    if (outputFile == null) {
      outputFile = temp.newFile();
      Assert.assertTrue("File should have been deleted", outputFile.delete());
    }

    try (FileAppender<GenericData.Record> appender = Avro.write(Files.localOutput(outputFile))
        .schema(schema)
        .setAll(properties)
        .build()) {
      appender.addAll(records);
    }

    return outputFile;
  }
}
