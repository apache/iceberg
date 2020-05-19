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

package org.apache.iceberg;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.avro.AvroIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class TestPositionBasedDeleteRecord {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void testRecordRead() throws IOException {
    Schema deleteSchema = PositionBasedDeleteRecord.schema();
    List<PositionBasedDeleteRecord> expected = new ArrayList<>();
    expected.add(new PositionBasedDeleteRecord("path/to/a.data", 1L));
    expected.add(new PositionBasedDeleteRecord("path/to/b.data", 2L));
    expected.add(new PositionBasedDeleteRecord("path/to/c.data", 3L));

    File testFile = temp.newFile();
    Assert.assertTrue("Delete should succeed", testFile.delete());

    try (FileAppender<PositionBasedDeleteRecord> writer = Avro.write(Files.localOutput(testFile))
        .schema(deleteSchema)
        .named("position_base_delete_file")
        .build()) {
      for (PositionBasedDeleteRecord rec : expected) {
        writer.add(rec);
      }
    }

    List<PositionBasedDeleteRecord> rows;
    try (AvroIterable<PositionBasedDeleteRecord> reader = Avro.read(Files.localInput(testFile))
        .rename("position_base_delete_file", PositionBasedDeleteRecord.class.getName())
        .project(deleteSchema)
        .reuseContainers(false)
        .build()) {
      rows = Lists.newArrayList(reader);
    }

    for (int i = 0; i < expected.size(); i += 1) {
      assertEquals(deleteSchema.asStruct(), expected.get(i), rows.get(i));
    }
  }

  void assertEquals(Types.StructType struct, PositionBasedDeleteRecord expected, PositionBasedDeleteRecord actual) {
    List<Types.NestedField> fields = struct.fields();
    for (int i = 0; i < fields.size(); i += 1) {
      Object expectedValue = expected.get(i);
      Object actualValue = actual.get(i);

      Assert.assertEquals("written value should equal to read value", expectedValue, actualValue);
    }
  }
}
