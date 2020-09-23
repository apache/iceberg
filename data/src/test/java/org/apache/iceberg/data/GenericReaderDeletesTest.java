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

package org.apache.iceberg.data;

import java.io.File;
import java.io.IOException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TestTables;
import org.apache.iceberg.types.Types;
import org.junit.After;
import org.junit.Before;

import static org.apache.iceberg.types.Types.NestedField.required;

public class GenericReaderDeletesTest extends DeletesReadTest {
  // Schema passed to create tables
  public static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  @Before
  public void writeTestDataFile() throws IOException {
    File tableDir = temp.newFolder();
    tableDir.delete();
    this.table = TestTables.create(tableDir, "test", SCHEMA, SPEC, 2);
    generateTestData();
    table.newAppend()
        .appendFile(dataFile)
        .commit();
  }

  @After
  public void cleanup() {
    TestTables.clearTables();
  }
}
