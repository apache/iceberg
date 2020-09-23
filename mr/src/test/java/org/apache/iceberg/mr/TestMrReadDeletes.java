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

package org.apache.iceberg.mr;

import java.io.File;
import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.DeletesReadTest;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.types.Types.NestedField.required;

@RunWith(Parameterized.class)
public class TestMrReadDeletes extends DeletesReadTest {
  // Schema passed to create tables
  public static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      required(2, "data", Types.StringType.get())
  );

  // Partition spec used to create tables
  static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .bucket("data", 16)
      .build();

  // parametrized variables
  private final TestIcebergInputFormats.TestInputFormat.Factory<Record> testInputFormat;
  private final FileFormat fileFormat;

  @Parameterized.Parameters
  public static Object[][] parameters() {
    Object[][] parameters = new Object[TestIcebergInputFormats.TESTED_INPUT_FORMATS.size() *
        TestIcebergInputFormats.TESTED_FILE_FORMATS.size()][2];

    int idx = 0;

    for (TestIcebergInputFormats.TestInputFormat.Factory<Record> inputFormat :
        TestIcebergInputFormats.TESTED_INPUT_FORMATS) {
      for (String fileFormat : TestIcebergInputFormats.TESTED_FILE_FORMATS) {
        parameters[idx++] = new Object[] {inputFormat, fileFormat};
      }
    }

    return parameters;
  }

  public TestMrReadDeletes(TestIcebergInputFormats.TestInputFormat.Factory<Record> testInputFormat, String fileFormat) {
    this.testInputFormat = testInputFormat;
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws IOException {
    Configuration conf = new Configuration();
    HadoopTables tables = new HadoopTables(conf);
    File location = temp.newFolder(testInputFormat.name(), fileFormat.name());
    Assert.assertTrue(location.delete());
    TestHelper helper = new TestHelper(conf, tables, location.toString(), SCHEMA, SPEC, fileFormat, temp);
    this.table = helper.createTable();
    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));
    generateTestData();
    helper.appendToTable(dataFile);
  }
}
