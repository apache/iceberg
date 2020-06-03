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
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.TestHelpers.Row;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.iceberg.mr.TestHelpers.writeFile;
import static org.apache.iceberg.types.Types.NestedField.required;


@RunWith(Parameterized.class)
public abstract class BaseInputFormatTest {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][]{
        new Object[]{"parquet"},
        new Object[]{"avro"},
        new Object[]{"orc"}
    };
  }

  protected static final Schema SCHEMA = new Schema(
      required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()),
      required(3, "date", Types.StringType.get()));

  protected static final PartitionSpec SPEC = PartitionSpec.builderFor(SCHEMA)
      .identity("date")
      .bucket("id", 1)
      .build();

  protected Configuration conf = new Configuration();
  protected  HadoopTables tables = new HadoopTables(conf);

  protected FileFormat fileFormat;

  protected abstract void runAndValidate(File tableLocation, List<Record> expectedRecords) throws IOException;

  @Test
  public void testUnpartitionedTable() throws Exception {
    File tableLocation = temp.newFolder(fileFormat.name());
    Table table = tables
        .create(SCHEMA, PartitionSpec.unpartitioned(),
            ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()), tableLocation.toString());
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    DataFile dataFile = writeFile(temp.newFile(), table, null, fileFormat, expectedRecords);
    table.newAppend().appendFile(dataFile).commit();
    runAndValidate(tableLocation, expectedRecords);
  }

  @Test
  public void testPartitionedTable() throws Exception {
    File tableLocation = temp.newFolder(fileFormat.name());
    Assert.assertTrue(tableLocation.delete());
    Table table = tables.create(SCHEMA, SPEC,
                                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()),
                                tableLocation.toString());
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 1, 0L);
    expectedRecords.get(0).set(2, "2020-03-20");
    DataFile dataFile = writeFile(temp.newFile(), table, Row.of("2020-03-20", 0), fileFormat, expectedRecords);
    table.newAppend()
         .appendFile(dataFile)
         .commit();

    runAndValidate(tableLocation, expectedRecords);
  }

}
