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

package org.apache.iceberg.mr.mapred;

import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.iceberg.mr.TestHelpers.writeFile;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestIcebergInputFormat {

  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergInputFormat.class);

  static final Schema SCHEMA = new Schema(required(1, "data", Types.StringType.get()),
      required(2, "id", Types.LongType.get()), required(3, "date", Types.StringType.get()));

  static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).identity("date").bucket("id", 1).build();

  private IcebergInputFormat inputFormat = new IcebergInputFormat();
  private JobConf jobConf = new JobConf();
  private Configuration conf = new Configuration();
  private HadoopTables tables = new HadoopTables(conf);

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] { new Object[] { "parquet" }, new Object[] { "avro" }
        /*
         * , TODO: put orc back, seems to be an issue with different versions of Orc in Hive and
         * Iceberg new Object[]{"orc"}
         */
    };
  }

  private final FileFormat fileFormat;

  public TestIcebergInputFormat(String fileFormat) {
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSplitsNoLocation() throws IOException {
    inputFormat.getSplits(jobConf, 1);
  }

  @Test(expected = IOException.class)
  public void testGetSplitsInvalidLocationUri() throws IOException {
    jobConf.set("location", "http:");
    inputFormat.getSplits(jobConf, 1);
  }

  @Test
  public void testGetRecordReader() throws IOException {
    File tableLocation = temp.newFolder(fileFormat.name());
    Table table = tables.create(SCHEMA, PartitionSpec.unpartitioned(),
        ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, fileFormat.name()),
        tableLocation.toString());
    List<Record> expectedRecords = RandomGenericData.generate(table.schema(), 3, 0L);

    DataFile dataFile = writeFile(temp.newFile(), table, null, fileFormat, expectedRecords);
    table.newAppend().appendFile(dataFile).commit();

    jobConf.set("location", "file:" + tableLocation);
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    RecordReader reader = inputFormat.getRecordReader(splits[0], jobConf, null);
    IcebergWritable value = (IcebergWritable) reader.createValue();

    List<Record> records = Lists.newArrayList();
    while (reader.next(null, value)) {
      records.add(value.getRecord().copy());
    }
    assertEquals(3, records.size());
  }

  // TODO: add more tests, based on the mapreduce InputFormat tests (possibly refactor shared code
  // into an abstract parent test class).

}
