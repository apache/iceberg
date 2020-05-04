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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.BaseInputFormatTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIcebergInputFormat extends BaseInputFormatTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergInputFormat.class);

  private IcebergInputFormat inputFormat = new IcebergInputFormat();

  @Parameterized.Parameters
  public static Object[][] parameters() {
    return new Object[][] { new Object[] { "parquet" }, new Object[] { "avro" }
        /*
         * , TODO: put orc back, seems to be an issue with different versions of Orc in Hive and Iceberg new
         * Object[]{"orc"}
         */
    };
  }

  public TestIcebergInputFormat(String fileFormat) {
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetSplitsNoLocation() throws IOException {
    JobConf jobConf = new JobConf();
    inputFormat.getSplits(jobConf, 1);
  }

  @Test(expected = IOException.class)
  public void testGetSplitsInvalidLocationUri() throws IOException {
    JobConf jobConf = new JobConf();
    jobConf.set(IcebergInputFormat.TABLE_LOCATION, "http:");
    inputFormat.getSplits(jobConf, 1);
  }

  @Override
  protected void runAndValidate(File tableLocation, List<Record> expectedRecords) throws IOException {
    JobConf jobConf = new JobConf();
    jobConf.set(IcebergInputFormat.TABLE_LOCATION, "file:" + tableLocation);
    validate(jobConf, expectedRecords);
  }

  private void validate(JobConf jobConf, List<Record> expectedRecords) throws IOException {
    List<Record> actualRecords = readRecords(jobConf);
    Assert.assertEquals(expectedRecords, actualRecords);
  }

  private List<Record> readRecords(JobConf jobConf) throws IOException {
    InputSplit[] splits = inputFormat.getSplits(jobConf, 1);
    RecordReader reader = inputFormat.getRecordReader(splits[0], jobConf, null);
    List<Record> records = new ArrayList<>();
    IcebergWritable value = (IcebergWritable) reader.createValue();
    while (reader.next(null, value)) {
      records.add(value.getRecord().copy());
    }
    return records;
  }

}
