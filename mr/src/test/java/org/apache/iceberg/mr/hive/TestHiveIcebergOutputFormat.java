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

package org.apache.iceberg.mr.hive;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobContext;
import org.apache.hadoop.mapred.JobContextImpl;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.OutputCommitter;
import org.apache.hadoop.mapred.TaskAttemptContext;
import org.apache.hadoop.mapred.TaskAttemptContextImpl;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHiveIcebergOutputFormat {

  private static final Object[] TESTED_FILE_FORMATS = new Object[] {"avro", "orc", "parquet"};

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameters(name = "{0}")
  public static Object[] parameters() {
    return TESTED_FILE_FORMATS;
  }

  // parametrized variables
  private final FileFormat fileFormat;

  private static final Configuration conf = new Configuration();
  private TestHelper helper;
  private Table table;
  private TestOutputFormat testOutputFormat;

  public TestHiveIcebergOutputFormat(String fileFormat) {
    this.fileFormat = FileFormat.valueOf(fileFormat.toUpperCase(Locale.ENGLISH));
  }

  @Before
  public void before() throws Exception {
    helper = new TestHelper(conf,
        new HadoopTables(conf),
        temp.newFolder(this.fileFormat.name()).toString(),
        HiveIcebergSerDeTestUtils.FULL_SCHEMA,
        null,
        fileFormat,
        temp);

    table = helper.createUnpartitionedTable();
    testOutputFormat = new TestOutputFormat(table, fileFormat);
  }

  @Test
  public void testWriteRow() throws IOException {
    // Write a row.
    List<Record> records =
        Arrays.asList(new Record[] { HiveIcebergSerDeTestUtils.getTestRecord(FileFormat.PARQUET.equals(fileFormat)) });

    testOutputFormat.write(records, false, false);
    testOutputFormat.validate(records);
  }

  @Test
  public void testNullRow() throws IOException {
    // FIXME: ORC file does not read back the row consisting only of nulls. The data in the files seems ok.
    if (FileFormat.ORC.equals(fileFormat)) {
      return;
    }
    // Write a row.
    List<Record> records = Arrays.asList(new Record[] { HiveIcebergSerDeTestUtils.getNullTestRecord() });

    testOutputFormat.write(records, false, false);
    testOutputFormat.validate(records);
  }

  @Test
  public void testMultipleRows() throws IOException {
    // Write 2 rows. One with nulls too.
    List<Record> records = Arrays.asList(new Record[] {
        HiveIcebergSerDeTestUtils.getTestRecord(FileFormat.PARQUET.equals(fileFormat)),
        HiveIcebergSerDeTestUtils.getNullTestRecord()
    });

    testOutputFormat.write(records, false, false);
    testOutputFormat.validate(records);
  }

  @Test
  public void testRandomRecords() throws IOException {
    // Write 30 random rows
    // FIXME: Parquet appender expect byte[] instead of UUID when writing values.
    if (FileFormat.PARQUET.equals(fileFormat)) {
      return;
    }

    List<Record> records = helper.generateRandomRecords(30, 0L);

    testOutputFormat.write(records, false, false);
    testOutputFormat.validate(records);
  }

  @Test
  public void testWithAbortedTask() throws IOException {
    // Write a row.
    List<Record> records =
        Arrays.asList(new Record[] { HiveIcebergSerDeTestUtils.getTestRecord(FileFormat.PARQUET.equals(fileFormat)) });

    testOutputFormat.write(records, true, false);
    testOutputFormat.validate(records);
  }

  @Test
  public void testEmptyWrite() throws IOException {
    testOutputFormat.writeEmpty();
    testOutputFormat.validate(Collections.emptyList());
  }

  @Test
  public void testAbortJob() throws IOException {
    // Write a row.
    List<Record> records =
        Arrays.asList(new Record[] { HiveIcebergSerDeTestUtils.getTestRecord(FileFormat.PARQUET.equals(fileFormat)) });

    testOutputFormat.write(records, true, true);
    testOutputFormat.validate(Collections.emptyList());
  }

  private static class TestOutputFormat {
    private Configuration configuration;
    private Properties serDeProperties;
    private JobConf jobConf;
    private JobContext jobContext;
    private TaskAttemptContext taskAttemptContext;
    private boolean jobAborted;

    private TestOutputFormat(Table table, FileFormat fileFormat) {
      configuration = new Configuration();

      // Create the SerDeProperties
      serDeProperties = new Properties();
      serDeProperties.put(InputFormatConfig.WRITE_FILE_FORMAT, fileFormat.name());
      serDeProperties.put(InputFormatConfig.TABLE_LOCATION, table.location());
      serDeProperties.put(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(table.schema()));
      serDeProperties.put(InputFormatConfig.PARTITION_SPEC, PartitionSpecParser.toJson(table.spec()));
      serDeProperties.put("location", table.location());
      serDeProperties.put(HiveConf.ConfVars.HIVEQUERYID.varname, "TestQuery_" + fileFormat);

      // Create a dummy jobContext and taskAttemptContext
      jobConf = new JobConf(configuration);
      TaskAttemptID taskAttemptID = new TaskAttemptID();
      jobConf.set(InputFormatConfig.TABLE_LOCATION, table.location());
      jobConf.set("mapred.task.id", taskAttemptID.toString());
      jobConf.set(HiveConf.ConfVars.HIVEQUERYID.varname, "TestQuery_" + fileFormat);
      jobContext = new JobContextImpl(jobConf, new JobID());
      taskAttemptContext = new TaskAttemptContextImpl(jobConf, taskAttemptID);
    }

    private void writeEmpty() throws IOException {
      OutputCommitter outputCommitter = new HiveIcebergOutputCommitter();

      outputCommitter.commitTask(taskAttemptContext);
      outputCommitter.commitJob(jobContext);
    }

    private void write(List<Record> records, boolean withAbortedTask, boolean withAbortJob) throws IOException {
      HiveIcebergOutputFormat outputFormat = new HiveIcebergOutputFormat();
      OutputCommitter outputCommitter = new HiveIcebergOutputCommitter();

      if (withAbortedTask) {
        HiveIcebergRecordWriter writer =
            (HiveIcebergRecordWriter) outputFormat.getHiveRecordWriter(jobConf,
                null, null, false, serDeProperties, null);

        records.forEach(record -> writer.write(new IcebergWritable(record)));

        writer.close(false);

        // Abort the previous task
        outputCommitter.abortTask(taskAttemptContext);

        // Create and set the new task attempt id
        TaskAttemptID newId = new TaskAttemptID(taskAttemptContext.getTaskAttemptID().getTaskID(), 1);
        jobConf.set("mapred.task.id", newId.toString());
        taskAttemptContext = new TaskAttemptContextImpl(jobConf, newId);
      }

      HiveIcebergRecordWriter writer =
          (HiveIcebergRecordWriter) outputFormat.getHiveRecordWriter(jobConf,
              null, null, false, serDeProperties, null);

      records.forEach(record -> writer.write(new IcebergWritable(record)));

      writer.close(false);

      outputCommitter.commitTask(taskAttemptContext);
      if (withAbortJob) {
        outputCommitter.abortJob(jobContext, JobStatus.State.KILLED);
        jobAborted = true;
      } else {
        outputCommitter.commitJob(jobContext);
        jobAborted = false;
      }
    }

    private void validate(List<Record> expected) throws IOException {
      Table table = Catalogs.loadTable(configuration, serDeProperties);
      HiveIcebergSerDeTestUtils.validate(table, expected, null);

      // Check the number of the files, and the content of the directory
      // We expect the following dir structure
      // table - queryId - jobId + attemptFile
      //                         \ task-0.toCommit
      // We definitely do not want more files in the directory

      String expectedBaseLocation = table.location() + "/" + jobConf.get(HiveConf.ConfVars.HIVEQUERYID.varname) +
          "/" + taskAttemptContext.getTaskAttemptID().getJobID();

      if (!jobAborted) {
        Set<String> fileList = Sets.newHashSet();
        for (String fileName : new File(expectedBaseLocation).list((dir, name) -> !name.startsWith("."))) {
          fileList.add(fileName.replaceAll("(.*)_([^_]*)", "$1"));
        }

        if (expected.size() > 0) {
          TableScan scan = table.newScan();
          String dataFilePath = scan.planFiles().iterator().next().file().path().toString();
          File parentDir = new File(dataFilePath).getParentFile();
          String expectedFileName = taskAttemptContext.getTaskAttemptID().toString();
          expectedFileName = expectedFileName.substring(0, expectedFileName.length() - 2);

          Assert.assertEquals(expectedBaseLocation, parentDir.getPath());
          Assert.assertEquals(fileList, Sets.newHashSet(new String[]{"task-0.toCommit", expectedFileName}));
        } else {
          Assert.assertEquals(fileList, Sets.newHashSet(new String[]{"task-0.toCommit"}));
        }
      } else {
        // If the job is aborted, we expect that the job directory is removed
        Assert.assertFalse(new File(expectedBaseLocation).exists());
      }
    }
  }
}
