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

import java.util.Properties;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.Container;

public class HiveIcebergOutputFormat<T> implements OutputFormat<NullWritable, Container<Record>>,
    HiveOutputFormat<NullWritable, Container<Record>> {

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass,
      boolean isCompressed, Properties tableAndSerDeProperties, Progressable progress) {
    return writer(jc);
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, Container<Record>> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) {
    return writer(job);
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }

  private static HiveIcebergRecordWriter writer(JobConf jc) {
    TaskAttemptID taskAttemptID = TezUtil.taskAttemptWrapper(jc);
    Schema schema = HiveIcebergStorageHandler.schema(jc);
    PartitionSpec spec = HiveIcebergStorageHandler.spec(jc);
    FileFormat fileFormat = FileFormat.valueOf(jc.get(InputFormatConfig.WRITE_FILE_FORMAT));
    long targetFileSize = jc.getLong(InputFormatConfig.WRITE_TARGET_FILE_SIZE, Long.MAX_VALUE);
    FileIO io = HiveIcebergStorageHandler.io(jc);
    LocationProvider location = HiveIcebergStorageHandler.location(jc);
    EncryptionManager encryption = HiveIcebergStorageHandler.encryption(jc);
    OutputFileFactory outputFileFactory =
        new OutputFileFactory(spec, fileFormat, location, io, encryption, taskAttemptID.getTaskID().getId(),
            taskAttemptID.getId(), jc.get(HiveConf.ConfVars.HIVEQUERYID.varname) + "-" + taskAttemptID.getJobID());
    HiveIcebergRecordWriter writer = new HiveIcebergRecordWriter(schema, spec, fileFormat,
        new GenericAppenderFactory(schema, spec), outputFileFactory, io, targetFileSize, taskAttemptID);

    return writer;
  }
}
