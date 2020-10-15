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

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.util.Progressable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapreduce.IcebergWritable;

public class HiveIcebergOutputFormat implements OutputFormat<NullWritable, IcebergWritable>,
    HiveOutputFormat<NullWritable, IcebergWritable> {

  private static final String TASK_ATTEMPT_ID_KEY = "mapred.task.id";

  // <TaskAttemptId, HiveIcebergRecordWriter> map to store the active writers
  // Stored in concurrent map, since some executor engines can share containers
  static final Map<TaskAttemptID, HiveIcebergRecordWriter> writers = new ConcurrentHashMap<>();

  private Configuration overlayedConf = null;
  private TaskAttemptID taskAttemptId = null;
  private Schema schema = null;
  private PartitionSpec spec = null;
  private FileFormat fileFormat = null;

  @Override
  @SuppressWarnings("rawtypes")
  public FileSinkOperator.RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath, Class valueClass,
      boolean isCompressed, Properties tableAndSerDeProperties, Progressable progress) {

    this.overlayedConf = createOverlayedConf(jc, tableAndSerDeProperties);
    this.taskAttemptId = TaskAttemptID.forName(overlayedConf.get(TASK_ATTEMPT_ID_KEY));
    this.schema = SchemaParser.fromJson(overlayedConf.get(InputFormatConfig.TABLE_SCHEMA));
    this.spec = PartitionSpecParser.fromJson(schema, overlayedConf.get(InputFormatConfig.PARTITION_SPEC));
    String fileFormatString =
        overlayedConf.get(InputFormatConfig.WRITE_FILE_FORMAT, InputFormatConfig.WRITE_FILE_FORMAT_DEFAULT.name());
    this.fileFormat = FileFormat.valueOf(fileFormatString);

    String location = LocationHelper.generateDataFileLocation(overlayedConf, taskAttemptId);
    HiveIcebergRecordWriter writer = new HiveIcebergRecordWriter(overlayedConf, location, fileFormat, schema, spec);
    writers.put(this.taskAttemptId, writer);

    return writer;
  }

  /**
   * Returns the union of the configuration and table properties with the
   * table properties taking precedence.
   */
  private static Configuration createOverlayedConf(Configuration conf, Properties tblProps) {
    Configuration newConf = new Configuration(conf);
    for (Map.Entry<Object, Object> prop : tblProps.entrySet()) {
      newConf.set((String) prop.getKey(), (String) prop.getValue());
    }
    return newConf;
  }

  @Override
  public org.apache.hadoop.mapred.RecordWriter<NullWritable, IcebergWritable> getRecordWriter(FileSystem ignored,
      JobConf job, String name, Progressable progress) {

    throw new UnsupportedOperationException("Please implement if needed");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) {
    // Not doing any check.
  }
}
