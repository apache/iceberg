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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
                                    implements CombineHiveInputFormat.AvoidSplitCombination {

  private transient Table table;
  private transient Schema schema;

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    table = TableResolver.resolveTableFromConfiguration(job);
    schema = table.schema();

    forwardConfigSettings(job);

    return Arrays.stream(super.getSplits(job, numSplits))
                 .map(split -> new HiveIcebergSplit((IcebergSplit) split, table.location()))
                 .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<Record>> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws IOException {
    // Since Hive passes a copy of `job` in `getSplits`, we need to forward the conf settings again.
    forwardConfigSettings(job);
    return super.getRecordReader(split, job, reporter);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return true;
  }

  /**
   * Forward configuration settings to the underlying MR input format.
   */
  private void forwardConfigSettings(JobConf job) {
    Preconditions.checkNotNull(table, "Table cannot be null");
    Preconditions.checkNotNull(schema, "Schema cannot be null");

    // Once mapred.TableResolver and mapreduce.TableResolver use the same property for the location of the table
    // (TABLE_LOCATION vs. TABLE_PATH), this line can be removed: see https://github.com/apache/iceberg/issues/1155.
    job.set(InputFormatConfig.TABLE_PATH, table.location());
    job.set(InputFormatConfig.TABLE_SCHEMA, SchemaParser.toJson(schema));
  }
}
