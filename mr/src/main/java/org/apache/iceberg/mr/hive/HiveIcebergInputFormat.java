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
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
                                    implements CombineHiveInputFormat.AvoidSplitCombination {

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    String location = job.get(InputFormatConfig.TABLE_LOCATION);
    return Arrays.stream(super.getSplits(job, numSplits))
                 .map(split -> new HiveIcebergSplit((IcebergSplit) split, location))
                 .toArray(InputSplit[]::new);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return true;
  }
}
