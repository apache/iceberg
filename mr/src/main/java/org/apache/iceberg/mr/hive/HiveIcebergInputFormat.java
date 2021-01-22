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
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
                                    implements CombineHiveInputFormat.AvoidSplitCombination {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergInputFormat.class);

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // Convert Hive filter to Iceberg filter
    String hiveFilter = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (hiveFilter != null) {
      ExprNodeGenericFuncDesc exprNodeDesc = SerializationUtilities
              .deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      try {
        Expression filter = HiveIcebergFilterFactory.generateFilterExpression(sarg);
        job.set(InputFormatConfig.FILTER_EXPRESSION, SerializationUtil.serializeToBase64(filter));
      } catch (UnsupportedOperationException e) {
        LOG.warn("Unable to create Iceberg filter, continuing without filter (will be applied by Hive later): ", e);
      }
    }

    String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(job);
    job.setStrings(InputFormatConfig.SELECTED_COLUMNS, selectedColumns);

    String location = job.get(InputFormatConfig.TABLE_LOCATION);
    return Arrays.stream(super.getSplits(job, numSplits))
                 .map(split -> new HiveIcebergSplit((IcebergSplit) split, location))
                 .toArray(InputSplit[]::new);
  }

  @Override
  public RecordReader<Void, Container<Record>> getRecordReader(InputSplit split, JobConf job,
                                                               Reporter reporter) throws IOException {
    String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(job);
    job.setStrings(InputFormatConfig.SELECTED_COLUMNS, selectedColumns);
    return super.getRecordReader(split, job, reporter);
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return true;
  }
}
