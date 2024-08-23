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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedInputFormatInterface;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedSupport;
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
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.hive.HiveVersion;
import org.apache.iceberg.mr.InputFormatConfig;
import org.apache.iceberg.mr.mapred.AbstractMapredIcebergRecordReader;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.mr.mapred.MapredIcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergInputFormat;
import org.apache.iceberg.mr.mapreduce.IcebergSplit;
import org.apache.iceberg.mr.mapreduce.IcebergSplitContainer;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.util.SerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveIcebergInputFormat extends MapredIcebergInputFormat<Record>
    implements CombineHiveInputFormat.AvoidSplitCombination, VectorizedInputFormatInterface {

  private static final Logger LOG = LoggerFactory.getLogger(HiveIcebergInputFormat.class);
  private static final String HIVE_VECTORIZED_RECORDREADER_CLASS =
      "org.apache.iceberg.mr.hive.vector.HiveIcebergVectorizedRecordReader";
  private static final DynConstructors.Ctor<AbstractMapredIcebergRecordReader>
      HIVE_VECTORIZED_RECORDREADER_CTOR;

  static {
    if (HiveVersion.min(HiveVersion.HIVE_3)) {
      HIVE_VECTORIZED_RECORDREADER_CTOR =
          DynConstructors.builder(AbstractMapredIcebergRecordReader.class)
              .impl(
                  HIVE_VECTORIZED_RECORDREADER_CLASS,
                  IcebergInputFormat.class,
                  IcebergSplit.class,
                  JobConf.class,
                  Reporter.class)
              .build();
    } else {
      HIVE_VECTORIZED_RECORDREADER_CTOR = null;
    }
  }

  @Override
  public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
    // Convert Hive filter to Iceberg filter
    String hiveFilter = job.get(TableScanDesc.FILTER_EXPR_CONF_STR);
    if (hiveFilter != null) {
      ExprNodeGenericFuncDesc exprNodeDesc =
          SerializationUtilities.deserializeObject(hiveFilter, ExprNodeGenericFuncDesc.class);
      SearchArgument sarg = ConvertAstToSearchArg.create(job, exprNodeDesc);
      try {
        Expression filter = HiveIcebergFilterFactory.generateFilterExpression(sarg);
        job.set(InputFormatConfig.FILTER_EXPRESSION, SerializationUtil.serializeToBase64(filter));
      } catch (UnsupportedOperationException e) {
        LOG.warn(
            "Unable to create Iceberg filter, continuing without filter (will be applied by Hive later): ",
            e);
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
  public RecordReader<Void, Container<Record>> getRecordReader(
      InputSplit split, JobConf job, Reporter reporter) throws IOException {
    String[] selectedColumns = ColumnProjectionUtils.getReadColumnNames(job);
    job.setStrings(InputFormatConfig.SELECTED_COLUMNS, selectedColumns);

    if (HiveConf.getBoolVar(job, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)
        && Utilities.getVectorizedRowBatchCtx(job) != null) {
      Preconditions.checkArgument(
          HiveVersion.min(HiveVersion.HIVE_3), "Vectorization only supported for Hive 3+");

      job.setEnum(InputFormatConfig.IN_MEMORY_DATA_MODEL, InputFormatConfig.InMemoryDataModel.HIVE);
      job.setBoolean(InputFormatConfig.SKIP_RESIDUAL_FILTERING, true);

      IcebergSplit icebergSplit = ((IcebergSplitContainer) split).icebergSplit();
      // bogus cast for favouring code reuse over syntax
      return (RecordReader)
          HIVE_VECTORIZED_RECORDREADER_CTOR.newInstance(
              new IcebergInputFormat<>(), icebergSplit, job, reporter);
    } else {
      return super.getRecordReader(split, job, reporter);
    }
  }

  @Override
  public boolean shouldSkipCombine(Path path, Configuration conf) {
    return true;
  }

  @Override
  public VectorizedSupport.Support[] getSupportedFeatures() {
    return new VectorizedSupport.Support[0];
  }
}
