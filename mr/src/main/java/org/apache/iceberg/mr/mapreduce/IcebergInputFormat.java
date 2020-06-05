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

package org.apache.iceberg.mr.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.common.DynConstructors;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.IcebergMRConfig;
import org.apache.iceberg.mr.InMemoryDataModel;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Mrv2 InputFormat API for Iceberg.
 * @param <T> T is the in memory data model which can either be Pig tuples, Hive rows. Default is Iceberg records
 */
public class IcebergInputFormat<T> extends InputFormat<Void, T> {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergInputFormat.class);

  private transient List<InputSplit> splits;

  /**
   * Configures the {@code Job} to use the {@code IcebergInputFormat} and
   * returns a helper to add further configuration.
   *
   * @param job the {@code Job} to configure
   */
  public static IcebergMRConfig.Builder configure(Job job) {
    job.setInputFormatClass(IcebergInputFormat.class);
    return IcebergMRConfig.Builder.newInstance(job.getConfiguration());
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) {
    if (splits != null) {
      LOG.info("Returning cached splits: {}", splits.size());
      return splits;
    }

    Configuration conf = context.getConfiguration();
    Table table = findTable(conf);
    TableScan scan = table.newScan()
                          .caseSensitive(IcebergMRConfig.caseSensitive(conf));

    long snapshotId = IcebergMRConfig.snapshotId(conf);
    if (snapshotId != -1) {
      scan = scan.useSnapshot(snapshotId);
    }

    long asOfTime = IcebergMRConfig.asOfTime(conf);
    if (asOfTime != -1) {
      scan = scan.asOfTime(asOfTime);
    }

    long splitSize = IcebergMRConfig.splitSize(conf);
    if (splitSize > 0) {
      scan = scan.option(TableProperties.SPLIT_SIZE, String.valueOf(splitSize));
    }

    Schema projection = IcebergMRConfig.projection(conf);
    if (projection != null) {
      scan.project(projection);
    }

    Expression filter = IcebergMRConfig.filter(conf);
    if (filter != null) {
      scan = scan.filter(filter);
    }

    splits = Lists.newArrayList();
    boolean applyResidual = IcebergMRConfig.applyResidualFiltering(conf);
    InMemoryDataModel model = IcebergMRConfig.inMemoryDataModel(conf);
    try (CloseableIterable<CombinedScanTask> tasksIterable = scan.planTasks()) {
      tasksIterable.forEach(task -> {
        if (applyResidual && model.isHiveOrPig()) {
          //TODO: We do not support residual evaluation for HIVE and PIG in memory data model yet
          checkResiduals(task);
        }
        splits.add(new IcebergSplit(conf, task));
      });
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to close table scan: %s", scan);
    }

    return splits;
  }

  private static void checkResiduals(CombinedScanTask task) {
    task.files().forEach(fileScanTask -> {
      Expression residual = fileScanTask.residual();
      if (residual != null && !residual.equals(Expressions.alwaysTrue())) {
        throw new UnsupportedOperationException(
            String.format(
                "Filter expression %s is not completely satisfied. Additional rows " +
                    "can be returned not satisfied by the filter expression", residual));
      }
    });
  }

  @Override
  public RecordReader<Void, T> createRecordReader(InputSplit split, TaskAttemptContext context) {
    return new IcebergRecordReader<>();
  }

  public static Table findTable(Configuration conf) {
    String path = IcebergMRConfig.readFrom(conf);
    Preconditions.checkArgument(path != null, "Table path should not be null");
    if (path.contains("/")) {
      HadoopTables tables = new HadoopTables(conf);
      return tables.load(path);
    }

    String catalogLoaderClass = IcebergMRConfig.catalogLoader(conf);
    if (catalogLoaderClass != null) {
      Function<Configuration, Catalog> catalogLoader = (Function<Configuration, Catalog>)
          DynConstructors.builder(Function.class)
                         .impl(catalogLoaderClass)
                         .build()
                         .newInstance();
      Catalog catalog = catalogLoader.apply(conf);
      TableIdentifier tableIdentifier = TableIdentifier.parse(path);
      return catalog.loadTable(tableIdentifier);
    } else {
      throw new IllegalArgumentException("No custom catalog specified to load table " + path);
    }
  }
}
