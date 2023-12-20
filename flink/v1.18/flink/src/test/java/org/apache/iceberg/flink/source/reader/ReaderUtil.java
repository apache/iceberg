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
package org.apache.iceberg.flink.source.reader;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.BaseCombinedScanTask;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.ResidualEvaluator;
import org.apache.iceberg.flink.TestFixtures;
import org.apache.iceberg.flink.source.DataIterator;
import org.apache.iceberg.flink.source.RowDataFileScanTaskReader;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.rules.TemporaryFolder;

public class ReaderUtil {

  private ReaderUtil() {}

  public static FileScanTask createFileTask(
      List<Record> records,
      File file,
      FileFormat fileFormat,
      FileAppenderFactory<Record> appenderFactory)
      throws IOException {
    FileAppender<Record> appender =
        appenderFactory.newAppender(Files.localOutput(file), fileFormat);
    try {
      appender.addAll(records);
    } finally {
      appender.close();
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(records.size())
            .withFileSizeInBytes(file.length())
            .withPath(file.toString())
            .withFormat(fileFormat)
            .withMetrics(appender.metrics())
            .build();

    ResidualEvaluator residuals = ResidualEvaluator.unpartitioned(Expressions.alwaysTrue());
    return new BaseFileScanTask(
        dataFile,
        null,
        SchemaParser.toJson(TestFixtures.SCHEMA),
        PartitionSpecParser.toJson(PartitionSpec.unpartitioned()),
        residuals);
  }

  public static DataIterator<RowData> createDataIterator(CombinedScanTask combinedTask) {
    return new DataIterator<>(
        new RowDataFileScanTaskReader(
            TestFixtures.SCHEMA, TestFixtures.SCHEMA, null, true, Collections.emptyList()),
        combinedTask,
        new HadoopFileIO(new org.apache.hadoop.conf.Configuration()),
        new PlaintextEncryptionManager());
  }

  public static List<List<Record>> createRecordBatchList(
      Schema schema, int listSize, int batchCount) {
    return createRecordBatchList(0L, schema, listSize, batchCount);
  }

  public static List<List<Record>> createRecordBatchList(
      long seed, Schema schema, int listSize, int batchCount) {
    List<Record> records = RandomGenericData.generate(schema, listSize * batchCount, seed);
    return Lists.partition(records, batchCount);
  }

  public static CombinedScanTask createCombinedScanTask(
      List<List<Record>> recordBatchList,
      TemporaryFolder temporaryFolder,
      FileFormat fileFormat,
      GenericAppenderFactory appenderFactory)
      throws IOException {
    List<FileScanTask> fileTasks = Lists.newArrayListWithCapacity(recordBatchList.size());
    for (List<Record> recordBatch : recordBatchList) {
      FileScanTask fileTask =
          ReaderUtil.createFileTask(
              recordBatch, temporaryFolder.newFile(), fileFormat, appenderFactory);
      fileTasks.add(fileTask);
    }

    return new BaseCombinedScanTask(fileTasks);
  }
}
