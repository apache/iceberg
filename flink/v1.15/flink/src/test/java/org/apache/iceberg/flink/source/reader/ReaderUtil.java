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
import java.util.List;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.BaseFileScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.SchemaParser;
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

public class ReaderUtil {

  private ReaderUtil() {}

  public static FileScanTask createFileTask(
      List<Record> records,
      File file,
      FileFormat fileFormat,
      FileAppenderFactory<Record> appenderFactory)
      throws IOException {
    try (FileAppender<Record> appender =
        appenderFactory.newAppender(Files.localOutput(file), fileFormat)) {
      appender.addAll(records);
    }

    DataFile dataFile =
        DataFiles.builder(PartitionSpec.unpartitioned())
            .withRecordCount(records.size())
            .withFileSizeInBytes(file.length())
            .withPath(file.toString())
            .withFormat(fileFormat)
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
        new RowDataFileScanTaskReader(TestFixtures.SCHEMA, TestFixtures.SCHEMA, null, true),
        combinedTask,
        new HadoopFileIO(new org.apache.hadoop.conf.Configuration()),
        new PlaintextEncryptionManager());
  }
}
